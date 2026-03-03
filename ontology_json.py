# -*- coding: utf-8 -*-
"""
ontology_json.py
================
DOVI(문서 비서) 프로토타입용 "가벼운 온톨로지 + 내부 저장" 모듈입니다.

목표
- 엑셀은 "사람이 보기 좋은 결과물"
- JSON-LD는 "서비스 내부 저장(검색/재사용/정합성 검증/추적)" 목적의 그래프 데이터

핵심 아이디어
- PDF 1개(Document) 안에 여러 지번(Parcel)과 여러 문서(등기/대장/이용계획)가 섞여도
  그래프 구조로 깔끔하게 저장합니다.
- 특히 등기사항전부증명서(토지)에서 표제부/갑구/을구를
  RegistryDocument -> RegistrySection -> RegistryEntry로 보존합니다.

입력
- df_final: 지번별 최종 필드(지목, 면적, 용도지역, 소유자, 발급일 등)
- df_candidates: 후보 근거(페이지/문서타입/우선순위)
- df_pages: 페이지 인덱스(페이지->문서타입->지번 추정 등)
- registry_tables: {"pyo": df_pyo, "gab": df_gab, "eul": df_eul}
- df_checks: 검증 결과(표제부 위 소재지번 vs 표제부 소재지번 일치 등)

출력
- JSON-LD(dict): {"@context": ..., "@graph": [...]}
- make_jsonld_bytes()로 다운로드 가능

주의
- JSON-LD는 개인정보(소유자 등)가 포함될 수 있습니다.
  서비스화 시 저장/권한/마스킹 정책을 반드시 설계하세요.
"""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

# ===== Namespace =====
DOVI_NS = "https://example.org/dovi#"
SCHEMA_NS = "https://schema.org/"
PROV_NS = "http://www.w3.org/ns/prov#"
XSD_NS = "http://www.w3.org/2001/XMLSchema#"

DEFAULT_BASE_IRI = "urn:dovi:"


def _ensure_base(base_iri: str) -> str:
    base = (base_iri or "").strip()
    if not base:
        return DEFAULT_BASE_IRI
    if base[-1] not in (":", "#", "/"):
        base += ":"
    return base


def _sha1_12(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()[:12]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_str(x: Any) -> str:
    if x is None:
        return ""
    try:
        if pd.isna(x):
            return ""
    except Exception:
        pass
    return str(x).strip()


def _safe_int(x: Any) -> Optional[int]:
    s = _safe_str(x)
    if not s:
        return None
    try:
        return int(float(s))
    except Exception:
        return None


def _safe_float(x: Any) -> Optional[float]:
    s = _safe_str(x)
    if not s:
        return None
    try:
        return float(s)
    except Exception:
        return None


def parse_area_sqm(text: str) -> Optional[float]:
    """
    '1,540㎡', '1540 m2', '1540m2' 등에서 숫자만 뽑아 float(sqm)으로 변환
    """
    s = _safe_str(text)
    if not s:
        return None
    m = re.search(r"(\d+(?:,\d+)*(?:\.\d+)?)", s)
    if not m:
        return None
    try:
        return float(m.group(1).replace(",", ""))
    except Exception:
        return None


def default_context() -> Dict[str, Any]:
    """
    JSON-LD @context: 최소 온톨로지(가벼운 스키마)
    - dovi: 커스텀 용어
    - schema: 메타데이터(파일명 등)
    - prov: provenance 확장 여지
    """
    return {
        "@version": 1.1,
        "dovi": DOVI_NS,
        "schema": SCHEMA_NS,
        "prov": PROV_NS,
        "xsd": XSD_NS,

        # JSON-LD basics
        "id": "@id",
        "type": "@type",

        # Classes
        "Document": "dovi:Document",
        "ExtractionRun": "dovi:ExtractionRun",
        "Page": "dovi:Page",
        "Parcel": "dovi:Parcel",
        "Fact": "dovi:Fact",
        "Candidate": "dovi:Candidate",
        "ValidationResult": "dovi:ValidationResult",
        "RegistryDocument": "dovi:RegistryDocument",
        "RegistrySection": "dovi:RegistrySection",
        "RegistryEntry": "dovi:RegistryEntry",

        # Common metadata
        "fileName": "schema:name",
        "fileHash": "dovi:fileHash",
        "createdAt": {"@id": "schema:dateCreated", "@type": "xsd:dateTime"},
        "generator": "schema:generator",
        "runId": "dovi:runId",
        "algorithmVersion": "dovi:algorithmVersion",

        # Document -> Run/Pages/Parcels/Registries
        "wasGeneratedBy": {"@id": "prov:wasGeneratedBy", "@type": "@id"},
        "hasPage": {"@id": "dovi:hasPage", "@type": "@id"},
        "mentionsParcel": {"@id": "dovi:mentionsParcel", "@type": "@id"},
        "hasRegistry": {"@id": "dovi:hasRegistry", "@type": "@id"},
        "hasCandidate": {"@id": "dovi:hasCandidate", "@type": "@id"},

        # Page
        "pageNumber": {"@id": "dovi:pageNumber", "@type": "xsd:integer"},
        "docType": "dovi:docType",
        "source": "dovi:source",
        "textLength": {"@id": "dovi:textLength", "@type": "xsd:integer"},
        "aboutParcel": {"@id": "dovi:aboutParcel", "@type": "@id"},

        # Parcel
        "lot": "dovi:lot",
        "siteAddress": "dovi:siteAddress",
        "roadAddress": "dovi:roadAddress",
        "hasFact": {"@id": "dovi:hasFact", "@type": "@id"},
        "hasValidation": {"@id": "dovi:hasValidation", "@type": "@id"},

        # Registry document/section/entry
        "registryType": "dovi:registryType",          # land/building etc
        "issueDate": "dovi:issueDate",
        "confirmNo": "dovi:confirmNo",
        "hasSection": {"@id": "dovi:hasSection", "@type": "@id"},
        "sectionType": "dovi:sectionType",            # pyo/gab/eul
        "hasEntry": {"@id": "dovi:hasEntry", "@type": "@id"},
        "inSection": {"@id": "dovi:inSection", "@type": "@id"},
        "rank": "dovi:rank",
        "purpose": "dovi:purpose",
        "acceptance": "dovi:acceptance",
        "cause": "dovi:cause",
        "details": "dovi:details",

        # Facts
        "field": "dovi:field",
        "value": "dovi:value",
        "valueNumber": {"@id": "dovi:valueNumber", "@type": "xsd:decimal"},
        "unit": "dovi:unit",
        "confidence": {"@id": "dovi:confidence", "@type": "xsd:integer"},
        "supportedBy": {"@id": "dovi:supportedBy", "@type": "@id"},

        # Candidates (evidence)
        "priority": {"@id": "dovi:priority", "@type": "xsd:integer"},
        "evidencePage": {"@id": "dovi:evidencePage", "@type": "@id"},

        # Validation
        "checkName": "dovi:checkName",
        "result": "dovi:result",
        "leftValue": "dovi:leftValue",
        "rightValue": "dovi:rightValue",
    }


def build_dovi_jsonld(
    *,
    file_name: str,
    file_hash: str,
    df_final: Optional[pd.DataFrame] = None,
    df_candidates: Optional[pd.DataFrame] = None,
    df_pages: Optional[pd.DataFrame] = None,
    registry_tables: Optional[Dict[str, pd.DataFrame]] = None,
    df_checks: Optional[pd.DataFrame] = None,
    base_iri: str = DEFAULT_BASE_IRI,
    ontology_version: str = "0.2",
    generator_name: str = "DOVI-Streamlit",
    include_candidates: bool = True,
) -> Dict[str, Any]:
    """
    추출 결과를 JSON-LD 그래프로 변환합니다.

    - df_final의 각 행(지번)을 Parcel 노드로 만들고 Fact를 붙입니다.
    - registry_tables가 있으면 RegistryDocument/RegistrySection/RegistryEntry를 생성합니다.
    - df_candidates가 있으면 Candidate(근거) 노드를 만들고 Fact.supportedBy로 연결합니다.
    - df_checks(검증)가 있으면 ValidationResult로 저장합니다.

    반환:
      {"@context": ..., "@graph": [...]}
    """
    base = _ensure_base(base_iri)
    doc_id = f"{base}document:{file_hash}"
    run_id = f"{doc_id}#run-{_sha1_12(_now_iso())}"

    graph: List[Dict[str, Any]] = []

    # ExtractionRun (Activity)
    run_node = {
        "@id": run_id,
        "@type": "ExtractionRun",
        "runId": run_id,
        "createdAt": _now_iso(),
        "algorithmVersion": ontology_version,
        "generator": generator_name,
    }
    graph.append(run_node)

    # Document
    doc_node: Dict[str, Any] = {
        "@id": doc_id,
        "@type": "Document",
        "fileName": file_name,
        "fileHash": file_hash,
        "createdAt": _now_iso(),
        "generator": generator_name,
        "wasGeneratedBy": run_id,
        "hasPage": [],
        "mentionsParcel": [],
        "hasRegistry": [],
        "hasCandidate": [],
    }
    graph.append(doc_node)

    # ---- caches ----
    page_ids: Dict[int, str] = {}
    parcel_nodes: Dict[str, Dict[str, Any]] = {}
    registry_ids_by_lot: Dict[str, str] = {}
    section_ids_by_key: Dict[Tuple[str, str], str] = {}  # (registry_id, sectionType) -> section_id
    cand_index: Dict[Tuple[str, str, str], List[str]] = {}  # (lot, field, value) -> [cand_ids]

    def ensure_parcel(lot: str, site: str = "", road: str = "", confidence: Optional[int] = None) -> str:
        lot_n = _safe_str(lot)
        site_n = re.sub(r"\s+", " ", _safe_str(site))
        road_n = re.sub(r"\s+", " ", _safe_str(road))
        key = f"{site_n}|{road_n}|{lot_n}"
        pid = f"{base}parcel:{_sha1_12(key)}"

        if pid not in parcel_nodes:
            node: Dict[str, Any] = {
                "@id": pid,
                "@type": "Parcel",
                "lot": lot_n,
                "siteAddress": site_n,
                "roadAddress": road_n,
                "hasFact": [],
                "hasValidation": [],
            }
            if confidence is not None:
                node["confidence"] = confidence
            parcel_nodes[pid] = node
            graph.append(node)
            doc_node["mentionsParcel"].append(pid)
        else:
            node = parcel_nodes[pid]
            # update if empty
            if site_n and not node.get("siteAddress"):
                node["siteAddress"] = site_n
            if road_n and not node.get("roadAddress"):
                node["roadAddress"] = road_n
            if confidence is not None:
                old = node.get("confidence")
                if old is None or (isinstance(old, int) and confidence > old):
                    node["confidence"] = confidence

        return pid

    def ensure_registry_for_lot(lot: str, *, issue_date: str = "", confirm_no: str = "") -> str:
        """
        지번별 등기문서(RegistryDocument) 노드를 1개 생성.
        동일 lot이 여러 번 나오면 최초 생성값 유지.
        """
        lot_n = _safe_str(lot)
        if lot_n in registry_ids_by_lot:
            return registry_ids_by_lot[lot_n]

        key = f"{file_hash}|{lot_n}|{issue_date}|{confirm_no}|registry_land"
        rid = f"{base}registry:{_sha1_12(key)}"
        registry_ids_by_lot[lot_n] = rid

        pid = ensure_parcel(lot_n)

        node = {
            "@id": rid,
            "@type": "RegistryDocument",
            "registryType": "land",
            "aboutParcel": pid,
            "issueDate": _safe_str(issue_date),
            "confirmNo": _safe_str(confirm_no),
            "hasSection": [],
        }
        graph.append(node)
        doc_node["hasRegistry"].append(rid)
        return rid

    def ensure_section(registry_id: str, section_type: str) -> str:
        key = (registry_id, section_type)
        if key in section_ids_by_key:
            return section_ids_by_key[key]
        sid = f"{registry_id}#section-{section_type}"
        node = {
            "@id": sid,
            "@type": "RegistrySection",
            "sectionType": section_type,  # pyo/gab/eul
            "hasEntry": [],
        }
        section_ids_by_key[key] = sid
        graph.append(node)
        # link
        for g in graph:
            if g.get("@id") == registry_id:
                g.setdefault("hasSection", [])
                if sid not in g["hasSection"]:
                    g["hasSection"].append(sid)
                break
        return sid

    # ============================================================
    # 1) Pages
    # ============================================================
    if df_pages is not None and len(df_pages) > 0:
        for rec in df_pages.to_dict("records"):
            pno = _safe_int(rec.get("페이지") or rec.get("page") or rec.get("pageNumber"))
            if pno is None:
                continue
            page_id = f"{doc_id}#page-{pno}"
            page_ids[pno] = page_id
            doc_node["hasPage"].append(page_id)

            doc_type = _safe_str(rec.get("문서타입") or rec.get("docType") or "")
            source = _safe_str(rec.get("소스") or rec.get("source") or "")
            text_len = _safe_int(rec.get("텍스트길이") or rec.get("textLength") or "")
            lot_guess = _safe_str(rec.get("지번(추정)") or rec.get("지번") or rec.get("lot") or "")

            node = {
                "@id": page_id,
                "@type": "Page",
                "pageNumber": pno,
                "docType": doc_type,
                "source": source,
            }
            if text_len is not None:
                node["textLength"] = text_len
            if lot_guess:
                pid = ensure_parcel(lot_guess)
                node["aboutParcel"] = pid
            graph.append(node)

    # ============================================================
    # 2) Candidates (evidence)
    # ============================================================
    if include_candidates and df_candidates is not None and len(df_candidates) > 0:
        for rec in df_candidates.to_dict("records"):
            lot = _safe_str(rec.get("지번") or rec.get("property_key") or rec.get("lot") or "")
            field = _safe_str(rec.get("필드") or rec.get("field") or "")
            value = _safe_str(rec.get("값") or rec.get("value") or "")
            if not (lot and field and value):
                continue

            doc_type = _safe_str(rec.get("문서타입") or rec.get("docType") or "")
            pno = _safe_int(rec.get("페이지") or rec.get("page") or rec.get("pageNumber"))
            source = _safe_str(rec.get("소스") or rec.get("source") or "")
            priority = _safe_int(rec.get("우선순위") or rec.get("priority") or 999)

            pid = ensure_parcel(lot)

            key = f"{doc_id}|{lot}|{field}|{value}|{doc_type}|{pno or ''}|{source}|{priority or ''}"
            cid = f"{doc_id}#cand-{_sha1_12(key)}"

            node = {
                "@id": cid,
                "@type": "Candidate",
                "aboutParcel": pid,
                "field": field,
                "value": value,
                "docType": doc_type,
                "source": source,
            }
            if pno is not None:
                node["pageNumber"] = pno
                node["evidencePage"] = page_ids.get(pno, f"{doc_id}#page-{pno}")
            if priority is not None:
                node["priority"] = priority

            graph.append(node)
            doc_node["hasCandidate"].append(cid)

            cand_index.setdefault((lot, field, value), []).append(cid)

    # candidate가 없으면 키 제거(깔끔)
    if not doc_node["hasCandidate"]:
        doc_node.pop("hasCandidate", None)

    # ============================================================
    # 3) Final Facts
    # ============================================================
    if df_final is not None and len(df_final) > 0:
        for rec in df_final.to_dict("records"):
            lot = _safe_str(rec.get("지번") or rec.get("lot") or "")
            if not lot:
                continue

            site = _safe_str(rec.get("대지위치") or rec.get("siteAddress") or "")
            road = _safe_str(rec.get("도로명주소") or rec.get("roadAddress") or "")
            conf = _safe_int(rec.get("신뢰도(0-100)") or rec.get("confidence") or None)

            pid = ensure_parcel(lot, site, road, confidence=conf)

            # RegistryDocument(토지 등기)도 같이 생성(발급일/확인번호 있으면 사용)
            issue_date = _safe_str(rec.get("발급일") or rec.get("issueDate") or "")
            confirm_no = _safe_str(rec.get("문서확인번호") or rec.get("confirmNo") or "")
            _ = ensure_registry_for_lot(lot, issue_date=issue_date, confirm_no=confirm_no)

            fact_specs = [
                ("지목", rec.get("지목"), None, None),
                ("토지면적", rec.get("토지면적"), "㎡", parse_area_sqm(_safe_str(rec.get("토지면적")))),
                ("연면적", rec.get("연면적"), "㎡", parse_area_sqm(_safe_str(rec.get("연면적")))),
                ("용도지역", rec.get("용도지역"), None, None),
                ("최종소유자", rec.get("최종소유자"), None, None),
                ("발급일", rec.get("발급일"), None, None),
                ("문서확인번호", rec.get("문서확인번호"), None, None),
            ]

            for field, raw_val, unit, num in fact_specs:
                val = _safe_str(raw_val)
                if not val:
                    continue

                fid = f"{pid}#fact-{_sha1_12(field)}"
                node: Dict[str, Any] = {
                    "@id": fid,
                    "@type": "Fact",
                    "aboutParcel": pid,
                    "field": field,
                    "value": val,
                }
                if unit:
                    node["unit"] = unit
                if num is not None:
                    node["valueNumber"] = num
                if conf is not None:
                    node["confidence"] = conf

                supports = cand_index.get((lot, field, val), [])
                if supports:
                    node["supportedBy"] = supports

                graph.append(node)
                parcel_nodes[pid]["hasFact"].append(fid)

    # ============================================================
    # 4) Registry Tables -> RegistryEntry
    # ============================================================
    if registry_tables:
        # normalize keys
        def _to_section_type(key: str) -> str:
            k = (key or "").lower()
            if k in ("pyo", "표제부"):
                return "pyo"
            if k in ("gab", "갑구"):
                return "gab"
            if k in ("eul", "을구"):
                return "eul"
            return k or "unknown"

        # for issueDate/confirmNo, try take from df_final per lot
        issue_map: Dict[str, str] = {}
        confirm_map: Dict[str, str] = {}
        if df_final is not None and len(df_final) > 0:
            for rec in df_final.to_dict("records"):
                lot = _safe_str(rec.get("지번") or "")
                if not lot:
                    continue
                issue = _safe_str(rec.get("발급일") or "")
                confno = _safe_str(rec.get("문서확인번호") or "")
                if issue and lot not in issue_map:
                    issue_map[lot] = issue
                if confno and lot not in confirm_map:
                    confirm_map[lot] = confno

        for key, df in registry_tables.items():
            if df is None or len(df) == 0:
                continue
            section_type = _to_section_type(str(key))

            for rec in df.to_dict("records"):
                lot = _safe_str(rec.get("지번") or rec.get("lot") or "")
                if not lot:
                    continue

                rid = ensure_registry_for_lot(
                    lot,
                    issue_date=issue_map.get(lot, ""),
                    confirm_no=confirm_map.get(lot, ""),
                )
                sid = ensure_section(rid, section_type)

                # rank: 표제부는 표시번호, 갑/을은 순위번호
                rank = _safe_str(rec.get("표시번호") or rec.get("순위번호") or rec.get("rank") or "")
                purpose = _safe_str(rec.get("등기목적") or rec.get("purpose") or "")
                acceptance = _safe_str(rec.get("접수") or rec.get("acceptance") or "")
                cause = _safe_str(rec.get("등기원인") or rec.get("cause") or rec.get("등기원인및기타사항") or "")
                details = _safe_str(rec.get("권리자및기타사항") or rec.get("details") or "")
                pno = _safe_int(rec.get("페이지") or rec.get("page") or rec.get("pageNumber"))

                key_str = f"{rid}|{section_type}|{rank}|{purpose}|{acceptance}|{cause}|{details}|{pno or ''}"
                eid = f"{rid}#entry-{_sha1_12(key_str)}"

                node: Dict[str, Any] = {
                    "@id": eid,
                    "@type": "RegistryEntry",
                    "inSection": sid,
                    "rank": rank,
                }
                if purpose:
                    node["purpose"] = purpose
                if acceptance:
                    node["acceptance"] = acceptance
                if cause:
                    node["cause"] = cause
                if details:
                    node["details"] = details
                if pno is not None:
                    node["pageNumber"] = pno
                    node["evidencePage"] = page_ids.get(pno, f"{doc_id}#page-{pno}")

                graph.append(node)

                # section -> entry link
                for g in graph:
                    if g.get("@id") == sid:
                        g.setdefault("hasEntry", [])
                        g["hasEntry"].append(eid)
                        break

    # ============================================================
    # 5) Validation checks
    # ============================================================
    if df_checks is not None and len(df_checks) > 0:
        for rec in df_checks.to_dict("records"):
            lot = _safe_str(rec.get("지번") or rec.get("lot") or "")
            if not lot:
                continue
            pid = ensure_parcel(lot)

            check = _safe_str(rec.get("검증항목") or rec.get("checkName") or "check")
            result = _safe_str(rec.get("일치여부") or rec.get("result") or "")
            left_val = _safe_str(
                rec.get("좌값")
                or rec.get("토지소재지번")
                or rec.get("토지_소재지번(표제부위)")
                or rec.get("토지_소재지번")
                or rec.get("leftValue")
                or ""
            )
            right_val = _safe_str(
                rec.get("우값")
                or rec.get("표제부소재지번")
                or rec.get("표제부_소재지번")
                or rec.get("rightValue")
                or ""
            )

            key = f"{doc_id}|{lot}|{check}|{result}|{left_val}|{right_val}"
            vid = f"{pid}#val-{_sha1_12(key)}"
            node: Dict[str, Any] = {
                "@id": vid,
                "@type": "ValidationResult",
                "aboutParcel": pid,
                "checkName": check,
                "result": result,
            }
            if left_val:
                node["leftValue"] = left_val
            if right_val:
                node["rightValue"] = right_val

            graph.append(node)
            parcel_nodes[pid]["hasValidation"].append(vid)

    # doc_node cleanup: remove empty arrays
    if not doc_node.get("hasRegistry"):
        doc_node.pop("hasRegistry", None)
    if not doc_node.get("hasPage"):
        doc_node.pop("hasPage", None)
    if not doc_node.get("mentionsParcel"):
        doc_node.pop("mentionsParcel", None)

    return {"@context": default_context(), "@graph": graph}


def make_jsonld_bytes(jsonld_obj: Dict[str, Any], *, indent: int = 2) -> bytes:
    """
    JSON-LD dict -> UTF-8 bytes (streamlit download)
    """
    return json.dumps(jsonld_obj, ensure_ascii=False, indent=indent).encode("utf-8")


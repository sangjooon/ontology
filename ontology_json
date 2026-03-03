# -*- coding: utf-8 -*-
"""
ontology_json.py
------------------
DOVI(문서 비서) 프로토타입에서 추출된 결과(df_final/df_candidates/df_pages/등기표)를
"간단 온톨로지(그래프)" 형태의 JSON-LD로 변환하는 유틸 모듈입니다.

- RDF/OWL까지 바로 가지 않고, JSON-LD(@context + @graph)로
  '문서 -> 페이지 -> (토지/건물) -> 사실(Fact) -> 근거(Candidate)' 구조를 만듭니다.
- 나중에 그래프DB(Neptune/Blazegraph/GraphDB 등)로 넣을 때도 그대로 쓸 수 있습니다.

필수:
  pandas

사용 예시는 README 없이도 아래 함수 시그니처와 docstring 보면 바로 붙일 수 있게 작성했습니다.
"""

from __future__ import annotations

import json
import hashlib
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd


# ============================================================
# 0) 기본 네임스페이스 / 컨텍스트
# ============================================================
DEFAULT_ONTOLOGY_IRI = "https://example.org/dovi/ontology#"
DEFAULT_BASE_IRI = "urn:dovi:"  # 내부용 URN (원하면 너 도메인으로 바꿔도 됨)

DOVI_NS = "https://example.org/dovi#"
SCHEMA_NS = "https://schema.org/"
PROV_NS = "http://www.w3.org/ns/prov#"
XSD_NS = "http://www.w3.org/2001/XMLSchema#"


def default_context(ontology_iri: str = DEFAULT_ONTOLOGY_IRI) -> Dict[str, Any]:
    """
    JSON-LD @context.
    - 'dovi:' 접두어는 우리가 정의한 간단 온톨로지 네임스페이스
    - 'schema:'는 범용 메타데이터(파일명/생성일 등)
    - 'prov:'는 provenance(근거/파생) 표현 확장 여지
    """
    return {
        "@version": 1.1,
        "dovi": DOVI_NS,
        "schema": SCHEMA_NS,
        "prov": PROV_NS,
        "xsd": XSD_NS,
        "Ontology": "prov:Entity",

        # Common
        "id": "@id",
        "type": "@type",
        "fileName": "schema:name",
        "fileHash": "dovi:fileHash",
        "createdAt": {"@id": "schema:dateCreated", "@type": "xsd:dateTime"},
        "generator": "schema:generator",

        # Document / Page
        "Document": "dovi:Document",
        "Page": "dovi:Page",
        "hasPage": {"@id": "dovi:hasPage", "@type": "@id"},
        "pageNumber": {"@id": "dovi:pageNumber", "@type": "xsd:integer"},
        "docType": "dovi:docType",
        "source": "dovi:source",
        "textLength": {"@id": "dovi:textLength", "@type": "xsd:integer"},

        # Parcel / Building
        "Parcel": "dovi:Parcel",
        "Building": "dovi:Building",
        "mentionsParcel": {"@id": "dovi:mentionsParcel", "@type": "@id"},
        "mentionsBuilding": {"@id": "dovi:mentionsBuilding", "@type": "@id"},
        "aboutParcel": {"@id": "dovi:aboutParcel", "@type": "@id"},
        "aboutBuilding": {"@id": "dovi:aboutBuilding", "@type": "@id"},
        "lot": "dovi:lot",
        "siteAddress": "dovi:siteAddress",
        "roadAddress": "dovi:roadAddress",

        # Facts
        "Fact": "dovi:Fact",
        "hasFact": {"@id": "dovi:hasFact", "@type": "@id"},
        "field": "dovi:field",
        "value": "dovi:value",
        "valueNumber": {"@id": "dovi:valueNumber", "@type": "xsd:decimal"},
        "unit": "dovi:unit",
        "confidence": {"@id": "dovi:confidence", "@type": "xsd:integer"},
        "supportedBy": {"@id": "dovi:supportedBy", "@type": "@id"},

        # Extraction candidates (evidence)
        "Candidate": "dovi:Candidate",
        "hasCandidate": {"@id": "dovi:hasCandidate", "@type": "@id"},
        "priority": {"@id": "dovi:priority", "@type": "xsd:integer"},
        "evidencePage": {"@id": "dovi:evidencePage", "@type": "@id"},

        # Registry tables
        "RegistryEntry": "dovi:RegistryEntry",
        "hasRegistryEntry": {"@id": "dovi:hasRegistryEntry", "@type": "@id"},
        "section": "dovi:section",
        "rank": "dovi:rank",
        "purpose": "dovi:purpose",
        "acceptance": "dovi:acceptance",
        "cause": "dovi:cause",
        "details": "dovi:details",

        # Validation checks
        "ValidationResult": "dovi:ValidationResult",
        "hasValidation": {"@id": "dovi:hasValidation", "@type": "@id"},
        "checkName": "dovi:checkName",
        "result": "dovi:result",
        "leftValue": "dovi:leftValue",
        "rightValue": "dovi:rightValue",

        # Ontology node
        "ontologyIri": {"@id": "dovi:ontologyIri", "@type": "@id"},
        "ontologyVersion": "dovi:ontologyVersion",
    }


# ============================================================
# 1) Helper
# ============================================================
def _ensure_base(base_iri: str) -> str:
    base = (base_iri or "").strip()
    if not base:
        return DEFAULT_BASE_IRI
    if base[-1] not in [":", "#", "/"]:
        base += ":"
    return base


def _sha1_12(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()[:12]


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


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_area_sqm(text: str) -> Optional[float]:
    """
    '1,540 ㎡' / '1540m2' / '1540' 같은 텍스트에서 숫자만 뽑아 sqm로 저장.
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


# ============================================================
# 2) JSON-LD Graph Builder
# ============================================================
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
    ontology_iri: str = DEFAULT_ONTOLOGY_IRI,
    ontology_version: str = "0.1",
    generator_name: str = "DOVI-Prototype",
    include_candidates: bool = True,
) -> Dict[str, Any]:
    """
    추출 결과를 JSON-LD 그래프로 변환.

    입력 DataFrame 기대 컬럼(유연하게 처리):
    - df_final: ['지번', '대지위치', '도로명주소', '지목', '토지면적', '연면적', '용도지역', '최종소유자', '발급일', '문서확인번호', '신뢰도(0-100)', ...]
    - df_candidates: ['지번', '필드', '값', '문서타입', '페이지', '소스', '우선순위', ...]
    - df_pages: ['페이지', '문서타입', '소스', '지번(추정)', '텍스트길이', ...]
    - registry_tables: {'pyo': df, 'gab': df, 'eul': df} 형태(각 df에 '지번' 필수)
    - df_checks: (선택) 소재지번 일치여부 등 검증 결과 테이블

    반환:
      dict(JSON-LD) = {"@context": ..., "@graph": [...]}
    """
    base = _ensure_base(base_iri)
    doc_id = f"{base}document:{file_hash}"

    context = default_context(ontology_iri)

    graph: List[Dict[str, Any]] = []

    # Ontology node(선택): 문서 그래프가 어떤 온톨로지에 기반하는지
    ontology_node = {
        "@id": f"{doc_id}#ontology",
        "@type": "Ontology",
        "ontologyIri": ontology_iri,
        "ontologyVersion": ontology_version,
    }
    graph.append(ontology_node)

    # Document node
    doc_node: Dict[str, Any] = {
        "@id": doc_id,
        "@type": "Document",
        "fileName": file_name,
        "fileHash": file_hash,
        "createdAt": _now_iso(),
        "generator": generator_name,
        "hasPage": [],
        "mentionsParcel": [],
        "hasCandidate": [],
    }
    graph.append(doc_node)

    # 내부 캐시(중복 방지)
    parcel_id_by_key: Dict[Tuple[str, str], str] = {}  # (lot, siteAddressNormalized) -> parcel_id
    parcel_nodes: Dict[str, Dict[str, Any]] = {}
    page_ids: Dict[int, str] = {}
    candidate_ids: Dict[Tuple[str, str, str, str, str, str], str] = {}  # 안정 해시 키 -> id

    def get_parcel_id(lot: str, site_addr: str = "", road_addr: str = "") -> str:
        lot_n = _safe_str(lot)
        site_n = re.sub(r"\s+", " ", _safe_str(site_addr))
        road_n = re.sub(r"\s+", " ", _safe_str(road_addr))
        # lot만 쓰면 타 지역 충돌 위험 → 주소가 있으면 섞어 해시
        key_str = f"{site_n}|{road_n}|{lot_n}"
        pid = f"{base}parcel:{_sha1_12(key_str)}"
        parcel_id_by_key[(lot_n, site_n)] = pid
        return pid

    def ensure_parcel_node(lot: str, site_addr: str = "", road_addr: str = "", *, confidence: Optional[int] = None) -> str:
        pid = get_parcel_id(lot, site_addr, road_addr)
        if pid in parcel_nodes:
            # confidence가 더 높은 값이 들어오면 갱신
            if confidence is not None:
                old = parcel_nodes[pid].get("confidence")
                if old is None or (isinstance(old, int) and confidence > old):
                    parcel_nodes[pid]["confidence"] = confidence
            return pid

        node: Dict[str, Any] = {
            "@id": pid,
            "@type": "Parcel",
            "lot": _safe_str(lot),
            "siteAddress": _safe_str(site_addr),
            "roadAddress": _safe_str(road_addr),
            "hasFact": [],
            "hasRegistryEntry": [],
            "hasValidation": [],
        }
        if confidence is not None:
            node["confidence"] = confidence

        parcel_nodes[pid] = node
        graph.append(node)

        # Document -> Parcel 링크
        if pid not in doc_node["mentionsParcel"]:
            doc_node["mentionsParcel"].append(pid)

        return pid

    # ========================================================
    # A) Page nodes
    # ========================================================
    if df_pages is not None and len(df_pages) > 0:
        for rec in df_pages.to_dict("records"):
            page_no = _safe_int(rec.get("페이지") or rec.get("page") or rec.get("pageNumber"))
            if page_no is None:
                continue
            page_id = f"{doc_id}#page-{page_no}"
            page_ids[page_no] = page_id
            doc_node["hasPage"].append(page_id)

            pk = _safe_str(rec.get("지번(추정)") or rec.get("지번") or rec.get("property_key") or "")
            doc_type = _safe_str(rec.get("문서타입") or rec.get("docType") or "")
            source = _safe_str(rec.get("소스") or rec.get("source") or "")
            text_len = _safe_int(rec.get("텍스트길이") or rec.get("textLength") or "")

            page_node: Dict[str, Any] = {
                "@id": page_id,
                "@type": "Page",
                "pageNumber": page_no,
                "docType": doc_type,
                "source": source,
            }
            if text_len is not None:
                page_node["textLength"] = text_len

            # page가 특정 지번을 가리킨다면 aboutParcel 링크도 걸어준다(있으면)
            if pk:
                # site_addr는 여기서 모름(후에 df_final에서 채워질 수 있음) → 일단 lot만으로 parcel 생성
                pid = ensure_parcel_node(pk)
                page_node["aboutParcel"] = pid

            graph.append(page_node)

    # ========================================================
    # B) Candidate nodes + index
    # ========================================================
    cand_index: Dict[Tuple[str, str, str], List[str]] = {}  # (lot, field, value) -> [cand_id]

    if include_candidates and df_candidates is not None and len(df_candidates) > 0:
        for rec in df_candidates.to_dict("records"):
            lot = _safe_str(rec.get("지번") or rec.get("lot") or rec.get("property_key") or "")
            field = _safe_str(rec.get("필드") or rec.get("field") or "")
            value = _safe_str(rec.get("값") or rec.get("value") or "")
            doc_type = _safe_str(rec.get("문서타입") or rec.get("docType") or "")
            page_no = _safe_int(rec.get("페이지") or rec.get("page") or rec.get("pageNumber"))
            source = _safe_str(rec.get("소스") or rec.get("source") or "")
            priority = _safe_int(rec.get("우선순위") or rec.get("priority") or 999)

            if not lot or not field or not value:
                continue

            # parcel 노드 최소 생성(후에 df_final에서 더 채움)
            pid = ensure_parcel_node(lot)

            # 안정 ID
            key = (doc_id, lot, field, value, doc_type, str(page_no or ""))
            cid = f"{doc_id}#cand-{_sha1_12('|'.join(key))}"
            candidate_ids[key] = cid

            node: Dict[str, Any] = {
                "@id": cid,
                "@type": "Candidate",
                "aboutParcel": pid,
                "field": field,
                "value": value,
                "docType": doc_type,
                "source": source,
            }
            if page_no is not None:
                node["pageNumber"] = page_no
                # page node 연결
                if page_no in page_ids:
                    node["evidencePage"] = page_ids[page_no]
                else:
                    node["evidencePage"] = f"{doc_id}#page-{page_no}"
            if priority is not None:
                node["priority"] = priority

            graph.append(node)
            doc_node["hasCandidate"].append(cid)

            cand_index.setdefault((lot, field, value), []).append(cid)

    # ========================================================
    # C) Final facts (df_final -> Parcel + Fact)
    # ========================================================
    if df_final is not None and len(df_final) > 0:
        for rec in df_final.to_dict("records"):
            lot = _safe_str(rec.get("지번") or rec.get("lot") or "")
            if not lot:
                continue

            site_addr = _safe_str(rec.get("대지위치") or rec.get("siteAddress") or "")
            road_addr = _safe_str(rec.get("도로명주소") or rec.get("roadAddress") or "")

            conf = _safe_int(rec.get("신뢰도(0-100)") or rec.get("confidence") or None)

            pid = ensure_parcel_node(lot, site_addr, road_addr, confidence=conf)

            # parcel 주요 속성도 업데이트
            pnode = parcel_nodes.get(pid)
            if pnode is not None:
                if site_addr:
                    pnode["siteAddress"] = site_addr
                if road_addr:
                    pnode["roadAddress"] = road_addr

            # 사실(Fact)로 저장할 필드들
            fact_fields = [
                ("지목", rec.get("지목"), None, None),
                ("토지면적", rec.get("토지면적"), "㎡", parse_area_sqm(_safe_str(rec.get("토지면적")))),
                ("연면적", rec.get("연면적"), "㎡", parse_area_sqm(_safe_str(rec.get("연면적")))),
                ("용도지역", rec.get("용도지역"), None, None),
                ("최종소유자", rec.get("최종소유자"), None, None),
                ("발급일", rec.get("발급일"), None, None),
                ("문서확인번호", rec.get("문서확인번호"), None, None),
            ]

            for f_name, raw_val, unit, num in fact_fields:
                val = _safe_str(raw_val)
                if not val:
                    continue

                fact_id = f"{pid}#fact-{_sha1_12(f_name)}"
                fact_node: Dict[str, Any] = {
                    "@id": fact_id,
                    "@type": "Fact",
                    "aboutParcel": pid,
                    "field": f_name,
                    "value": val,
                }
                if unit:
                    fact_node["unit"] = unit
                if num is not None:
                    fact_node["valueNumber"] = num
                if conf is not None:
                    fact_node["confidence"] = conf

                # 근거 후보 연결(같은 lot/field/value)
                supports = cand_index.get((lot, f_name, val), [])
                if supports:
                    fact_node["supportedBy"] = supports

                graph.append(fact_node)

                # parcel -> fact 링크
                parcel_nodes[pid]["hasFact"].append(fact_id)

    # ========================================================
    # D) Registry tables (표제부/갑구/을구) -> RegistryEntry
    # ========================================================
    if registry_tables:
        # key 이름은 유연하게: 'pyo'/'표제부', 'gab'/'갑구', 'eul'/'을구'
        mapping = {
            "pyo": "표제부",
            "표제부": "표제부",
            "gab": "갑구",
            "갑구": "갑구",
            "eul": "을구",
            "을구": "을구",
        }
        for k, df in registry_tables.items():
            if df is None or len(df) == 0:
                continue
            section = mapping.get(k, str(k))

            for rec in df.to_dict("records"):
                lot = _safe_str(rec.get("지번") or rec.get("lot") or "")
                if not lot:
                    continue
                pid = ensure_parcel_node(lot)

                # 표시번호/순위번호 모두 rank로
                rank = _safe_str(rec.get("표시번호") or rec.get("순위번호") or rec.get("rank") or "")
                purpose = _safe_str(rec.get("등기목적") or rec.get("purpose") or "")
                acceptance = _safe_str(rec.get("접수") or rec.get("acceptance") or "")
                cause = _safe_str(rec.get("등기원인") or rec.get("cause") or "")
                details = _safe_str(rec.get("권리자및기타사항") or rec.get("details") or "")
                page_no = _safe_int(rec.get("페이지") or rec.get("page") or rec.get("pageNumber"))

                key_str = f"{pid}|{section}|{rank}|{purpose}|{acceptance}|{cause}|{details}|{page_no or ''}"
                eid = f"{pid}#reg-{_sha1_12(key_str)}"

                node: Dict[str, Any] = {
                    "@id": eid,
                    "@type": "RegistryEntry",
                    "aboutParcel": pid,
                    "section": section,
                }
                if rank:
                    node["rank"] = rank
                if purpose:
                    node["purpose"] = purpose
                if acceptance:
                    node["acceptance"] = acceptance
                if cause:
                    node["cause"] = cause
                if details:
                    node["details"] = details
                if page_no is not None:
                    node["pageNumber"] = page_no
                    node["evidencePage"] = page_ids.get(page_no, f"{doc_id}#page-{page_no}")

                graph.append(node)
                parcel_nodes[pid]["hasRegistryEntry"].append(eid)

    # ========================================================
    # E) Validation checks (예: 소재지번 일치)
    # ========================================================
    if df_checks is not None and len(df_checks) > 0:
        for rec in df_checks.to_dict("records"):
            lot = _safe_str(rec.get("지번") or rec.get("lot") or rec.get("property_key") or "")
            if not lot:
                continue
            pid = ensure_parcel_node(lot)

            check_name = _safe_str(rec.get("검증항목") or rec.get("checkName") or "lot_consistency_check")
            result = _safe_str(rec.get("일치여부") or rec.get("result") or "")
            left_val = _safe_str(rec.get("좌값") or rec.get("토지소재지번") or rec.get("leftValue") or "")
            right_val = _safe_str(rec.get("우값") or rec.get("표제부소재지번") or rec.get("rightValue") or "")

            key_str = f"{pid}|{check_name}|{result}|{left_val}|{right_val}"
            vid = f"{pid}#val-{_sha1_12(key_str)}"

            node = {
                "@id": vid,
                "@type": "ValidationResult",
                "aboutParcel": pid,
                "checkName": check_name,
                "result": result,
            }
            if left_val:
                node["leftValue"] = left_val
            if right_val:
                node["rightValue"] = right_val

            graph.append(node)
            parcel_nodes[pid]["hasValidation"].append(vid)

    # 문서에 candidate가 하나도 없으면 키 삭제(깔끔하게)
    if not doc_node["hasCandidate"]:
        doc_node.pop("hasCandidate", None)

    return {"@context": context, "@graph": graph}


def make_jsonld_bytes(jsonld_obj: Dict[str, Any], *, indent: int = 2) -> bytes:
    """
    JSON-LD dict -> UTF-8 bytes (다운로드용)
    """
    return json.dumps(jsonld_obj, ensure_ascii=False, indent=indent).encode("utf-8")

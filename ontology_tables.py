# -*- coding: utf-8 -*-
"""
ontology_tables.py
------------------
"PDF의 모든 표를 엑셀로 정리"하는 파이프라인을 위해,
표 추출 결과를 JSON-LD(온톨로지 그래프)로 저장하는 모듈.

핵심 철학:
- OWL/RDF를 바로 강제하지 않고, JSON-LD로 '그래프 구조'를 먼저 잡는다.
- Document -> Page -> Table -> (Row -> Cell) 구조
- provenance(근거)를 위해 Table/Cell에 bbox를 포함할 수 있다(옵션).
- 추출 파라미터(config)도 함께 저장해서 재현 가능하게 한다.

이 파일은 "데이터를 채우는 파일"이 아니라,
app.py에서 생성된 테이블 결과를 받아 JSON-LD로 변환하는 '코드'입니다.
"""

from __future__ import annotations

import json
import hashlib
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple


# 네임스페이스(원하면 너 도메인으로 교체)
DOVI_NS = "https://example.org/dovi#"
SCHEMA_NS = "https://schema.org/"
PROV_NS = "http://www.w3.org/ns/prov#"
XSD_NS = "http://www.w3.org/2001/XMLSchema#"

DEFAULT_BASE_IRI = "urn:dovi:"


def _ensure_base(base_iri: str) -> str:
    base = (base_iri or "").strip()
    if not base:
        return DEFAULT_BASE_IRI
    if base[-1] not in [":", "#", "/"]:
        base += ":"
    return base


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _sha1_12(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()[:12]


def default_context() -> Dict[str, Any]:
    """
    JSON-LD @context
    - dovi: 우리 앱에서 쓰는 간단 온톨로지 네임스페이스
    - schema: 범용 메타데이터(파일명/생성일 등)
    - prov: provenance 확장 여지
    """
    return {
        "@version": 1.1,
        "dovi": DOVI_NS,
        "schema": SCHEMA_NS,
        "prov": PROV_NS,
        "xsd": XSD_NS,

        # Common
        "id": "@id",
        "type": "@type",
        "fileName": "schema:name",
        "fileHash": "dovi:fileHash",
        "createdAt": {"@id": "schema:dateCreated", "@type": "xsd:dateTime"},
        "generator": "schema:generator",

        # Document/Page
        "Document": "dovi:Document",
        "Page": "dovi:Page",
        "hasPage": {"@id": "dovi:hasPage", "@type": "@id"},
        "pageNumber": {"@id": "dovi:pageNumber", "@type": "xsd:integer"},
        "hasTable": {"@id": "dovi:hasTable", "@type": "@id"},

        # Table/Row/Cell
        "Table": "dovi:Table",
        "TableRow": "dovi:TableRow",
        "TableCell": "dovi:TableCell",
        "tableId": "dovi:tableId",
        "sheetName": "dovi:sheetName",
        "pageStart": {"@id": "dovi:pageStart", "@type": "xsd:integer"},
        "pageEnd": {"@id": "dovi:pageEnd", "@type": "xsd:integer"},
        "nRows": {"@id": "dovi:nRows", "@type": "xsd:integer"},
        "nCols": {"@id": "dovi:nCols", "@type": "xsd:integer"},
        "bbox": "dovi:bbox",

        "hasRow": {"@id": "dovi:hasRow", "@type": "@id"},
        "rowIndex": {"@id": "dovi:rowIndex", "@type": "xsd:integer"},
        "hasCell": {"@id": "dovi:hasCell", "@type": "@id"},
        "colIndex": {"@id": "dovi:colIndex", "@type": "xsd:integer"},
        "text": "dovi:text",

        # Config
        "ExtractionConfig": "dovi:ExtractionConfig",
        "config": "dovi:config",
    }


def build_tables_jsonld(
    *,
    file_name: str,
    file_hash: str,
    tables: List[Any],
    include_cells: bool = False,
    base_iri: str = DEFAULT_BASE_IRI,
    generator_name: str = "DOVI-TableExtractor",
    config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    tables: app.py의 ExtractedTable 객체 리스트를 그대로 받는 전제
      - 필요한 속성: table_id, sheet_name, page_start, page_end, bbox, n_rows, n_cols, df
      - (옵션) cell_bboxes

    include_cells=False면 Table 메타데이터 중심(가벼움)
    include_cells=True면 Row/Cell까지 내려가며 텍스트를 그래프로 저장(파일 커짐)

    반환:
      {"@context": ..., "@graph": [...]}
    """
    base = _ensure_base(base_iri)
    doc_id = f"{base}document:{file_hash}"

    graph: List[Dict[str, Any]] = []

    # Document node
    doc_node: Dict[str, Any] = {
        "@id": doc_id,
        "@type": "Document",
        "fileName": file_name,
        "fileHash": file_hash,
        "createdAt": _now_iso(),
        "generator": generator_name,
        "hasPage": [],
        "hasTable": [],
    }
    graph.append(doc_node)

    # Config node (재현용)
    if config is not None:
        cfg_id = f"{doc_id}#config"
        graph.append({"@id": cfg_id, "@type": "ExtractionConfig", "config": config})
        # 문서에 연결(간단히 확장필드로)
        doc_node["config"] = cfg_id

    # Page 노드 생성(테이블이 걸쳐있는 페이지 범위만)
    pages_needed = set()
    for t in tables:
        pages_needed.add(int(t.page_start))
        pages_needed.add(int(t.page_end))
        for p in range(int(t.page_start), int(t.page_end) + 1):
            pages_needed.add(p)

    page_ids: Dict[int, str] = {}
    for p in sorted(pages_needed):
        pid = f"{doc_id}#page-{p}"
        page_ids[p] = pid
        doc_node["hasPage"].append(pid)
        graph.append({"@id": pid, "@type": "Page", "pageNumber": p, "hasTable": []})

    # Table/Row/Cell 노드
    for t in tables:
        tid = f"{doc_id}#table-{t.table_id}"
        doc_node["hasTable"].append(tid)

        table_node: Dict[str, Any] = {
            "@id": tid,
            "@type": "Table",
            "tableId": t.table_id,
            "sheetName": t.sheet_name,
            "pageStart": int(t.page_start),
            "pageEnd": int(t.page_end),
            "nRows": int(t.n_rows),
            "nCols": int(t.n_cols),
            "bbox": [float(x) for x in t.bbox] if getattr(t, "bbox", None) else None,
        }
        # bbox None 제거
        if table_node.get("bbox") is None:
            table_node.pop("bbox", None)

        # page -> table 연결
        for p in range(int(t.page_start), int(t.page_end) + 1):
            if p in page_ids:
                # page node 찾아서 hasTable에 추가
                for node in graph:
                    if node.get("@id") == page_ids[p]:
                        node.setdefault("hasTable", []).append(tid)
                        break

        graph.append(table_node)

        if include_cells:
            # Row/Cell까지 저장
            table_node["hasRow"] = []
            df = t.df
            for r in range(df.shape[0]):
                rid = f"{tid}#row-{r}"
                table_node["hasRow"].append(rid)
                row_node = {"@id": rid, "@type": "TableRow", "rowIndex": r, "hasCell": []}
                graph.append(row_node)

                for c in range(df.shape[1]):
                    val = df.iat[r, c]
                    txt = "" if val is None else str(val)
                    cid = f"{rid}#cell-{c}"
                    row_node["hasCell"].append(cid)
                    cell_node: Dict[str, Any] = {
                        "@id": cid,
                        "@type": "TableCell",
                        "rowIndex": r,
                        "colIndex": c,
                        "text": txt,
                    }

                    # cell bbox가 있으면 포함(있는 경우만)
                    cb = getattr(t, "cell_bboxes", None)
                    if cb and (r, c) in cb:
                        cell_node["bbox"] = [float(x) for x in cb[(r, c)]]

                    graph.append(cell_node)

    return {"@context": default_context(), "@graph": graph}


def make_jsonld_bytes(jsonld_obj: Dict[str, Any], *, indent: int = 2) -> bytes:
    return json.dumps(jsonld_obj, ensure_ascii=False, indent=indent).encode("utf-8")

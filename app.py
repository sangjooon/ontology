# -*- coding: utf-8 -*-
"""
문서 비서 📄  (Naver CLOVA OCR: Table Detection -> Excel)
-------------------------------------------------------
요구사항:
- 네이버 CLOVA OCR "General OCR + enableTableDetection=true" 사용
- PDF의 모든 페이지를 OCR 처리
- OCR 응답의 images[].tables[].cells 정보를 이용해 표를 '그대로' 엑셀로 저장
  - rowIndex/columnIndex 기반으로 셀 배치
  - rowSpan/columnSpan을 Excel 병합(merge cells)로 반영(옵션)

주의:
- 표 추출은 "도메인 설정에서 '표 추출 여부'를 ON" 해야 동작합니다.
  (OFF인 상태에서 enableTableDetection=true로 호출하면 에러가 납니다.)

requirements.txt (추천)
----------------------
streamlit
requests
pandas
openpyxl
pypdf
PyPDF2
"""

from __future__ import annotations

import io
import json
import time
import uuid
import hashlib
import re
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Tuple

import requests
import pandas as pd
import streamlit as st
from openpyxl import Workbook
from openpyxl.styles import Alignment
from openpyxl.utils import get_column_letter


# ============================================================
# 0) 앱 설정
# ============================================================
APP_TITLE = "문서 비서📄 dev — 네이버 표추출 OCR → 엑셀"
APP_VERSION = "v0.4.0"

DEFAULT_PASSWORD = "alohomora"  # 데모용

# Naver General OCR: PDF는 API 호출당 최대 10페이지 지원(공식 문서)
MAX_PAGES_PER_REQUEST = 10


# ============================================================
# 1) PDF 유틸 (pypdf/PyPDF2)
# ============================================================
def _import_pypdf():
    try:
        from pypdf import PdfReader, PdfWriter  # type: ignore
        return PdfReader, PdfWriter
    except Exception:
        from PyPDF2 import PdfReader, PdfWriter  # type: ignore
        return PdfReader, PdfWriter


def split_pdf_into_chunks(pdf_bytes: bytes, chunk_size: int) -> List[Tuple[bytes, int, int]]:
    """
    PDF bytes를 chunk_size 페이지 단위로 쪼개서
    [(chunk_pdf_bytes, start_page(1-indexed), end_page(1-indexed)), ...] 반환
    """
    PdfReader, PdfWriter = _import_pypdf()
    reader = PdfReader(io.BytesIO(pdf_bytes))
    total = len(reader.pages)
    out: List[Tuple[bytes, int, int]] = []

    for start in range(0, total, chunk_size):
        end = min(start + chunk_size, total)
        w = PdfWriter()
        for i in range(start, end):
            w.add_page(reader.pages[i])
        buf = io.BytesIO()
        w.write(buf)
        out.append((buf.getvalue(), start + 1, end))
    return out


# ============================================================
# 2) 네이버 OCR 호출 (enableTableDetection=True)
# ============================================================
def call_naver_ocr_table(
    file_bytes: bytes,
    api_url: str,
    secret_key: str,
    *,
    lang: str = "ko",
    timeout: int = 180,
) -> Dict[str, Any]:
    """
    네이버 CLOVA OCR General 호출 (multipart/form-data) + 표 추출 활성화.
    - api_url: 도메인 Invoke URL의 /general 엔드포인트
    """
    request_json = {
        "images": [{"format": "pdf", "name": "upload"}],
        "requestId": str(uuid.uuid4()),
        "version": "V2",
        "timestamp": int(round(time.time() * 1000)),
        "lang": lang,
        "enableTableDetection": True,  # ✅ 표 추출
    }

    payload = {"message": json.dumps(request_json, ensure_ascii=False)}
    headers = {"X-OCR-SECRET": secret_key}

    files = {"file": ("upload.pdf", file_bytes, "application/pdf")}

    try:
        r = requests.post(api_url, headers=headers, data=payload, files=files, timeout=timeout)
        ok = r.status_code == 200
        j = None
        if "application/json" in (r.headers.get("Content-Type") or ""):
            try:
                j = r.json()
            except Exception:
                j = None
        return {"ok": ok, "status_code": r.status_code, "text": (r.text or "")[:2000], "json": j}
    except Exception as e:
        return {"ok": False, "status_code": None, "error": str(e), "text": ""}


# ============================================================
# 3) OCR 테이블 파싱
# ============================================================
@dataclass
class ParsedCell:
    row: int
    col: int
    row_span: int
    col_span: int
    text: str
    bbox: Optional[Tuple[float, float, float, float]] = None
    conf: Optional[float] = None


@dataclass
class ParsedTable:
    table_id: str
    sheet_name: str
    page_no: int
    table_index_on_page: int
    bbox: Optional[Tuple[float, float, float, float]]
    n_rows: int
    n_cols: int
    grid: List[List[str]]  # [r][c]
    merges: List[Tuple[int, int, int, int]]  # (r0,c0,r1,c1) 0-based inclusive
    cells: List[ParsedCell]


def _bbox_from_vertices(vertices: List[Dict[str, Any]]) -> Optional[Tuple[float, float, float, float]]:
    if not vertices:
        return None
    try:
        xs = [float(v.get("x", 0)) for v in vertices]
        ys = [float(v.get("y", 0)) for v in vertices]
        return (min(xs), min(ys), max(xs), max(ys))
    except Exception:
        return None


def _cell_text(cell: Dict[str, Any]) -> str:
    """
    cellTextLines -> cellWords -> inferText 를 join해서 셀 텍스트 복원
    """
    lines = cell.get("cellTextLines") or []
    out_lines: List[str] = []
    for ln in lines:
        words = ln.get("cellWords") or []
        wtxt = " ".join((w.get("inferText") or "").strip() for w in words if (w.get("inferText") or "").strip())
        wtxt = re.sub(r"\s+", " ", wtxt).strip()
        if wtxt:
            out_lines.append(wtxt)
    # 줄바꿈 유지
    return "\n".join(out_lines).strip()


def parse_tables_from_ocr_json(
    ocr_json: Dict[str, Any],
    *,
    page_numbers: List[int],
    sheet_prefix: str = "",
) -> List[ParsedTable]:
    """
    Naver OCR 응답(JSON)에서 tables를 파싱해 ParsedTable 리스트 반환.
    - page_numbers: 호출한 PDF chunk의 원본 페이지 번호 매핑(이미지 인덱스 -> page_no)
    """
    tables_out: List[ParsedTable] = []
    images = ocr_json.get("images", []) if isinstance(ocr_json, dict) else []

    for img_idx, img in enumerate(images):
        page_no = page_numbers[img_idx] if img_idx < len(page_numbers) else (img_idx + 1)

        # 표 추출 결과는 images[].tables 로 제공됨 (enableTableDetection=true)
        tables = img.get("tables") or []
        if not tables:
            continue

        for t_idx, t in enumerate(tables, start=1):
            cells_raw = t.get("cells") or []
            parsed_cells: List[ParsedCell] = []

            max_r = 0
            max_c = 0
            merges: List[Tuple[int, int, int, int]] = []

            # 테이블 bbox
            t_bbox = None
            bp = (t.get("boundingPoly") or {}).get("vertices") or []
            t_bbox = _bbox_from_vertices(bp)

            for c in cells_raw:
                r = int(c.get("rowIndex", 0))
                col = int(c.get("columnIndex", 0))
                rspan = int(c.get("rowSpan", 1) or 1)
                cspan = int(c.get("columnSpan", 1) or 1)

                txt = _cell_text(c)

                c_bbox = None
                cbp = (c.get("boundingPoly") or {}).get("vertices") or []
                c_bbox = _bbox_from_vertices(cbp)

                conf = c.get("inferConfidence")
                try:
                    conf_val = float(conf) if conf is not None else None
                except Exception:
                    conf_val = None

                parsed_cells.append(
                    ParsedCell(row=r, col=col, row_span=rspan, col_span=cspan, text=txt, bbox=c_bbox, conf=conf_val)
                )

                max_r = max(max_r, r + rspan)
                max_c = max(max_c, col + cspan)

                if rspan > 1 or cspan > 1:
                    merges.append((r, col, r + rspan - 1, col + cspan - 1))

            n_rows = max_r
            n_cols = max_c

            # grid 채우기
            grid = [["" for _ in range(n_cols)] for _ in range(n_rows)]
            for pc in parsed_cells:
                # top-left에만 값 기입
                if 0 <= pc.row < n_rows and 0 <= pc.col < n_cols:
                    grid[pc.row][pc.col] = pc.text

            table_id = f"p{page_no}_t{t_idx}"
            sheet_name = f"{sheet_prefix}p{page_no}_t{t_idx}" if sheet_prefix else f"p{page_no}_t{t_idx}"
            # Excel sheet name 제한 31
            sheet_name = sheet_name[:31]

            tables_out.append(
                ParsedTable(
                    table_id=table_id,
                    sheet_name=sheet_name,
                    page_no=page_no,
                    table_index_on_page=t_idx,
                    bbox=t_bbox,
                    n_rows=n_rows,
                    n_cols=n_cols,
                    grid=grid,
                    merges=merges,
                    cells=parsed_cells,
                )
            )

    return tables_out


# ============================================================
# 4) Excel 생성 (merge cells 반영)
# ============================================================
def _safe_sheet_name(name: str, used: set) -> str:
    n = re.sub(r"[\[\]\*:/\\\?]", "_", name)  # Excel 금지문자 대체
    n = n[:31] if len(n) > 31 else n
    if n not in used:
        used.add(n)
        return n
    # 중복 방지
    i = 2
    while True:
        cand = f"{n[:28]}_{i}" if len(n) >= 28 else f"{n}_{i}"
        cand = cand[:31]
        if cand not in used:
            used.add(cand)
            return cand
        i += 1


def tables_to_excel_bytes(
    tables: List[ParsedTable],
    *,
    merge_cells: bool = True,
    autosize: bool = True,
) -> bytes:
    wb = Workbook()
    ws_index = wb.active
    ws_index.title = "index"

    # index header
    ws_index.append(["sheet_name", "page", "table_idx", "rows", "cols", "bbox(x0,y0,x1,y1)"])

    used = {"index"}
    wrap = Alignment(wrap_text=True, vertical="top")

    # index rows
    for t in tables:
        bbox_s = ""
        if t.bbox:
            bbox_s = f"{t.bbox[0]:.1f},{t.bbox[1]:.1f},{t.bbox[2]:.1f},{t.bbox[3]:.1f}"
        ws_index.append([t.sheet_name, t.page_no, t.table_index_on_page, t.n_rows, t.n_cols, bbox_s])

    # table sheets
    for t in tables:
        sname = _safe_sheet_name(t.sheet_name, used)
        ws = wb.create_sheet(sname)

        # write grid (1-based in Excel)
        for r in range(t.n_rows):
            row_vals = t.grid[r]
            ws.append(row_vals)

        # apply wrap
        for row in ws.iter_rows(min_row=1, max_row=t.n_rows, min_col=1, max_col=t.n_cols):
            for cell in row:
                cell.alignment = wrap

        # merge cells
        if merge_cells:
            for (r0, c0, r1, c1) in t.merges:
                # openpyxl: 1-based
                ws.merge_cells(
                    start_row=r0 + 1,
                    start_column=c0 + 1,
                    end_row=r1 + 1,
                    end_column=c1 + 1,
                )

        # autosize columns (rough)
        if autosize:
            for c in range(1, t.n_cols + 1):
                col_letter = get_column_letter(c)
                max_len = 0
                for r in range(1, t.n_rows + 1):
                    v = ws.cell(row=r, column=c).value
                    if v is None:
                        continue
                    s = str(v)
                    max_len = max(max_len, max((len(line) for line in s.splitlines()), default=len(s)))
                # 폭 제한
                width = min(60, max(10, max_len + 2))
                ws.column_dimensions[col_letter].width = width

    # 저장
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


# ============================================================
# 5) (선택) JSON-LD(온톨로지) — 표 구조 저장
# ============================================================
def build_tables_jsonld(
    *,
    file_name: str,
    file_hash: str,
    tables: List[ParsedTable],
    base_iri: str = "urn:dovi:",
    generator: str = "DOVI-NaverTable",
    include_cells: bool = False,
    include_bboxes: bool = True,
    config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Document -> Page -> Table -> (Cell) 그래프(JSON-LD) 생성.
    """
    base = base_iri if base_iri.endswith(("/", "#", ":")) else base_iri + ":"
    doc_id = f"{base}document:{file_hash}"

    context = {
        "@version": 1.1,
        "dovi": "https://example.org/dovi#",
        "schema": "https://schema.org/",
        "xsd": "http://www.w3.org/2001/XMLSchema#",

        "id": "@id",
        "type": "@type",
        "Document": "dovi:Document",
        "Page": "dovi:Page",
        "Table": "dovi:Table",
        "TableCell": "dovi:TableCell",
        "fileName": "schema:name",
        "fileHash": "dovi:fileHash",
        "createdAt": {"@id": "schema:dateCreated", "@type": "xsd:dateTime"},
        "generator": "schema:generator",

        "hasPage": {"@id": "dovi:hasPage", "@type": "@id"},
        "pageNumber": {"@id": "dovi:pageNumber", "@type": "xsd:integer"},
        "hasTable": {"@id": "dovi:hasTable", "@type": "@id"},

        "tableId": "dovi:tableId",
        "sheetName": "dovi:sheetName",
        "nRows": {"@id": "dovi:nRows", "@type": "xsd:integer"},
        "nCols": {"@id": "dovi:nCols", "@type": "xsd:integer"},
        "bbox": "dovi:bbox",
        "config": "dovi:config",

        "rowIndex": {"@id": "dovi:rowIndex", "@type": "xsd:integer"},
        "colIndex": {"@id": "dovi:colIndex", "@type": "xsd:integer"},
        "rowSpan": {"@id": "dovi:rowSpan", "@type": "xsd:integer"},
        "colSpan": {"@id": "dovi:colSpan", "@type": "xsd:integer"},
        "text": "dovi:text",
        "confidence": "dovi:confidence",
        "belongsToTable": {"@id": "dovi:belongsToTable", "@type": "@id"},
    }

    def now_iso():
        return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    graph: List[Dict[str, Any]] = []

    doc_node: Dict[str, Any] = {
        "@id": doc_id,
        "@type": "Document",
        "fileName": file_name,
        "fileHash": file_hash,
        "createdAt": now_iso(),
        "generator": generator,
        "hasPage": [],
    }
    if config is not None:
        doc_node["config"] = config
    graph.append(doc_node)

    # pages
    pages = sorted({t.page_no for t in tables})
    page_ids = {}
    for p in pages:
        pid = f"{doc_id}#page-{p}"
        page_ids[p] = pid
        doc_node["hasPage"].append(pid)
        graph.append({"@id": pid, "@type": "Page", "pageNumber": p, "hasTable": []})

    # table nodes + optional cells
    for t in tables:
        tid = f"{doc_id}#table-{t.table_id}"
        # attach to page
        page_node_id = page_ids.get(t.page_no)
        if page_node_id:
            # find page node and append
            for n in graph:
                if n.get("@id") == page_node_id:
                    n.setdefault("hasTable", []).append(tid)
                    break

        tnode: Dict[str, Any] = {
            "@id": tid,
            "@type": "Table",
            "tableId": t.table_id,
            "sheetName": t.sheet_name,
            "nRows": t.n_rows,
            "nCols": t.n_cols,
        }
        if include_bboxes and t.bbox:
            tnode["bbox"] = [float(x) for x in t.bbox]
        graph.append(tnode)

        if include_cells:
            for pc in t.cells:
                cid = f"{tid}#cell-{pc.row}-{pc.col}"
                cnode: Dict[str, Any] = {
                    "@id": cid,
                    "@type": "TableCell",
                    "belongsToTable": tid,
                    "rowIndex": pc.row,
                    "colIndex": pc.col,
                    "rowSpan": pc.row_span,
                    "colSpan": pc.col_span,
                    "text": pc.text,
                }
                if pc.conf is not None:
                    cnode["confidence"] = pc.conf
                if include_bboxes and pc.bbox:
                    cnode["bbox"] = [float(x) for x in pc.bbox]
                graph.append(cnode)

    return {"@context": context, "@graph": graph}


def make_jsonld_bytes(obj: Dict[str, Any]) -> bytes:
    return json.dumps(obj, ensure_ascii=False, indent=2).encode("utf-8")


# ============================================================
# 6) 전체 처리
# ============================================================
def process_pdf_to_tables(
    file_bytes: bytes,
    api_url: str,
    secret_key: str,
    *,
    pages_per_request: int,
    lang: str,
    progress_cb: Optional[Callable[[int, int, int, int], None]] = None,
) -> List[ParsedTable]:
    pages_per_request = max(1, min(int(pages_per_request), MAX_PAGES_PER_REQUEST))
    chunks = split_pdf_into_chunks(file_bytes, pages_per_request)

    all_tables: List[ParsedTable] = []
    for i, (chunk_pdf, start_p, end_p) in enumerate(chunks, start=1):
        if progress_cb:
            progress_cb(i, len(chunks), start_p, end_p)

        res = call_naver_ocr_table(chunk_pdf, api_url, secret_key, lang=lang)
        if not res.get("ok"):
            raise RuntimeError(
                f"OCR 실패 (pages {start_p}-{end_p})\n"
                f"status={res.get('status_code')}\n{res.get('text') or res.get('error')}"
            )
        ocr_json = res.get("json")
        if not ocr_json:
            raise RuntimeError(f"OCR JSON 파싱 실패 (pages {start_p}-{end_p})\n{res.get('text')}")

        page_numbers = list(range(start_p, end_p + 1))
        parsed = parse_tables_from_ocr_json(ocr_json, page_numbers=page_numbers)
        all_tables.extend(parsed)

    if progress_cb:
        progress_cb(len(chunks), len(chunks), 0, 0)

    return all_tables


# ============================================================
# 7) Streamlit UI
# ============================================================
def main():
    st.set_page_config(page_title=APP_TITLE, page_icon="📄", layout="wide")
    st.title(APP_TITLE)
    st.caption(f"{APP_VERSION} | General OCR(enableTableDetection) → tables/cells → Excel")

    st.markdown(
        """
이 버전은 **네이버 CLOVA OCR의 표 추출 기능(enableTableDetection=true)** 을 사용해서  
응답에 포함된 `tables -> cells(rowIndex/columnIndex/rowSpan/columnSpan)` 정보를 기반으로 **표를 엑셀로 그대로** 만듭니다.

✅ 표 추출 사용 전 체크:
- 네이버 클라우드 콘솔 → CLOVA OCR → Domain → **General** 도메인
- **‘표 추출 여부’ 토글을 ON** 해야 동작합니다. (OFF면 에러)
"""
    )

    # 데모 접근
    with st.expander("🔐 접근(데모용)", expanded=True):
        password = st.text_input("비밀번호", type="password")
        if password != DEFAULT_PASSWORD:
            st.warning("비밀번호가 올바르지 않습니다.")
            st.stop()
        st.success("접속 완료")

    with st.sidebar:
        st.header("⚙️ API 설정")
        try:
            api_url = st.secrets["NAVER_API_URL"]
            secret_key = st.secrets["NAVER_SECRET_KEY"]
            st.success("st.secrets에서 API 정보를 불러왔습니다.")
        except Exception:
            api_url = st.text_input("NAVER_API_URL (…/general)")
            secret_key = st.text_input("NAVER_SECRET_KEY", type="password")

        st.divider()
        st.header("🧾 추출 옵션")
        pages_per_req = st.number_input("OCR 요청당 페이지 수(<=10)", min_value=1, max_value=10, value=10, step=1)
        lang = st.selectbox("언어(lang)", options=["ko", "ja", "zh-TW", "ko,ja"], index=0)

        merge_cells = st.checkbox("엑셀에서 셀 병합 반영(rowSpan/columnSpan)", value=True)
        autosize = st.checkbox("열 너비 자동 조정(대략)", value=True)

        st.divider()
        st.header("🧠 온톨로지(JSON-LD)")
        export_jsonld = st.checkbox("JSON-LD도 함께 생성", value=True)
        include_cells = st.checkbox("JSON-LD에 셀까지 포함(파일 커짐)", value=False)

    uploaded_file = st.file_uploader("📎 PDF 업로드", type=["pdf"])
    if uploaded_file is None:
        st.info("PDF를 업로드하면 시작할 수 있어요.")
        return

    file_bytes = uploaded_file.getvalue()
    file_hash = hashlib.sha256(file_bytes).hexdigest()

    # 파일이 바뀌면 결과 초기화
    if st.session_state.get("file_hash") != file_hash:
        for k in ["tables", "excel_bytes", "jsonld_obj", "jsonld_bytes"]:
            st.session_state.pop(k, None)
        st.session_state["file_hash"] = file_hash

    clicked = st.button("🚀 표 추출 시작", disabled=not bool(api_url and secret_key))
    if clicked:
        if not api_url or not secret_key:
            st.error("API URL/SECRET을 입력하세요.")
            st.stop()

        progress = st.progress(0)
        status = st.empty()

        def progress_cb(i, total, sp, ep):
            pct = int(i / max(1, total) * 100)
            progress.progress(min(100, pct))
            if sp and ep:
                status.write(f"📄 OCR 진행 {i}/{total} (페이지 {sp}~{ep})")
            else:
                status.write("📄 마무리 중...")

        with st.spinner("OCR 및 표 변환 중..."):
            tables = process_pdf_to_tables(
                file_bytes,
                api_url,
                secret_key,
                pages_per_request=int(pages_per_req),
                lang=str(lang),
                progress_cb=progress_cb,
            )

            excel_bytes = tables_to_excel_bytes(tables, merge_cells=merge_cells, autosize=autosize)

            jsonld_obj = None
            jsonld_bytes = b""
            if export_jsonld:
                jsonld_obj = build_tables_jsonld(
                    file_name=uploaded_file.name,
                    file_hash=file_hash,
                    tables=tables,
                    include_cells=include_cells,
                    include_bboxes=True,
                    config={
                        "pages_per_request": int(pages_per_req),
                        "lang": str(lang),
                        "merge_cells": bool(merge_cells),
                        "autosize": bool(autosize),
                        "include_cells_in_jsonld": bool(include_cells),
                    },
                )
                jsonld_bytes = make_jsonld_bytes(jsonld_obj)

        st.session_state["tables"] = tables
        st.session_state["excel_bytes"] = excel_bytes
        st.session_state["jsonld_obj"] = jsonld_obj
        st.session_state["jsonld_bytes"] = jsonld_bytes

        progress.progress(100)
        status.write("✅ 완료")

    # 결과 표시
    if st.session_state.get("tables") is not None:
        tables: List[ParsedTable] = st.session_state["tables"]
        excel_bytes: bytes = st.session_state.get("excel_bytes", b"")
        jsonld_bytes: bytes = st.session_state.get("jsonld_bytes", b"")

        st.divider()
        st.subheader(f"✅ 추출된 표 수: {len(tables)}")

        col1, col2 = st.columns([1, 1])

        with col1:
            if excel_bytes:
                st.download_button(
                    "📥 엑셀 다운로드(표마다 시트 + index)",
                    data=excel_bytes,
                    file_name=f"{uploaded_file.name}_tables.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key="download_excel",
                )

            if export_jsonld and jsonld_bytes:
                st.download_button(
                    "🧠 JSON-LD 다운로드(표 그래프)",
                    data=jsonld_bytes,
                    file_name=f"{uploaded_file.name}_tables.jsonld",
                    mime="application/ld+json",
                    key="download_jsonld",
                )

            # index 표
            idx = []
            for t in tables:
                bbox_s = ""
                if t.bbox:
                    bbox_s = f"{t.bbox[0]:.1f},{t.bbox[1]:.1f},{t.bbox[2]:.1f},{t.bbox[3]:.1f}"
                idx.append(
                    {
                        "sheet": t.sheet_name,
                        "page": t.page_no,
                        "table_idx": t.table_index_on_page,
                        "rows": t.n_rows,
                        "cols": t.n_cols,
                        "bbox": bbox_s,
                    }
                )
            st.subheader("📌 표 목록(index)")
            st.dataframe(pd.DataFrame(idx), use_container_width=True, hide_index=True)

        with col2:
            st.subheader("🔎 표 미리보기")
            if tables:
                options = [f"{t.sheet_name} (p{t.page_no}, {t.n_rows}x{t.n_cols})" for t in tables]
                sel = st.selectbox("표 선택", options, index=0)
                t = tables[options.index(sel)]
                st.dataframe(pd.DataFrame(t.grid), use_container_width=True, hide_index=True)
            if export_jsonld:
                with st.expander("🧠 JSON-LD 미리보기", expanded=False):
                    st.json(st.session_state.get("jsonld_obj", {}))
    else:
        st.info("추출을 실행하면 결과가 여기에 표시됩니다.")


if __name__ == "__main__":
    main()

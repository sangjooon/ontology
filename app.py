# -*- coding: utf-8 -*-
"""
문서 비서 📄  (Naver Table OCR → "토지 등기 표제부"만 정리해서 엑셀로)
============================================================

요구사항(사용자 요청):
- 네이버 CLOVA OCR General + enableTableDetection=true (표추출 OCR) 사용
- PDF 전체를 OCR 처리한 뒤,
  "등기사항전부증명서(토지) - 표제부" 테이블만 골라서 보기 좋게 재가공
- 결과를 "표제부 전용 엑셀"로 따로 다운로드
- (선택) JSON-LD(간단 온톨로지 그래프)도 함께 다운로드

핵심 아이디어:
1) Naver가 내려주는 tables/cells(rowIndex, columnIndex, rowSpan, columnSpan)를 그대로 활용한다.
2) 표제부 후보 테이블은 '헤더 라인'에서
   [표시번호, 소재지번, 지목, 면적] 컬럼이 함께 나타나는 테이블을 우선으로 잡는다.
3) 소재지번이 여러 칸으로 쪼개지는 경우가 많아서,
   "소재지번 col ~ 지목 col 직전"까지를 합쳐서 소재지번으로 만든다.
   면적도 "면적 col ~ 끝"까지 합친다.
4) 표시번호가 비어있는 줄은 '이어쓰기(continuation)'로 판단해 이전 행에 병합한다.

주의:
- 표 추출은 도메인 설정에서 "표 추출 여부"가 ON이어야 동작합니다.
- OCR 결과가 표를 잘못 나누면, 완벽 복원은 불가능합니다.
  그래도 표제부는 구조가 비교적 단순해서 성공률이 높은 편입니다.

requirements.txt (권장)
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

import pandas as pd
import requests
import streamlit as st
from openpyxl import Workbook
from openpyxl.styles import Alignment
from openpyxl.utils import get_column_letter

# ============================================================
# 0) 앱 설정
# ============================================================
APP_TITLE = "문서 비서📄 dev — 토지 등기 표제부 전용"
APP_VERSION = "v0.5.0"

DEFAULT_PASSWORD = "alohomora"  # 데모용
MAX_PAGES_PER_REQUEST = 10      # Naver General OCR PDF 최대 10페이지/요청

MAX_SHEETNAME_LEN = 31


# ============================================================
# 1) PDF 유틸
# ============================================================
def _import_pypdf():
    try:
        from pypdf import PdfReader, PdfWriter  # type: ignore
        return PdfReader, PdfWriter
    except Exception:
        from PyPDF2 import PdfReader, PdfWriter  # type: ignore
        return PdfReader, PdfWriter


def split_pdf_into_chunks(pdf_bytes: bytes, chunk_size: int) -> List[Tuple[bytes, int, int]]:
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
# 2) 네이버 OCR 호출 (표추출 ON)
# ============================================================
def call_naver_ocr_table(
    file_bytes: bytes,
    api_url: str,
    secret_key: str,
    *,
    lang: str = "ko",
    timeout: int = 180,
) -> Dict[str, Any]:
    request_json = {
        "images": [{"format": "pdf", "name": "upload"}],
        "requestId": str(uuid.uuid4()),
        "version": "V2",
        "timestamp": int(round(time.time() * 1000)),
        "lang": lang,
        "enableTableDetection": True,
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
    grid: List[List[str]]               # [r][c] (top-left only if merged)
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
    lines = cell.get("cellTextLines") or []
    out_lines: List[str] = []
    for ln in lines:
        words = ln.get("cellWords") or []
        wtxt = " ".join(
            (w.get("inferText") or "").strip()
            for w in words
            if (w.get("inferText") or "").strip()
        )
        wtxt = re.sub(r"\s+", " ", wtxt).strip()
        if wtxt:
            out_lines.append(wtxt)
    return "\n".join(out_lines).strip()


def parse_tables_from_ocr_json(ocr_json: Dict[str, Any], *, page_numbers: List[int]) -> List[ParsedTable]:
    tables_out: List[ParsedTable] = []
    images = ocr_json.get("images", []) if isinstance(ocr_json, dict) else []

    for img_idx, img in enumerate(images):
        page_no = page_numbers[img_idx] if img_idx < len(page_numbers) else (img_idx + 1)
        tables = img.get("tables") or []
        if not tables:
            continue

        for t_idx, t in enumerate(tables, start=1):
            cells_raw = t.get("cells") or []
            parsed_cells: List[ParsedCell] = []

            max_r = 0
            max_c = 0
            merges: List[Tuple[int, int, int, int]] = []

            t_bbox = _bbox_from_vertices(((t.get("boundingPoly") or {}).get("vertices")) or [])

            for c in cells_raw:
                r = int(c.get("rowIndex", 0))
                col = int(c.get("columnIndex", 0))
                rspan = int(c.get("rowSpan", 1) or 1)
                cspan = int(c.get("columnSpan", 1) or 1)

                txt = _cell_text(c)

                c_bbox = _bbox_from_vertices(((c.get("boundingPoly") or {}).get("vertices")) or [])

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

            grid = [["" for _ in range(n_cols)] for _ in range(n_rows)]
            for pc in parsed_cells:
                if 0 <= pc.row < n_rows and 0 <= pc.col < n_cols:
                    grid[pc.row][pc.col] = pc.text

            table_id = f"p{page_no}_t{t_idx}"
            sheet_name = f"p{page_no}_t{t_idx}"[:MAX_SHEETNAME_LEN]

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
# 4) "표제부" 온톨로지(간단) + 추출/정리 로직
# ============================================================
# (온톨로지 역할) 표제부에서 의미가 있는 필드(개념)와 라벨(동의어/오독)을 정의
PYO_ONTOLOGY: Dict[str, Dict[str, Any]] = {
    "display_no": {
        "label": "표시번호",
        "aliases": ["표시번호", "표시 번호", "표시no", "표시No", "표시"],
        "dtype": "string",
    },
    "lot_address": {
        "label": "소재지번",
        "aliases": ["소재지번", "소재 지번", "소재지", "소 재 지 번", "소재지번(토지)", "소재지번(대지)"],
        "dtype": "string",
    },
    "land_category": {
        "label": "지목",
        "aliases": ["지목", "지 목"],
        "dtype": "string",
    },
    "area": {
        "label": "면적",
        "aliases": ["면적", "면 적", "면적(㎡)", "면적(m2)", "면적(m²)", "면 적(㎡)"],
        "dtype": "number",
        "unit": "㎡",
    },
}

# 지목 통제어휘(간단)
LAND_CATEGORIES = [
    "전","답","과수원","목장용지","임야","광천지","염전","대","공장용지","학교용지","주차장","주유소용지",
    "창고용지","도로","철도용지","제방","하천","구거","유지","양어장","수도용지","공원","체육용지",
    "유원지","종교용지","사적지","묘지","잡종지"
]
LAND_CATEGORY_ALIASES = {
    "음야": "임야",
    "임아": "임야",
    "대지": "대",
}


def _norm(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"\s+", "", s)
    return s.lower()


def _contains_any(norm_text: str, aliases: List[str]) -> bool:
    for a in aliases:
        if _norm(a) and _norm(a) in norm_text:
            return True
    return False


def normalize_land_category(raw: str) -> str:
    s = (raw or "").strip()
    s2 = re.sub(r"\s+", "", s)
    if s2 in LAND_CATEGORY_ALIASES:
        s2 = LAND_CATEGORY_ALIASES[s2]
    if s2 in LAND_CATEGORIES:
        return s2
    # 약한 fuzzy: 글자 1~2개 차이 정도만 보정 (한글 길이가 짧으므로 보수적으로)
    # ex) '임야' vs '입야' 같은 경우
    best = ""
    best_score = 0
    for cat in LAND_CATEGORIES:
        # 공통 글자 수 기반
        score = sum(1 for ch in s2 if ch in cat)
        if score > best_score:
            best_score = score
            best = cat
    if best_score >= 2 and len(s2) <= 4:
        return best
    return s


def parse_area_to_number(area_text: str) -> Optional[float]:
    s = (area_text or "").replace(",", " ")
    m = re.search(r"(\d+(?:\.\d+)?)", s)
    if not m:
        return None
    try:
        return float(m.group(1))
    except Exception:
        return None


def extract_lot_no(addr: str) -> str:
    """
    소재지번 문자열에서 지번(예: 496-10 / 496) 추출(휴리스틱).
    - 가장 마지막에 나오는 지번 패턴을 사용
    """
    s = (addr or "").strip()
    m = re.findall(r"\d{1,5}-\d{1,5}", s)
    if m:
        return m[-1]
    m2 = re.findall(r"(?:^|\s)(\d{1,5})(?:\s|$)", s)
    if m2:
        return m2[-1].strip()
    return ""


def find_pyo_tables(tables: List[ParsedTable]) -> List[Tuple[ParsedTable, int, Dict[str, int]]]:
    """
    표제부 후보 테이블 탐색.
    반환: [(table, header_row_index, col_map), ...]
      - header_row_index: 헤더로 판단된 행 인덱스
      - col_map: {'display_no': c, 'lot_address': c, 'land_category': c, 'area': c}
    """
    out: List[Tuple[ParsedTable, int, Dict[str, int]]] = []

    for t in tables:
        # 너무 작은 표는 제외(노이즈)
        if t.n_rows < 2 or t.n_cols < 3:
            continue

        # 각 행에서 "헤더" 찾기
        header_row = -1
        header_hits: Dict[str, int] = {}

        for r in range(min(t.n_rows, 8)):  # 헤더는 보통 상단에 있음
            row_texts = [t.grid[r][c] for c in range(t.n_cols)]
            row_norm = _norm(" ".join(row_texts))

            hits = {}
            for key, spec in PYO_ONTOLOGY.items():
                if _contains_any(row_norm, spec["aliases"]):
                    hits[key] = 1

            # 표제부 헤더 조건: 표시번호 + (소재지번/지목/면적 중 2개 이상)
            cond = ("display_no" in hits) and (len(hits) >= 3)
            if cond:
                header_row = r
                header_hits = hits
                break

        if header_row < 0:
            continue

        # 헤더행에서 컬럼 매핑
        col_map: Dict[str, int] = {}
        for c in range(t.n_cols):
            cell_norm = _norm(t.grid[header_row][c])

            # 표시번호
            if "display_no" not in col_map and _contains_any(cell_norm, PYO_ONTOLOGY["display_no"]["aliases"]):
                col_map["display_no"] = c
                continue

            # 소재지번
            if "lot_address" not in col_map and _contains_any(cell_norm, PYO_ONTOLOGY["lot_address"]["aliases"]):
                col_map["lot_address"] = c
                continue

            # 지목
            if "land_category" not in col_map and _contains_any(cell_norm, PYO_ONTOLOGY["land_category"]["aliases"]):
                col_map["land_category"] = c
                continue

            # 면적
            if "area" not in col_map and _contains_any(cell_norm, PYO_ONTOLOGY["area"]["aliases"]):
                col_map["area"] = c
                continue

        # fallback: 헤더 텍스트가 합쳐져 있으면 순서로 추정
        # (표제부 표는 대개 표시번호-소재지번-지목-면적 순)
        if "display_no" not in col_map:
            col_map["display_no"] = 0

        # 소재지번/지목/면적의 위치가 안 잡히면 제외
        if "land_category" not in col_map or "area" not in col_map:
            continue
        if "lot_address" not in col_map:
            # 소재지번은 지목 앞쪽에 있을 확률이 큼
            col_map["lot_address"] = max(0, min(col_map["land_category"] - 1, t.n_cols - 1))

        # sanity: 순서 강제(오탐 방지)
        if not (col_map["display_no"] < col_map["land_category"] < col_map["area"]):
            # 구조가 다르면 표제부 아닐 가능성
            continue

        out.append((t, header_row, col_map))

    return out


def extract_pyo_records_from_table(
    t: ParsedTable,
    header_row: int,
    col_map: Dict[str, int],
) -> pd.DataFrame:
    """
    표제부 테이블 하나를 '정리된' 레코드 테이블로 변환.
    반환 컬럼:
      - 페이지, table_id, 표시번호, 소재지번, 지번, 지목, 면적, 면적_숫자
    """
    c_disp = col_map["display_no"]
    c_addr = col_map["lot_address"]
    c_cat = col_map["land_category"]
    c_area = col_map["area"]

    records: List[Dict[str, Any]] = []
    cur: Optional[Dict[str, Any]] = None

    for r in range(header_row + 1, t.n_rows):
        row = t.grid[r]
        # 완전 빈 행 skip
        if all((not (row[c] or "").strip()) for c in range(t.n_cols)):
            continue

        disp = (row[c_disp] or "").strip()
        # 헤더/잡음 행 skip
        if disp and _contains_any(_norm(disp), ["표제부", "갑구", "을구", "순위번호"]):
            continue

        # 소재지번: 소재지번 col ~ 지목 직전까지 합치기
        addr_parts = []
        for c in range(c_addr, c_cat):
            if c < 0 or c >= t.n_cols:
                continue
            v = (row[c] or "").strip()
            if v:
                addr_parts.append(v)
        lot_addr = "\n".join(addr_parts).strip()

        # 지목: 지목 col ~ 면적 직전까지 합치기(지목이 쪼개지는 경우 대비)
        cat_parts = []
        for c in range(c_cat, c_area):
            if c < 0 or c >= t.n_cols:
                continue
            v = (row[c] or "").strip()
            if v:
                cat_parts.append(v)
        land_cat_raw = " ".join(cat_parts).strip()

        # 면적: 면적 col ~ 끝까지 합치기
        area_parts = []
        for c in range(c_area, t.n_cols):
            v = (row[c] or "").strip()
            if v:
                area_parts.append(v)
        area_text = " ".join(area_parts).strip()

        # continuation row 판단: 표시번호가 없고, 어떤 값이 있으면 이전 행에 이어붙이기
        is_continuation = (not disp) and (lot_addr or land_cat_raw or area_text)

        if is_continuation and cur is not None:
            if lot_addr:
                cur["소재지번"] = (cur["소재지번"] + "\n" + lot_addr).strip() if cur["소재지번"] else lot_addr
            if land_cat_raw:
                cur["지목_raw"] = (cur["지목_raw"] + " " + land_cat_raw).strip() if cur["지목_raw"] else land_cat_raw
            if area_text:
                cur["면적"] = (cur["면적"] + " " + area_text).strip() if cur["면적"] else area_text
            continue

        # 새 레코드 시작 조건: 표시번호가 숫자로 시작하거나(1,2,3) / 5-1 같은 패턴
        if disp:
            cur = {
                "페이지": t.page_no,
                "table_id": t.table_id,
                "표시번호": disp,
                "소재지번": lot_addr,
                "지목_raw": land_cat_raw,
                "면적": area_text,
            }
            records.append(cur)
        else:
            # 표시번호가 없는데 continuation도 아니면 일단 skip
            continue

    df = pd.DataFrame(records)

    if df.empty:
        return df

    # 지번 추출
    df["지번"] = df["소재지번"].apply(extract_lot_no)

    # 지목 정규화
    df["지목"] = df["지목_raw"].apply(normalize_land_category)

    # 면적 숫자(㎡)
    df["면적_숫자"] = df["면적"].apply(parse_area_to_number)

    # 보기 좋게 컬럼 순서
    cols = ["페이지", "table_id", "표시번호", "소재지번", "지번", "지목", "면적", "면적_숫자"]
    df = df[[c for c in cols if c in df.columns]]

    return df


# ============================================================
# 5) 엑셀 생성 (표제부 전용)
# ============================================================
def _safe_sheet_name(name: str, used: set) -> str:
    n = re.sub(r"[\[\]\*:/\\\?]", "_", name)
    n = n[:MAX_SHEETNAME_LEN]
    if n not in used:
        used.add(n)
        return n
    i = 2
    while True:
        cand = f"{n[:MAX_SHEETNAME_LEN-2]}_{i}"[:MAX_SHEETNAME_LEN]
        if cand not in used:
            used.add(cand)
            return cand
        i += 1


def _autosize(ws, max_col: int, max_row: int):
    for c in range(1, max_col + 1):
        letter = get_column_letter(c)
        max_len = 0
        for r in range(1, max_row + 1):
            v = ws.cell(row=r, column=c).value
            if v is None:
                continue
            s = str(v)
            max_len = max(max_len, max((len(line) for line in s.splitlines()), default=len(s)))
        ws.column_dimensions[letter].width = min(70, max(10, max_len + 2))


def build_pyo_excel_bytes(
    *,
    pyo_df: pd.DataFrame,
    pyo_tables: List[ParsedTable],
    merge_cells: bool = True,
    include_raw_tables: bool = True,
) -> bytes:
    wb = Workbook()
    ws_summary = wb.active
    ws_summary.title = "표제부_정리"

    wrap = Alignment(wrap_text=True, vertical="top")

    # 1) 정리 시트 (DataFrame)
    if pyo_df.empty:
        ws_summary.append(["표제부를 찾지 못했습니다."])
    else:
        ws_summary.append(list(pyo_df.columns))
        for _, row in pyo_df.iterrows():
            ws_summary.append([row.get(c, "") for c in pyo_df.columns])

        # wrap
        for row in ws_summary.iter_rows(min_row=1, max_row=ws_summary.max_row, min_col=1, max_col=ws_summary.max_column):
            for cell in row:
                cell.alignment = wrap

        _autosize(ws_summary, ws_summary.max_column, ws_summary.max_row)

    # 2) index 시트
    ws_index = wb.create_sheet("index")
    ws_index.append(["sheet_name", "page", "table_id", "rows", "cols", "bbox(x0,y0,x1,y1)"])
    used = {"표제부_정리", "index"}

    # 3) raw table sheets (선택)
    if include_raw_tables:
        for t in pyo_tables:
            bbox_s = ""
            if t.bbox:
                bbox_s = f"{t.bbox[0]:.1f},{t.bbox[1]:.1f},{t.bbox[2]:.1f},{t.bbox[3]:.1f}"
            sheet_name = _safe_sheet_name(f"raw_{t.table_id}", used)
            ws_index.append([sheet_name, t.page_no, t.table_id, t.n_rows, t.n_cols, bbox_s])

            ws = wb.create_sheet(sheet_name)

            # write grid
            for r in range(t.n_rows):
                ws.append(t.grid[r])

            # wrap
            for row in ws.iter_rows(min_row=1, max_row=t.n_rows, min_col=1, max_col=t.n_cols):
                for cell in row:
                    cell.alignment = wrap

            # merge cells
            if merge_cells:
                for (r0, c0, r1, c1) in t.merges:
                    ws.merge_cells(start_row=r0 + 1, start_column=c0 + 1, end_row=r1 + 1, end_column=c1 + 1)

            _autosize(ws, t.n_cols, t.n_rows)

    # 저장
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


# ============================================================
# 6) (선택) JSON-LD(온톨로지 그래프) — 표제부만
# ============================================================
def build_pyo_jsonld(
    *,
    file_name: str,
    file_hash: str,
    pyo_df: pd.DataFrame,
    base_iri: str = "urn:dovi:",
    generator: str = "DOVI-PYO",
) -> Dict[str, Any]:
    """
    간단 JSON-LD:
    Document -> Parcel -> Fact
    + evidence로 page/table_id를 Fact에 남김
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
        "Parcel": "dovi:Parcel",
        "Fact": "dovi:Fact",

        "fileName": "schema:name",
        "fileHash": "dovi:fileHash",
        "createdAt": {"@id": "schema:dateCreated", "@type": "xsd:dateTime"},
        "generator": "schema:generator",

        "mentionsParcel": {"@id": "dovi:mentionsParcel", "@type": "@id"},
        "hasFact": {"@id": "dovi:hasFact", "@type": "@id"},

        "lot": "dovi:lot",
        "siteAddress": "dovi:siteAddress",

        "field": "dovi:field",
        "value": "dovi:value",
        "valueNumber": {"@id": "dovi:valueNumber", "@type": "xsd:decimal"},
        "unit": "dovi:unit",
        "evidencePage": {"@id": "dovi:evidencePage", "@type": "xsd:integer"},
        "evidenceTable": "dovi:evidenceTable",
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
        "mentionsParcel": [],
    }
    graph.append(doc_node)

    if pyo_df is None or pyo_df.empty:
        return {"@context": context, "@graph": graph}

    def pid_for(lot: str, addr: str) -> str:
        key = (addr or "") + "|" + (lot or "")
        hid = hashlib.sha1(key.encode("utf-8")).hexdigest()[:12]
        return f"{doc_id}#parcel-{hid}"

    for _, row in pyo_df.iterrows():
        addr = str(row.get("소재지번", "") or "")
        lot = str(row.get("지번", "") or "")
        pid = pid_for(lot, addr)

        if pid not in doc_node["mentionsParcel"]:
            doc_node["mentionsParcel"].append(pid)
            graph.append(
                {
                    "@id": pid,
                    "@type": "Parcel",
                    "lot": lot,
                    "siteAddress": addr,
                    "hasFact": [],
                }
            )

        # parcel node ref
        parcel_node = next(n for n in graph if n.get("@id") == pid)

        page = row.get("페이지")
        table_id = row.get("table_id")

        def add_fact(field: str, value: Any, *, value_number: Optional[float] = None, unit: Optional[str] = None):
            if value is None:
                return
            v = str(value).strip()
            if not v:
                return
            fid = f"{pid}#fact-{hashlib.sha1((field+'|'+v).encode('utf-8')).hexdigest()[:10]}"
            fact = {
                "@id": fid,
                "@type": "Fact",
                "field": field,
                "value": v,
            }
            if value_number is not None:
                fact["valueNumber"] = value_number
            if unit:
                fact["unit"] = unit
            if page is not None and str(page).isdigit():
                fact["evidencePage"] = int(page)
            if table_id:
                fact["evidenceTable"] = str(table_id)

            graph.append(fact)
            parcel_node["hasFact"].append(fid)

        add_fact("표시번호", row.get("표시번호"))
        add_fact("소재지번", row.get("소재지번"))
        add_fact("지목", row.get("지목"))
        add_fact("면적", row.get("면적"), value_number=row.get("면적_숫자"), unit="㎡")

    return {"@context": context, "@graph": graph}


def make_jsonld_bytes(obj: Dict[str, Any]) -> bytes:
    return json.dumps(obj, ensure_ascii=False, indent=2).encode("utf-8")


# ============================================================
# 7) 전체 처리
# ============================================================
def process_pdf(
    file_bytes: bytes,
    api_url: str,
    secret_key: str,
    *,
    pages_per_request: int,
    lang: str,
    progress_cb: Optional[Callable[[int, int, int, int], None]] = None,
) -> Tuple[List[ParsedTable], pd.DataFrame, List[ParsedTable]]:
    """
    반환:
      - all_tables: OCR에서 나온 전체 tables
      - pyo_df: 표제부 정리 DataFrame(합친 결과)
      - pyo_tables: 표제부 후보로 판정된 tables
    """
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
        tables = parse_tables_from_ocr_json(ocr_json, page_numbers=page_numbers)
        all_tables.extend(tables)

    if progress_cb:
        progress_cb(len(chunks), len(chunks), 0, 0)

    # 표제부 후보 테이블 찾기
    pyo_candidates = find_pyo_tables(all_tables)

    # 표제부 정리 DF 만들기
    pyo_dfs = []
    pyo_tables = []
    for (t, header_row, col_map) in pyo_candidates:
        df = extract_pyo_records_from_table(t, header_row, col_map)
        if not df.empty:
            pyo_dfs.append(df)
            pyo_tables.append(t)

    pyo_df = pd.concat(pyo_dfs, ignore_index=True) if pyo_dfs else pd.DataFrame()

    return all_tables, pyo_df, pyo_tables


# ============================================================
# 8) Streamlit UI
# ============================================================
def main():
    st.set_page_config(page_title=APP_TITLE, page_icon="📄", layout="wide")
    st.title(APP_TITLE)
    st.caption(f"{APP_VERSION} | Naver Table OCR → 표제부 정리 엑셀")

    st.markdown(
        """
### 이 앱이 하는 일
- **네이버 표추출 OCR(enableTableDetection)** 로 PDF 전체를 인식한 뒤  
- **토지 등기 '표제부' 테이블만** 자동으로 찾아서  
- **보기 좋은 형태(정리된 표)** 로 재가공한 엑셀을 만들어줍니다.

> 표 추출은 도메인 설정에서 **‘표 추출 여부’가 ON**이어야 합니다.
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
        st.header("🧾 OCR 옵션")
        pages_per_req = st.number_input("OCR 요청당 페이지 수(<=10)", min_value=1, max_value=10, value=10, step=1)
        lang = st.selectbox("언어(lang)", options=["ko", "ja", "zh-TW", "ko,ja"], index=0)

        st.divider()
        st.header("📦 출력 옵션")
        include_raw_tables = st.checkbox("표제부 원본 테이블 시트(raw_...)도 포함", value=True)
        merge_cells = st.checkbox("raw 시트에서 병합셀 반영", value=True)

        st.divider()
        st.header("🧠 온톨로지(JSON-LD)")
        export_jsonld = st.checkbox("표제부 JSON-LD도 생성", value=True)

    uploaded_file = st.file_uploader("📎 PDF 업로드", type=["pdf"])
    if uploaded_file is None:
        st.info("PDF를 업로드하면 시작할 수 있어요.")
        return

    file_bytes = uploaded_file.getvalue()
    file_hash = hashlib.sha256(file_bytes).hexdigest()

    # 파일이 바뀌면 초기화
    if st.session_state.get("file_hash") != file_hash:
        for k in ["pyo_df", "pyo_excel", "pyo_tables", "jsonld_obj", "jsonld_bytes"]:
            st.session_state.pop(k, None)
        st.session_state["file_hash"] = file_hash

    clicked = st.button("🚀 표제부 추출 시작", disabled=not bool(api_url and secret_key))
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

        with st.spinner("OCR 및 표제부 정리 중..."):
            all_tables, pyo_df, pyo_tables = process_pdf(
                file_bytes,
                api_url,
                secret_key,
                pages_per_request=int(pages_per_req),
                lang=str(lang),
                progress_cb=progress_cb,
            )

            pyo_excel = build_pyo_excel_bytes(
                pyo_df=pyo_df,
                pyo_tables=pyo_tables,
                merge_cells=merge_cells,
                include_raw_tables=include_raw_tables,
            )

            jsonld_obj = None
            jsonld_bytes = b""
            if export_jsonld:
                jsonld_obj = build_pyo_jsonld(
                    file_name=uploaded_file.name,
                    file_hash=file_hash,
                    pyo_df=pyo_df,
                )
                jsonld_bytes = make_jsonld_bytes(jsonld_obj)

        st.session_state["pyo_df"] = pyo_df
        st.session_state["pyo_excel"] = pyo_excel
        st.session_state["pyo_tables"] = pyo_tables
        st.session_state["jsonld_obj"] = jsonld_obj
        st.session_state["jsonld_bytes"] = jsonld_bytes

        progress.progress(100)
        status.write("✅ 완료")

    # 결과 표시
    if st.session_state.get("pyo_df") is not None:
        pyo_df: pd.DataFrame = st.session_state.get("pyo_df") or pd.DataFrame()
        pyo_excel: bytes = st.session_state.get("pyo_excel", b"")
        pyo_tables: List[ParsedTable] = st.session_state.get("pyo_tables") or []
        jsonld_bytes: bytes = st.session_state.get("jsonld_bytes", b"")

        st.divider()
        st.subheader("✅ 표제부 정리 결과")

        col1, col2 = st.columns([1, 1])

        with col1:
            st.write(f"- 표제부 후보 테이블 수: **{len(pyo_tables)}**")
            st.write(f"- 정리 레코드 수(표제부 행): **{len(pyo_df)}**")

            if pyo_excel:
                st.download_button(
                    "📥 표제부 전용 엑셀 다운로드",
                    data=pyo_excel,
                    file_name=f"{uploaded_file.name}_표제부.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key="download_pyo_excel",
                )

            if export_jsonld and jsonld_bytes:
                st.download_button(
                    "🧠 표제부 JSON-LD 다운로드",
                    data=jsonld_bytes,
                    file_name=f"{uploaded_file.name}_표제부.jsonld",
                    mime="application/ld+json",
                    key="download_pyo_jsonld",
                )

        with col2:
            if pyo_df.empty:
                st.warning("표제부 테이블을 찾지 못했어요. (표제부 헤더 인식 실패 가능)")
                st.caption("팁: 원본 PDF가 너무 기울어져 있거나 표선이 약하면 표추출이 실패할 수 있어요.")
            else:
                st.dataframe(pyo_df, use_container_width=True, hide_index=True)

            if export_jsonld:
                with st.expander("🧠 JSON-LD 미리보기", expanded=False):
                    st.json(st.session_state.get("jsonld_obj", {}))

    else:
        st.info("추출을 실행하면 결과가 여기에 표시됩니다.")


if __name__ == "__main__":
    main()

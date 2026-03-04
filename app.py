# -*- coding: utf-8 -*-
"""
문서 비서 📄  (Naver Table OCR → 토지 등기 "표제부" + "갑구" 정리)
===============================================================

✅ 사용자 요구 반영(v0.6.0)
1) 표제부가 여러 지번(여러 페이지)에 따로 존재하면, **지번별로 표를 분리**해서 보여주고/엑셀 시트도 분리
2) 갑구도 **지번별로 분리**해서 정리
   - 컬럼: 순위번호, 등기목적, 접수, 등기원인, 권리자 및 기타사항
   - 권리자 및 기타사항이 여러 셀/여러 줄로 나뉘는 경우 → 같은 순위번호로 이어붙이기

기술 스택
- 네이버 CLOVA OCR General + enableTableDetection=true (표추출 OCR)
- 표( tables/cells )는 네이버가 준 rowIndex/columnIndex/Span을 기반으로 복원
- 표제부/갑구는 "작은 온톨로지(aliases + 정규화 규칙)"로 헤더 인식 및 컬럼 매핑

주의
- 표 추출은 네이버 콘솔 Domain 설정에서 "표 추출 여부"가 ON이어야 동작합니다.
- OCR이 표를 잘못 쪼개면 완벽 복원은 불가능하지만, 등기 표제부/갑구는 구조가 비교적 규칙적이라 성공률이 높습니다.

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
from dataclasses import dataclass, field
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
APP_TITLE = "문서 비서📄 dev — 토지 등기(표제부+갑구) 정리"
APP_VERSION = "v0.6.2"

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


def get_pdf_total_pages(pdf_bytes: bytes) -> int:
    PdfReader, _ = _import_pypdf()
    reader = PdfReader(io.BytesIO(pdf_bytes))
    return len(reader.pages)


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
# 3) OCR 테이블 파싱 + 페이지 텍스트라인(갑/을 구 판별용)
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
    grid: List[List[str]]  # [r][c] (top-left only if merged)
    merges: List[Tuple[int, int, int, int]]  # (r0,c0,r1,c1) 0-based inclusive
    cells: List[ParsedCell]


@dataclass
class PageLine:
    y: float
    text: str


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


def _fields_to_page_lines(img: Dict[str, Any], *, y_thresh: float = 14.0) -> List[PageLine]:
    """
    OCR fields(일반 텍스트 박스)를 y기준으로 줄 단위로 묶어서 반환.
    (갑구/을구 구분을 위해 테이블 근처의 텍스트를 찾는 용도)
    """
    fields = img.get("fields") or []
    items = []
    for f in fields:
        txt = (f.get("inferText") or "").strip()
        if not txt:
            continue
        verts = ((f.get("boundingPoly") or {}).get("vertices")) or []
        if not verts:
            continue
        x0 = float(verts[0].get("x", 0))
        y0 = float(verts[0].get("y", 0))
        items.append((y0, x0, txt))

    if not items:
        return []

    items.sort(key=lambda x: (x[0], x[1]))
    lines: List[PageLine] = []
    cur = []
    last_y = items[0][0]

    def flush():
        nonlocal cur
        if not cur:
            return
        cur.sort(key=lambda x: x[1])
        y_avg = sum(x[0] for x in cur) / len(cur)
        text = " ".join(x[2] for x in cur).strip()
        text = re.sub(r"\s+", " ", text)
        if text:
            lines.append(PageLine(y=y_avg, text=text))
        cur = []

    for y0, x0, txt in items:
        if cur and abs(y0 - last_y) > y_thresh:
            flush()
        cur.append((y0, x0, txt))
        last_y = y0

    flush()
    return lines


def parse_tables_and_lines_from_ocr_json(
    ocr_json: Dict[str, Any], *, page_numbers: List[int]
) -> Tuple[List[ParsedTable], Dict[int, List[PageLine]]]:
    """
    반환:
      - tables: ParsedTable 리스트
      - page_lines: {page_no: [PageLine, ...]} (갑/을 구 판별용)
    """
    tables_out: List[ParsedTable] = []
    page_lines: Dict[int, List[PageLine]] = {}

    images = ocr_json.get("images", []) if isinstance(ocr_json, dict) else []

    for img_idx, img in enumerate(images):
        page_no = page_numbers[img_idx] if img_idx < len(page_numbers) else (img_idx + 1)

        # page lines
        page_lines[page_no] = _fields_to_page_lines(img)

        # tables
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


            # 테이블 bbox가 응답에 없을 수 있어 cell bbox로 보강
            if t_bbox is None:
                xs0, ys0, xs1, ys1 = [], [], [], []
                for pc in parsed_cells:
                    if pc.bbox:
                        x0, y0, x1, y1 = pc.bbox
                        xs0.append(x0); ys0.append(y0); xs1.append(x1); ys1.append(y1)
                if xs0 and ys0 and xs1 and ys1:
                    t_bbox = (min(xs0), min(ys0), max(xs1), max(ys1))

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

    return tables_out, page_lines


# ============================================================
# 4) 공통 유틸/온톨로지(aliases) 기반 헤더 탐지
# ============================================================
def _norm(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"\s+", "", s)
    return s.lower()


def _contains_any(norm_text: str, aliases: List[str]) -> bool:
    for a in aliases:
        if _norm(a) and _norm(a) in norm_text:
            return True
    return False


def join_cols(row: List[str], start: int, end: int, *, sep: str = "\n") -> str:
    parts: List[str] = []
    for c in range(start, min(end, len(row))):
        v = (row[c] or "").strip()
        if v:
            parts.append(v)
    s = sep.join(parts).strip()
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s


# ============================================================
# 5) 표제부 추출/정리
# ============================================================
PYO_ONTOLOGY: Dict[str, Dict[str, Any]] = {
    "display_no": {
        "label": "표시번호",
        "aliases": ["표시번호", "표시 번호", "표시no", "표시No", "표시"],
        "dtype": "string",
    },
    "acceptance": {
        "label": "접수",
        "aliases": ["접수", "접 수", "접수일자", "접수번호"],
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


def normalize_land_category(raw: str) -> str:
    s = (raw or "").strip()
    s2 = re.sub(r"\s+", "", s)
    if s2 in LAND_CATEGORY_ALIASES:
        s2 = LAND_CATEGORY_ALIASES[s2]
    if s2 in LAND_CATEGORIES:
        return s2
    # 약한 보정(공통 글자수)
    best = ""
    best_score = 0
    for cat in LAND_CATEGORIES:
        score = sum(1 for ch in s2 if ch in cat)
        if score > best_score:
            best_score = score
            best = cat
    if best_score >= 2 and len(s2) <= 4:
        return best
    return s


def extract_lot_no(addr: str) -> str:
    s = (addr or "").strip()
    m = re.findall(r"\d{1,5}-\d{1,5}", s)
    if m:
        return m[-1]
    m2 = re.findall(r"(?:^|\s)(\d{1,5})(?:\s|$)", s)
    if m2:
        return m2[-1].strip()
    return ""


def split_area_and_note(area_text: str) -> Tuple[str, str]:
    """
    면적 텍스트에서 '숫자+단위(m2/㎡/m²)'를 분리하고,
    나머지를 '등기원인 및 기타사항'으로 돌려준다.
    """
    s = (area_text or "").strip()
    if not s:
        return "", ""

    m = re.search(r"(?i)(\d+(?:,\d+)*(?:\.\d+)?)\s*(m2|m²|㎡)", s)
    if not m:
        return "", s

    num = (m.group(1) or "").replace(",", "")
    area_only = f"{num}m2"

    note = s[m.end():].strip()
    note = re.sub(r"^[\s:;,.\)\]]+", "", note).strip()
    return area_only, note



# -----------------------------
# 표제부 컬럼 추정(헤더 인식 실패/병합 헤더 대비)
# -----------------------------
_LOT_RE = re.compile(r"\b(\d{1,5}-\d{1,5})\b")


def _score_is_display_no(v: str) -> int:
    s = (v or "").strip()
    return 1 if re.fullmatch(r"\d+(?:-\d+)?", s) else 0


def _score_is_area(v: str) -> int:
    s = (v or "").strip().replace(" ", "")
    return 1 if re.search(r"(?i)\d+(?:,\d+)*(?:\.\d+)?(m2|m²|㎡)", s) else 0


def _score_is_land_category(v: str) -> int:
    s = normalize_land_category(v)
    return 1 if s in LAND_CATEGORIES else 0


def _score_is_acceptance(v: str) -> int:
    s = (v or "").strip()
    # 날짜(YYYY년M월D일) 또는 접수번호(제xxxx호) 패턴
    if re.search(r"\d{4}\s*년\s*\d{1,2}\s*월\s*\d{1,2}\s*일", s):
        return 2
    if re.search(r"제\s*\d+\s*호", s):
        return 1
    return 0


def _score_is_address(v: str) -> int:
    s = (v or "").strip()
    score = 0
    if _LOT_RE.search(s):
        score += 2
    if re.search(r"(시|군|구|읍|면|동|리)", s):
        score += 1
    if "소재지" in s or "지번" in s:
        score += 1
    return score


def infer_pyo_col_map(t: ParsedTable, header_row: int) -> Dict[str, int]:
    """
    헤더가 병합/오독으로 컬럼명이 안 보이는 경우를 위해,
    데이터 행 패턴으로 표제부 컬럼을 추정한다.
    """
    # 샘플링 범위
    r0 = header_row + 1
    r1 = min(t.n_rows, header_row + 1 + 30)

    # 컬럼별 점수 계산
    scores = []
    for c in range(t.n_cols):
        disp = area = cat = acc = addr = 0
        for r in range(r0, r1):
            v = (t.grid[r][c] or "").strip()
            if not v:
                continue
            disp += _score_is_display_no(v)
            area += _score_is_area(v)
            cat += _score_is_land_category(v)
            acc += _score_is_acceptance(v)
            addr += _score_is_address(v)
        scores.append({"c": c, "disp": disp, "area": area, "cat": cat, "acc": acc, "addr": addr})

    # best columns
    c_disp = max(scores, key=lambda x: x["disp"])["c"] if scores else 0
    c_area = max(scores, key=lambda x: x["area"])["c"] if scores else min(3, t.n_cols - 1)
    c_cat = max(scores, key=lambda x: x["cat"])["c"] if scores else max(0, min(c_area - 1, t.n_cols - 1))

    # address는 지목 앞쪽에서 찾는 게 안전
    cand_addr = [s for s in scores if s["c"] < c_cat]
    c_addr = max(cand_addr, key=lambda x: x["addr"])["c"] if cand_addr else max(0, min(c_cat - 1, t.n_cols - 1))

    # acceptance는 표시번호 다음~소재지번 이전에서 찾는 게 안전
    cand_acc = [s for s in scores if c_disp < s["c"] < c_addr]
    if cand_acc:
        c_acc = max(cand_acc, key=lambda x: x["acc"])["c"]
    else:
        c_acc = min(c_disp + 1, t.n_cols - 1)

    col_map = {
        "display_no": c_disp,
        "acceptance": c_acc,
        "lot_address": c_addr,
        "land_category": c_cat,
        "area": c_area,
    }

    # 순서가 뒤집힌 경우 보정(최소한 display_no < land_category < area)
    if not (col_map["display_no"] < col_map["land_category"] < col_map["area"]):
        # land_category/area가 바뀐 경우가 많음 → swap 시도
        if col_map["display_no"] < col_map["area"] < col_map["land_category"]:
            col_map["land_category"], col_map["area"] = col_map["area"], col_map["land_category"]

    return col_map
def find_pyo_tables(tables: List[ParsedTable]) -> List[Tuple[ParsedTable, int, Dict[str, int]]]:
    """
    표제부 테이블 후보 탐색.
    - 헤더가 명확하면 헤더 기반 매핑
    - 헤더가 병합/오독이면 데이터 패턴으로 컬럼 추정(infer_pyo_col_map)
    """
    out: List[Tuple[ParsedTable, int, Dict[str, int]]] = []

    for t in tables:
        if t.n_rows < 2 or t.n_cols < 3:
            continue

        header_row = -1
        for r in range(min(t.n_rows, 15)):  # ✅ 탐색 범위 확대
            row_norm = _norm(" ".join(t.grid[r]))
            hits = {k: 1 for k, spec in PYO_ONTOLOGY.items() if _contains_any(row_norm, spec["aliases"])}

            # 케이스1) 표시번호 포함 + 최소 3개 필드 히트
            cond1 = ("display_no" in hits) and (len(hits) >= 3)
            # 케이스2) 표시번호가 안 보여도 소재지번/지목/면적 3종이 보이면 표제부로 간주
            cond2 = ("lot_address" in hits) and ("land_category" in hits) and ("area" in hits)
            # 케이스3) '표제부' 텍스트가 섞여 있고 지목/면적이 보이면 표제부 가능성
            cond3 = ("표제부" in row_norm) and (("land_category" in hits) and ("area" in hits))

            if cond1 or cond2 or cond3:
                header_row = r
                break

        if header_row < 0:
            continue

        # 1차: 헤더 기반 매핑
        col_map: Dict[str, int] = {}
        for c in range(t.n_cols):
            cell_norm = _norm(t.grid[header_row][c])

            if "display_no" not in col_map and _contains_any(cell_norm, PYO_ONTOLOGY["display_no"]["aliases"]):
                col_map["display_no"] = c
                continue
            if "acceptance" not in col_map and _contains_any(cell_norm, PYO_ONTOLOGY["acceptance"]["aliases"]):
                col_map["acceptance"] = c
                continue
            if "lot_address" not in col_map and _contains_any(cell_norm, PYO_ONTOLOGY["lot_address"]["aliases"]):
                col_map["lot_address"] = c
                continue
            if "land_category" not in col_map and _contains_any(cell_norm, PYO_ONTOLOGY["land_category"]["aliases"]):
                col_map["land_category"] = c
                continue
            if "area" not in col_map and _contains_any(cell_norm, PYO_ONTOLOGY["area"]["aliases"]):
                col_map["area"] = c
                continue

        # 2차: 부족한 컬럼은 데이터 패턴으로 추정
        need_infer = ("land_category" not in col_map) or ("area" not in col_map) or ("lot_address" not in col_map)
        if need_infer:
            inferred = infer_pyo_col_map(t, header_row)
            for k, v in inferred.items():
                col_map.setdefault(k, v)

        # acceptance가 없으면 표시번호 다음칸으로 보수적 추정
        col_map.setdefault("display_no", 0)
        if "acceptance" not in col_map:
            cand = col_map.get("display_no", 0) + 1
            if cand < t.n_cols:
                col_map["acceptance"] = cand

        # sanity: 표시번호 < 지목 < 면적 (일반적)
        if not (col_map["display_no"] < col_map["land_category"] < col_map["area"]):
            # 마지막으로 infer 재시도(간혹 헤더 매핑이 뒤틀림)
            col_map = infer_pyo_col_map(t, header_row)
            if not (col_map["display_no"] < col_map["land_category"] < col_map["area"]):
                continue

        out.append((t, header_row, col_map))

    return out


def extract_pyo_records_from_table(t: ParsedTable, header_row: int, col_map: Dict[str, int]) -> pd.DataFrame:
    c_disp = col_map["display_no"]
    c_acc = col_map.get("acceptance")
    c_addr = col_map["lot_address"]
    c_cat = col_map["land_category"]
    c_area = col_map["area"]

    records: List[Dict[str, Any]] = []
    cur: Optional[Dict[str, Any]] = None

    for r in range(header_row + 1, t.n_rows):
        row = t.grid[r]
        if all((not (row[c] or "").strip()) for c in range(t.n_cols)):
            continue

        disp = (row[c_disp] or "").strip()
        # 헤더/잡음 행 skip
        if disp and _contains_any(_norm(disp), ["표제부", "갑구", "을구", "순위번호"]):
            continue

        # 접수: 접수 col ~ 소재지번 직전
        acc_text = ""
        if c_acc is not None and 0 <= c_acc < t.n_cols:
            if c_acc < c_addr:
                acc_text = join_cols(row, c_acc, c_addr, sep="\n")
            else:
                acc_text = (row[c_acc] or "").strip()

        # 소재지번: 소재지번 col ~ 지목 직전
        lot_addr = join_cols(row, c_addr, c_cat, sep="\n")

        # 지목: 지목 col ~ 면적 직전
        land_cat_raw = join_cols(row, c_cat, c_area, sep=" ")

        # 면적: 면적 col ~ 끝
        area_text_raw = join_cols(row, c_area, t.n_cols, sep=" ")
        area_only, note_text = split_area_and_note(area_text_raw)

        is_cont = (not disp) and (acc_text or lot_addr or land_cat_raw or area_only or note_text)
        if is_cont and cur is not None:
            if acc_text:
                cur["접수"] = (cur["접수"] + "\n" + acc_text).strip() if cur["접수"] else acc_text
            if lot_addr:
                cur["소재지번"] = (cur["소재지번"] + "\n" + lot_addr).strip() if cur["소재지번"] else lot_addr
            if land_cat_raw:
                cur["_지목_raw"] = (cur["_지목_raw"] + " " + land_cat_raw).strip() if cur["_지목_raw"] else land_cat_raw
            if area_only and not cur.get("면적"):
                cur["면적"] = area_only
            if note_text:
                cur["등기원인 및 기타사항"] = (
                    (cur.get("등기원인 및 기타사항") or "").strip() + "\n" + note_text
                ).strip() if (cur.get("등기원인 및 기타사항") or "").strip() else note_text
            continue

        if disp:
            cur = {
                "페이지": t.page_no,
                "table_id": t.table_id,
                "표시번호": disp,
                "접수": acc_text,
                "소재지번": lot_addr,
                "_지목_raw": land_cat_raw,
                "면적": area_only,
                "등기원인 및 기타사항": note_text,
            }
            records.append(cur)

    df = pd.DataFrame(records)
    if df.empty:
        return df

    df["지번"] = df["소재지번"].apply(extract_lot_no)
    df["지목"] = df["_지목_raw"].apply(normalize_land_category)
    df = df.drop(columns=["_지목_raw"], errors="ignore")

    # 정렬(표시번호)
    def disp_key(x: Any) -> Tuple[int, int, str]:
        s = str(x or "").strip()
        m = re.match(r"^(\d+)(?:-(\d+))?", s)
        if not m:
            return (10**9, 0, s)
        a = int(m.group(1))
        b = int(m.group(2) or 0)
        return (a, b, s)

    df["_k1"] = df["표시번호"].apply(disp_key)
    df = df.sort_values("_k1").drop(columns=["_k1"])

    cols = ["페이지", "table_id", "표시번호", "접수", "소재지번", "지번", "지목", "면적", "등기원인 및 기타사항"]
    return df[[c for c in cols if c in df.columns]]


# ============================================================
# 6) 갑구 추출/정리
# ============================================================
GAB_ONTOLOGY: Dict[str, Dict[str, Any]] = {
    "rank": {
        "label": "순위번호",
        "aliases": ["순위번호", "순위 번호", "순 위 번 호", "순위", "순위No", "순위NO"],
    },
    "purpose": {
        "label": "등기목적",
        "aliases": ["등기목적", "등기 목적", "등 기 목 적"],
    },
    "acceptance": {
        "label": "접수",
        "aliases": ["접수", "접 수", "접수일자", "접수번호"],
    },
    "cause": {
        "label": "등기원인",
        "aliases": ["등기원인", "등기 원인", "등 기 원 인"],
    },
    "holder": {
        "label": "권리자 및 기타사항",
        "aliases": ["권리자및기타사항", "권리자 및 기타사항", "권리자및 기타사항", "권리자 및기타사항", "권리자 및 기타 사항"],
    },
}


def classify_gab_or_eul(table: ParsedTable, page_lines: List[PageLine]) -> str:
    """
    테이블이 '갑구/을구' 중 어디에 속하는지 분류.
    1) page_lines(일반 OCR 텍스트)에서 테이블 바로 위의 '갑 구/을 구' 라벨을 찾는다.
    2) 없으면, 테이블 상단 몇 행(grid)에서 라벨을 찾는다.
    3) 그래도 없으면 unknown.
    """
    # table y0 추정 (bbox가 None이면 cells bbox로 다시 계산)
    y0 = None
    if table.bbox:
        y0 = table.bbox[1]
    else:
        ys = []
        for pc in table.cells:
            if pc.bbox:
                ys.append(pc.bbox[1])
        if ys:
            y0 = min(ys)

    def is_gab_marker(txt: str) -> bool:
        n = _norm(txt)
        # '갑구' 직접 또는 '소유권에 관한 사항' (이외/권리 제외)로 판정
        if "갑구" in n:
            return True
        if "소유권에관한사항" in n and "이외" not in n:
            return True
        # OCR 흔들림: '값구','각구' 등
        if ("값구" in n) or ("각구" in n):
            return True
        return False

    def is_eul_marker(txt: str) -> bool:
        n = _norm(txt)
        if "을구" in n:
            return True
        if "소유권이외의권리에관한사항" in n:
            return True
        # OCR 흔들림
        if "을구" in n:
            return True
        return False

    # 1) page_lines 기준: 테이블 위쪽에서 가장 가까운 마커
    if y0 is not None and page_lines:
        cand = [ln for ln in page_lines if ln.y < y0]
        # 가까운 것부터 역순
        cand.sort(key=lambda ln: ln.y, reverse=True)

        # 우선 가까운 범위(<=800px)에서 찾고, 없으면 전체에서 찾기
        for max_gap in (800, 2000, 10**9):
            for ln in cand:
                if (y0 - ln.y) > max_gap:
                    break
                if is_gab_marker(ln.text):
                    return "갑구"
                if is_eul_marker(ln.text):
                    return "을구"

    # 2) table grid 상단에서 라벨 탐색 (가끔 라벨이 테이블 내부로 들어오는 경우)
    top_rows = min(5, table.n_rows)
    for r in range(top_rows):
        row_text = " ".join(table.grid[r])
        if is_gab_marker(row_text):
            return "갑구"
        if is_eul_marker(row_text):
            return "을구"

    return "unknown"

    y0 = table.bbox[1]
    # 테이블 위 260px 범위에서 가장 가까운 라인
    cand = [ln for ln in page_lines if ln.y < y0 and (y0 - ln.y) <= 260]
    if not cand:
        return "unknown"
    cand.sort(key=lambda ln: ln.y, reverse=True)
    text_norm = _norm(cand[0].text)

    if "갑구" in text_norm or ("갑" in text_norm and "구" in text_norm and "을구" not in text_norm):
        return "갑구"
    if "을구" in text_norm or ("을" in text_norm and "구" in text_norm):
        return "을구"
    return "unknown"


def find_gab_tables(
    tables: List[ParsedTable],
) -> List[Tuple[ParsedTable, int, Dict[str, int]]]:
    """
    '순위번호/등기목적/접수/등기원인/권리자및기타사항' 헤더가 있는 테이블 후보 탐색
    (갑구/을구 공통 구조라서, 실제 갑/을 구 판별은 page_lines + bbox로 추가 분류)
    """
    out: List[Tuple[ParsedTable, int, Dict[str, int]]] = []

    for t in tables:
        if t.n_rows < 2 or t.n_cols < 4:
            continue

        header_row = -1
        for r in range(min(t.n_rows, 10)):
            row_norm = _norm(" ".join(t.grid[r]))
            hits = {k: 1 for k, spec in GAB_ONTOLOGY.items() if _contains_any(row_norm, spec["aliases"])}
            # 최소 조건: 순위번호 + 권리자 + 접수 + 등기목적 중 3~4개
            if ("rank" in hits) and ("holder" in hits) and (len(hits) >= 4 or ("purpose" in hits and "acceptance" in hits)):
                header_row = r
                break
        if header_row < 0:
            continue

        col_map: Dict[str, int] = {}
        for c in range(t.n_cols):
            cell_norm = _norm(t.grid[header_row][c])

            if "rank" not in col_map and _contains_any(cell_norm, GAB_ONTOLOGY["rank"]["aliases"]):
                col_map["rank"] = c
                continue
            if "purpose" not in col_map and _contains_any(cell_norm, GAB_ONTOLOGY["purpose"]["aliases"]):
                col_map["purpose"] = c
                continue
            if "acceptance" not in col_map and _contains_any(cell_norm, GAB_ONTOLOGY["acceptance"]["aliases"]):
                col_map["acceptance"] = c
                continue
            if "cause" not in col_map and _contains_any(cell_norm, GAB_ONTOLOGY["cause"]["aliases"]):
                col_map["cause"] = c
                continue
            if "holder" not in col_map and _contains_any(cell_norm, GAB_ONTOLOGY["holder"]["aliases"]):
                col_map["holder"] = c
                continue

        # fallback (일반적인 컬럼 순서)
        col_map.setdefault("rank", 0)
        col_map.setdefault("purpose", 1)
        col_map.setdefault("acceptance", min(2, t.n_cols - 1))
        col_map.setdefault("cause", min(3, t.n_cols - 1))
        col_map.setdefault("holder", min(4, t.n_cols - 1))

        # 순서 보정: rank < purpose < acceptance < cause < holder
        # 만약 뒤죽박죽이면 오탐일 가능성이 높아서 제외
        try:
            if not (col_map["rank"] <= col_map["purpose"] <= col_map["acceptance"] <= col_map["cause"] <= col_map["holder"]):
                # 그래도 holder가 맨 끝에 오도록 강제
                col_map["holder"] = max(col_map["holder"], col_map["cause"], col_map["acceptance"], col_map["purpose"])
                if col_map["holder"] >= t.n_cols:
                    col_map["holder"] = t.n_cols - 1
        except Exception:
            continue

        out.append((t, header_row, col_map))

    return out


GAB_PURPOSE_KEYWORDS = [
    # 갑구(소유권/압류/가처분 등)
    "소유권", "공유", "압류", "가압류", "가처분", "경매", "환매", "가등기", "신탁", "가처분", "처분금지",
    "말소", "이전", "변경", "회복", "보존",
]
EUL_PURPOSE_KEYWORDS = [
    # 을구(담보/임차권 등)
    "근저당", "저당", "전세권", "지상권", "임차권", "지역권", "담보", "질권", "저당권", "근질권",
]


def _purpose_type(purpose_text: str) -> str:
    """
    등기목적 텍스트 기반으로 갑구/을구 성격을 판정.
    return: 'gab' | 'eul' | 'unknown'
    """
    n = _norm(purpose_text or "")
    gab = 0
    eul = 0
    if any(k in n for k in map(_norm, GAB_PURPOSE_KEYWORDS)):
        gab += 1
    if any(k in n for k in map(_norm, EUL_PURPOSE_KEYWORDS)):
        eul += 1
    if gab == 0 and eul == 0:
        return "unknown"
    return "gab" if gab >= eul else "eul"


def guess_section_by_purpose_keywords(t: ParsedTable, header_row: int, col_map: Dict[str, int]) -> str:
    """
    '등기목적' 텍스트를 보고 갑구/을구를 추정하는 백업 로직.
    - 소유권/압류/가처분 등 → 갑구 가능성↑
    - 근저당/전세권/지상권/임차권 등 → 을구 가능성↑
    """
    c_purpose = col_map.get("purpose", 1)
    c_next = col_map.get("acceptance", min(c_purpose + 1, t.n_cols))
    gab = 0
    eul = 0

    for r in range(header_row + 1, min(t.n_rows, header_row + 30)):
        txt = join_cols(t.grid[r], c_purpose, c_next, sep=" ")
        n = _norm(txt)
        if not n:
            continue
        if any(k in n for k in map(_norm, GAB_PURPOSE_KEYWORDS)):
            gab += 1
        if any(k in n for k in map(_norm, EUL_PURPOSE_KEYWORDS)):
            eul += 1

    if gab == 0 and eul == 0:
        return "unknown"
    return "갑구" if gab >= eul else "을구"
def extract_gab_records_from_table(t: ParsedTable, header_row: int, col_map: Dict[str, int]) -> pd.DataFrame:
    c_rank = col_map["rank"]
    c_purpose = col_map["purpose"]
    c_acc = col_map["acceptance"]
    c_cause = col_map["cause"]
    c_holder = col_map["holder"]

    records: List[Dict[str, Any]] = []
    cur: Optional[Dict[str, Any]] = None

    for r in range(header_row + 1, t.n_rows):
        row = t.grid[r]
        if all((not (row[c] or "").strip()) for c in range(t.n_cols)):
            continue

        rank = (row[c_rank] or "").strip()

        # 잡음/헤더 반복 제거
        if rank and _contains_any(_norm(rank), ["순위번호", "갑구", "을구", "표제부"]):
            continue

        purpose = join_cols(row, c_purpose, c_acc, sep="\n") if c_purpose < c_acc else (row[c_purpose] or "").strip()
        acc = join_cols(row, c_acc, c_cause, sep="\n") if c_acc < c_cause else (row[c_acc] or "").strip()
        cause = join_cols(row, c_cause, c_holder, sep="\n") if c_cause < c_holder else (row[c_cause] or "").strip()
        holder = join_cols(row, c_holder, t.n_cols, sep="\n")

        # continuation: 순위번호가 비어있고 내용이 있으면 이전 레코드에 병합
        is_cont = (not rank) and (purpose or acc or cause or holder)
        if is_cont and cur is not None:
            if purpose:
                cur["등기목적"] = (cur["등기목적"] + "\n" + purpose).strip() if cur["등기목적"] else purpose
            if acc:
                cur["접수"] = (cur["접수"] + "\n" + acc).strip() if cur["접수"] else acc
            if cause:
                cur["등기원인"] = (cur["등기원인"] + "\n" + cause).strip() if cur["등기원인"] else cause
            if holder:
                cur["권리자 및 기타사항"] = (
                    (cur.get("권리자 및 기타사항") or "").strip() + "\n" + holder
                ).strip() if (cur.get("권리자 및 기타사항") or "").strip() else holder
            continue

        if rank:
            cur = {
                "페이지": t.page_no,
                "table_id": t.table_id,
                "순위번호": rank,
                "등기목적": purpose,
                "접수": acc,
                "등기원인": cause,
                "권리자 및 기타사항": holder,
            }
            records.append(cur)

    df = pd.DataFrame(records)
    if df.empty:
        return df

    # ✅ 을구(근저당/전세권/지상권 등) 성격의 행이 섞이면 갑구 결과가 깨지므로 제거
    if "등기목적" in df.columns:
        df["_ptype"] = df["등기목적"].apply(_purpose_type)
        df = df[df["_ptype"] != "eul"].drop(columns=["_ptype"], errors="ignore")
        if df.empty:
            return df

    # 정렬(순위번호)
    def rank_key(x: Any) -> Tuple[int, int, str]:
        s = str(x or "").strip()
        m = re.match(r"^(\d+)(?:-(\d+))?", s)
        if not m:
            return (10**9, 0, s)
        a = int(m.group(1))
        b = int(m.group(2) or 0)
        return (a, b, s)

    df["_rk"] = df["순위번호"].apply(rank_key)
    df = df.sort_values("_rk").drop(columns=["_rk"])

    cols = ["페이지", "table_id", "순위번호", "등기목적", "접수", "등기원인", "권리자 및 기타사항"]
    return df[[c for c in cols if c in df.columns]]


# ============================================================
# 7) 지번(필지)별 그룹핑
# ============================================================
@dataclass
class ParcelGroup:
    key: str
    start_page: int
    end_page: int
    pyo_tables: List[ParsedTable] = field(default_factory=list)
    pyo_df: pd.DataFrame = field(default_factory=pd.DataFrame)
    gab_tables: List[ParsedTable] = field(default_factory=list)
    gab_df: pd.DataFrame = field(default_factory=pd.DataFrame)


def _pick_group_key_from_pyo_df(df: pd.DataFrame, fallback: str) -> str:
    if "지번" in df.columns:
        vals = [str(v).strip() for v in df["지번"].tolist() if str(v).strip()]
        if vals:
            return vals[0]
    return fallback


def group_parcels_from_pyo(
    pyo_items: List[Tuple[ParsedTable, pd.DataFrame]],
    total_pages: int,
) -> List[ParcelGroup]:
    """
    표제부 결과를 기반으로 지번별 그룹을 만들고, 페이지 범위를 추정한다.
    - start_page: 해당 지번의 표제부가 처음 등장한 페이지
    - end_page: 다음 지번 start_page - 1 (마지막은 total_pages)
    """
    tmp: Dict[str, Dict[str, Any]] = {}
    for t, df in pyo_items:
        key = _pick_group_key_from_pyo_df(df, fallback=t.table_id)
        g = tmp.setdefault(key, {"start": t.page_no, "tables": [], "dfs": []})
        g["start"] = min(g["start"], t.page_no)
        g["tables"].append(t)
        g["dfs"].append(df)

    groups: List[ParcelGroup] = []
    for key, g in tmp.items():
        pyo_df = pd.concat(g["dfs"], ignore_index=True) if g["dfs"] else pd.DataFrame()
        pyo_df = pyo_df.drop_duplicates()
        groups.append(ParcelGroup(key=key, start_page=int(g["start"]), end_page=total_pages, pyo_tables=g["tables"], pyo_df=pyo_df))

    groups.sort(key=lambda x: x.start_page)

    # end_page 계산
    for i in range(len(groups)):
        if i < len(groups) - 1:
            groups[i].end_page = max(groups[i].start_page, groups[i + 1].start_page - 1)
        else:
            groups[i].end_page = total_pages

    return groups


def assign_table_to_group(groups: List[ParcelGroup], page_no: int) -> Optional[ParcelGroup]:
    for g in groups:
        if g.start_page <= page_no <= g.end_page:
            return g
    return None


# -----------------------------
# (보강) 갑구 테이블을 지번(필지) 그룹에 더 정확히 매핑하기 위한 lot 추정
# -----------------------------
_LOT_SHORT_RE = re.compile(r"\b(\d{1,5}-\d{1,5})\b")


def guess_lot_from_lines(page_lines: List[PageLine]) -> str:
    """
    페이지의 텍스트 라인에서 지번 후보를 추정.
    - 1~5자리-1~5자리 패턴만 사용(주민/법인번호 등 긴 패턴 배제)
    - '리/동/지번/소재지' 같은 문맥이 있으면 가중치
    """
    if not page_lines:
        return ""

    scores: Dict[str, float] = {}
    # 상단부에 보통 소재지가 있으므로 위쪽(작은 y)부터 30줄만
    lines_sorted = sorted(page_lines, key=lambda x: x.y)[:30]
    for ln in lines_sorted:
        txt = ln.text or ""
        for lot in _LOT_SHORT_RE.findall(txt):
            sc = 1.0
            if re.search(r"(리|동)\s*" + re.escape(lot), txt):
                sc += 2.0
            if ("소재지" in txt) or ("지번" in txt) or ("토지" in txt):
                sc += 1.0
            # 위쪽일수록 가중치(0~1)
            sc += max(0.0, 1.0 - (ln.y / 2000.0))
            scores[lot] = scores.get(lot, 0.0) + sc

    if not scores:
        return ""

    # 최고점 lot 반환
    return max(scores.items(), key=lambda x: x[1])[0]


def guess_lot_from_table_text(t: ParsedTable) -> str:
    """
    테이블 텍스트에서 지번 후보를 추정.
    - 권리자/기타사항에 주소(…리 496-10)가 들어가는 케이스가 많아 효과적
    """
    scores: Dict[str, float] = {}
    # 너무 큰 테이블도 있으니, 상단 40행 정도만
    max_r = min(t.n_rows, 40)
    for r in range(max_r):
        row_txt = " ".join((t.grid[r][c] or "") for c in range(t.n_cols))
        for lot in _LOT_SHORT_RE.findall(row_txt):
            sc = 1.0
            if re.search(r"(리|동)\s*" + re.escape(lot), row_txt):
                sc += 2.0
            scores[lot] = scores.get(lot, 0.0) + sc

    if not scores:
        return ""
    return max(scores.items(), key=lambda x: x[1])[0]


def guess_lot_key_for_gab_table(t: ParsedTable, page_lines: List[PageLine]) -> str:
    """
    갑구 테이블의 지번 key 추정:
    1) 테이블 텍스트에서 추정
    2) 실패 시 페이지 라인에서 추정
    """
    lot = guess_lot_from_table_text(t)
    if lot:
        return lot
    return guess_lot_from_lines(page_lines)


# ============================================================
# 8) Excel 생성 (지번별 시트 분리)
# ============================================================
def _safe_sheet_name(name: str, used: set) -> str:
    n = re.sub(r"[\[\]\*:/\\\?]", "_", name).strip()
    if not n:
        n = "sheet"
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
        ws.column_dimensions[letter].width = min(80, max(10, max_len + 2))


def _write_df(ws, df: pd.DataFrame):
    wrap = Alignment(wrap_text=True, vertical="top")
    ws.append(list(df.columns))
    for _, r in df.iterrows():
        ws.append([r.get(c, "") for c in df.columns])
    for row in ws.iter_rows(min_row=1, max_row=ws.max_row, min_col=1, max_col=ws.max_column):
        for cell in row:
            cell.alignment = wrap
    _autosize(ws, ws.max_column, ws.max_row)


def build_registry_excel_bytes(
    *,
    groups: List[ParcelGroup],
    include_raw_pyo: bool,
    include_raw_gab: bool,
    merge_cells_on_raw: bool,
) -> bytes:
    wb = Workbook()
    ws_index = wb.active
    ws_index.title = "INDEX"
    ws_index.append(["지번(그룹)", "페이지범위", "표제부 행수", "갑구 행수", "표제부 테이블 수", "갑구 테이블 수"])

    used = {"INDEX"}

    # 그룹별 시트
    for g in groups:
        # index row
        ws_index.append([
            g.key,
            f"{g.start_page}-{g.end_page}",
            int(len(g.pyo_df)) if isinstance(g.pyo_df, pd.DataFrame) else 0,
            int(len(g.gab_df)) if isinstance(g.gab_df, pd.DataFrame) else 0,
            len(g.pyo_tables),
            len(g.gab_tables),
        ])

        # 표제부 시트
        pyo_df = g.pyo_df if isinstance(g.pyo_df, pd.DataFrame) else pd.DataFrame()
        pyo_cols_clean = ["표시번호", "접수", "소재지번", "지목", "면적", "등기원인 및 기타사항"]
        pyo_export = pyo_df[[c for c in pyo_cols_clean if c in pyo_df.columns]].copy() if not pyo_df.empty else pd.DataFrame(columns=pyo_cols_clean)
        ws_pyo = wb.create_sheet(_safe_sheet_name(f"표제부_{g.key}", used))
        if pyo_export.empty:
            ws_pyo.append(["표제부 없음"])
        else:
            _write_df(ws_pyo, pyo_export)

        # 갑구 시트
        gab_df = g.gab_df if isinstance(g.gab_df, pd.DataFrame) else pd.DataFrame()
        gab_cols_clean = ["순위번호", "등기목적", "접수", "등기원인", "권리자 및 기타사항"]
        gab_export = gab_df[[c for c in gab_cols_clean if c in gab_df.columns]].copy() if not gab_df.empty else pd.DataFrame(columns=gab_cols_clean)
        ws_gab = wb.create_sheet(_safe_sheet_name(f"갑구_{g.key}", used))
        if gab_export.empty:
            ws_gab.append(["갑구 없음"])
        else:
            _write_df(ws_gab, gab_export)

        # raw tables (선택)
        wrap = Alignment(wrap_text=True, vertical="top")

        def write_raw_table(t: ParsedTable, sheet_prefix: str):
            sname = _safe_sheet_name(f"{sheet_prefix}_{t.table_id}", used)
            ws = wb.create_sheet(sname)
            for r in range(t.n_rows):
                ws.append(t.grid[r])
            for row in ws.iter_rows(min_row=1, max_row=t.n_rows, min_col=1, max_col=t.n_cols):
                for cell in row:
                    cell.alignment = wrap
            if merge_cells_on_raw:
                for (r0, c0, r1, c1) in t.merges:
                    ws.merge_cells(start_row=r0 + 1, start_column=c0 + 1, end_row=r1 + 1, end_column=c1 + 1)
            _autosize(ws, t.n_cols, t.n_rows)

        if include_raw_pyo:
            for t in g.pyo_tables:
                write_raw_table(t, "rawPYO")

        if include_raw_gab:
            for t in g.gab_tables:
                write_raw_table(t, "rawGAB")

    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


# ============================================================
# 9) JSON-LD(온톨로지) — 표제부 + 갑구
# ============================================================
def build_registry_jsonld(
    *,
    file_name: str,
    file_hash: str,
    groups: List[ParcelGroup],
    base_iri: str = "urn:dovi:",
    generator: str = "DOVI-REGISTRY",
) -> Dict[str, Any]:
    """
    매우 단순한 JSON-LD 그래프:
      Document -> Parcel -> Facts(표제부) + GabEntry(갑구 항목)
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
        "GabEntry": "dovi:GabEntry",

        "fileName": "schema:name",
        "fileHash": "dovi:fileHash",
        "createdAt": {"@id": "schema:dateCreated", "@type": "xsd:dateTime"},
        "generator": "schema:generator",

        "mentionsParcel": {"@id": "dovi:mentionsParcel", "@type": "@id"},
        "hasFact": {"@id": "dovi:hasFact", "@type": "@id"},
        "hasGabEntry": {"@id": "dovi:hasGabEntry", "@type": "@id"},

        "lot": "dovi:lot",
        "siteAddress": "dovi:siteAddress",

        "field": "dovi:field",
        "value": "dovi:value",

        "rankNo": "dovi:rankNo",
        "purpose": "dovi:purpose",
        "acceptance": "dovi:acceptance",
        "cause": "dovi:cause",
        "holderNote": "dovi:holderNote",
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

    def parcel_id(key: str) -> str:
        hid = hashlib.sha1(key.encode("utf-8")).hexdigest()[:12]
        return f"{doc_id}#parcel-{hid}"

    for g in groups:
        pid = parcel_id(g.key)
        doc_node["mentionsParcel"].append(pid)

        # 표제부에서 대표 주소(첫 행) 가져오기
        addr = ""
        if isinstance(g.pyo_df, pd.DataFrame) and not g.pyo_df.empty and "소재지번" in g.pyo_df.columns:
            addr = str(g.pyo_df.iloc[0].get("소재지번", "") or "")

        parcel_node: Dict[str, Any] = {
            "@id": pid,
            "@type": "Parcel",
            "lot": g.key,
            "siteAddress": addr,
            "hasFact": [],
            "hasGabEntry": [],
        }
        graph.append(parcel_node)

        # 표제부 Facts
        if isinstance(g.pyo_df, pd.DataFrame) and not g.pyo_df.empty:
            for _, row in g.pyo_df.iterrows():
                for field_name in ["표시번호", "접수", "소재지번", "지목", "면적", "등기원인 및 기타사항"]:
                    v = str(row.get(field_name, "") or "").strip()
                    if not v:
                        continue
                    fid = f"{pid}#fact-{hashlib.sha1((field_name+'|'+v).encode('utf-8')).hexdigest()[:10]}"
                    graph.append({"@id": fid, "@type": "Fact", "field": field_name, "value": v})
                    parcel_node["hasFact"].append(fid)

        # 갑구 Entries
        if isinstance(g.gab_df, pd.DataFrame) and not g.gab_df.empty:
            for _, row in g.gab_df.iterrows():
                rank = str(row.get("순위번호", "") or "").strip()
                purpose = str(row.get("등기목적", "") or "").strip()
                acc = str(row.get("접수", "") or "").strip()
                cause = str(row.get("등기원인", "") or "").strip()
                holder = str(row.get("권리자 및 기타사항", "") or "").strip()
                if not (rank or purpose or acc or cause or holder):
                    continue
                eid = f"{pid}#gab-{hashlib.sha1((rank+'|'+purpose+'|'+acc).encode('utf-8')).hexdigest()[:12]}"
                graph.append(
                    {
                        "@id": eid,
                        "@type": "GabEntry",
                        "rankNo": rank,
                        "purpose": purpose,
                        "acceptance": acc,
                        "cause": cause,
                        "holderNote": holder,
                    }
                )
                parcel_node["hasGabEntry"].append(eid)

    return {"@context": context, "@graph": graph}


def make_jsonld_bytes(obj: Dict[str, Any]) -> bytes:
    return json.dumps(obj, ensure_ascii=False, indent=2).encode("utf-8")


# ============================================================
# 10) 전체 처리
# ============================================================
def process_pdf(
    file_bytes: bytes,
    api_url: str,
    secret_key: str,
    *,
    pages_per_request: int,
    lang: str,
    progress_cb: Optional[Callable[[int, int, int, int], None]] = None,
) -> Tuple[List[ParcelGroup], int]:
    """
    반환:
      - groups: 지번별 ParcelGroup 리스트(표제부+갑구)
      - total_pages
    """
    pages_per_request = max(1, min(int(pages_per_request), MAX_PAGES_PER_REQUEST))
    total_pages = get_pdf_total_pages(file_bytes)
    chunks = split_pdf_into_chunks(file_bytes, pages_per_request)

    all_tables: List[ParsedTable] = []
    all_page_lines: Dict[int, List[PageLine]] = {}

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
        tables, page_lines = parse_tables_and_lines_from_ocr_json(ocr_json, page_numbers=page_numbers)
        all_tables.extend(tables)
        all_page_lines.update(page_lines)

    if progress_cb:
        progress_cb(len(chunks), len(chunks), 0, 0)

    # --------------------
    # 표제부 추출
    # --------------------
    pyo_candidates = find_pyo_tables(all_tables)
    pyo_items: List[Tuple[ParsedTable, pd.DataFrame]] = []
    for (t, header_row, col_map) in pyo_candidates:
        df = extract_pyo_records_from_table(t, header_row, col_map)
        if not df.empty:
            pyo_items.append((t, df))

    groups = group_parcels_from_pyo(pyo_items, total_pages=total_pages)

    # 표제부가 하나도 없으면 UNKNOWN 그룹 하나 생성
    if not groups:
        groups = [ParcelGroup(key="UNKNOWN", start_page=1, end_page=total_pages)]

    # 그룹에 표제부 테이블/DF 할당
    # (group_parcels_from_pyo가 이미 할당했지만, UNKNOWN 케이스 대비)
    if groups and pyo_items:
        by_key = {g.key: g for g in groups}
        for t, df in pyo_items:
            key = _pick_group_key_from_pyo_df(df, fallback=t.table_id)
            g = by_key.get(key)
            if g is None:
                # 새 그룹 생성(예외)
                g = ParcelGroup(key=key, start_page=t.page_no, end_page=total_pages)
                groups.append(g)
                by_key[key] = g
            g.pyo_tables.append(t)
            g.pyo_df = pd.concat([g.pyo_df, df], ignore_index=True) if isinstance(g.pyo_df, pd.DataFrame) and not g.pyo_df.empty else df

    # --------------------
    # 갑구 추출
    # --------------------
    gab_candidates = find_gab_tables(all_tables)

    gab_df_by_group: Dict[str, List[pd.DataFrame]] = {g.key: [] for g in groups}

    for (t, header_row, col_map) in gab_candidates:
        # 1차: 페이지 라벨(갑구/을구) 기반 분류
        section = classify_gab_or_eul(t, all_page_lines.get(t.page_no, []))

        # 2차: 라벨이 안 잡히면 등기목적 키워드로 추정
        if section == "unknown":
            section = guess_section_by_purpose_keywords(t, header_row, col_map)

        # 3차: 라벨이 '갑구'로 찍혔더라도, 실제 내용이 을구 키워드가 압도하면 을구로 뒤집기(오탐 방지)
        if section in ("갑구", "을구"):
            by_kw = guess_section_by_purpose_keywords(t, header_row, col_map)
            if by_kw != "unknown" and by_kw != section:
                section = by_kw

        if section != "갑구":
            continue  # ✅ 지금은 갑구만

        df = extract_gab_records_from_table(t, header_row, col_map)
        if df.empty:
            continue

        # ✅ 표제부가 누락되어도 갑구 표 안에서 지번이 나오는 경우가 많음 → 지번 기반으로 그룹 매핑
        lot_key = guess_lot_key_for_gab_table(t, all_page_lines.get(t.page_no, []))

        # 그룹 dict(동적) 구성
        by_key = {gg.key: gg for gg in groups}

        g: Optional[ParcelGroup] = None
        if lot_key:
            g = by_key.get(lot_key)
            if g is None:
                # 표제부를 못 찾은 지번이라도 갑구에서 지번이 추정되면 그룹 생성
                g = ParcelGroup(key=lot_key, start_page=t.page_no, end_page=total_pages)
                groups.append(g)
                gab_df_by_group.setdefault(g.key, [])
            # start_page 보강
            g.start_page = min(g.start_page, t.page_no)
        else:
            # 지번 추정 실패 → 기존 방식(페이지 범위)로 할당
            g = assign_table_to_group(groups, t.page_no)

        if g is None:
            # 마지막 fallback: UNKNOWN
            g = next((x for x in groups if x.key == "UNKNOWN"), None)
            if g is None:
                g = ParcelGroup(key="UNKNOWN", start_page=1, end_page=total_pages)
                groups.append(g)
                gab_df_by_group.setdefault(g.key, [])

        g.gab_tables.append(t)
        gab_df_by_group.setdefault(g.key, []).append(df)

    # 그룹별 gab_df finalize
    for g in groups:
        dfs = gab_df_by_group.get(g.key) or []
        if dfs:
            gab_df = pd.concat(dfs, ignore_index=True).drop_duplicates()
            # 컬럼 정리
            cols = ["페이지", "table_id", "순위번호", "등기목적", "접수", "등기원인", "권리자 및 기타사항"]
            g.gab_df = gab_df[[c for c in cols if c in gab_df.columns]]
        else:
            g.gab_df = pd.DataFrame(columns=["순위번호", "등기목적", "접수", "등기원인", "권리자 및 기타사항"])

    # 표제부 DF 중복 제거
    for g in groups:
        if isinstance(g.pyo_df, pd.DataFrame) and not g.pyo_df.empty:
            g.pyo_df = g.pyo_df.drop_duplicates()

    # --------------------
    # 그룹 범위 재계산(표제부를 못 찾은 지번이 갑구에서 추가된 경우 대비)
    # --------------------
    # start_page 보강: pyo_tables/gab_tables의 최소 페이지로 재계산
    for g in groups:
        pages = []
        pages.extend([t.page_no for t in g.pyo_tables])
        pages.extend([t.page_no for t in g.gab_tables])
        if pages:
            g.start_page = min([g.start_page] + pages)

    groups.sort(key=lambda x: x.start_page)

    for i in range(len(groups)):
        if i < len(groups) - 1:
            groups[i].end_page = max(groups[i].start_page, groups[i + 1].start_page - 1)
        else:
            groups[i].end_page = total_pages

    return groups, total_pages


# ============================================================
# 11) Streamlit UI
# ============================================================
def main():
    st.set_page_config(page_title=APP_TITLE, page_icon="📄", layout="wide")
    st.title(APP_TITLE)
    st.caption(f"{APP_VERSION} | Naver Table OCR(enableTableDetection) → 표제부/갑구 정리")

    st.markdown(
        """
### 기능
- 네이버 **표추출 OCR(enableTableDetection)** 로 PDF 전체를 인식
- **표제부**: 표시번호/접수/소재지번/지목/면적/등기원인 및 기타사항 정리
- **갑구**: 순위번호/등기목적/접수/등기원인/권리자 및 기타사항 정리
- 표제부/갑구가 여러 지번으로 존재하면 **지번별로 표를 분리**해서 보여주고 엑셀 시트도 분리

> 표 추출은 네이버 콘솔 Domain에서 **‘표 추출 여부’가 ON**이어야 합니다.
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
        include_raw_pyo = st.checkbox("표제부 raw 테이블 시트 포함", value=False)
        include_raw_gab = st.checkbox("갑구 raw 테이블 시트 포함", value=False)
        merge_cells_on_raw = st.checkbox("raw 시트에서 병합셀 반영", value=True)

        st.divider()
        st.header("🧠 온톨로지(JSON-LD)")
        export_jsonld = st.checkbox("표제부+갑구 JSON-LD 생성", value=False)

    uploaded_file = st.file_uploader("📎 PDF 업로드", type=["pdf"])
    if uploaded_file is None:
        st.info("PDF를 업로드하면 시작할 수 있어요.")
        return

    file_bytes = uploaded_file.getvalue()
    file_hash = hashlib.sha256(file_bytes).hexdigest()

    if st.session_state.get("file_hash") != file_hash:
        for k in ["groups", "excel_bytes", "jsonld_obj", "jsonld_bytes", "total_pages"]:
            st.session_state.pop(k, None)
        st.session_state["file_hash"] = file_hash

    clicked = st.button("🚀 표제부+갑구 추출 시작", disabled=not bool(api_url and secret_key))
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

        with st.spinner("OCR 및 표제부/갑구 정리 중..."):
            groups, total_pages = process_pdf(
                file_bytes,
                api_url,
                secret_key,
                pages_per_request=int(pages_per_req),
                lang=str(lang),
                progress_cb=progress_cb,
            )

            excel_bytes = build_registry_excel_bytes(
                groups=groups,
                include_raw_pyo=include_raw_pyo,
                include_raw_gab=include_raw_gab,
                merge_cells_on_raw=merge_cells_on_raw,
            )

            jsonld_obj = None
            jsonld_bytes = b""
            if export_jsonld:
                jsonld_obj = build_registry_jsonld(
                    file_name=uploaded_file.name,
                    file_hash=file_hash,
                    groups=groups,
                )
                jsonld_bytes = make_jsonld_bytes(jsonld_obj)

        st.session_state["groups"] = groups
        st.session_state["excel_bytes"] = excel_bytes
        st.session_state["jsonld_obj"] = jsonld_obj
        st.session_state["jsonld_bytes"] = jsonld_bytes
        st.session_state["total_pages"] = total_pages

        progress.progress(100)
        status.write("✅ 완료")

    # 결과 표시
    groups_obj = st.session_state.get("groups")
    if groups_obj is None:
        st.info("추출을 실행하면 결과가 여기에 표시됩니다.")
        return

    groups: List[ParcelGroup] = groups_obj
    excel_bytes: bytes = st.session_state.get("excel_bytes", b"")
    jsonld_bytes: bytes = st.session_state.get("jsonld_bytes", b"")

    st.divider()
    st.subheader(f"✅ 추출 결과 (지번 그룹 수: {len(groups)})")

    # 다운로드
    if excel_bytes:
        st.download_button(
            "📥 엑셀 다운로드 (지번별 시트: 표제부/갑구)",
            data=excel_bytes,
            file_name=f"{uploaded_file.name}_등기정리.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="download_excel",
        )

    if export_jsonld and jsonld_bytes:
        st.download_button(
            "🧠 JSON-LD 다운로드",
            data=jsonld_bytes,
            file_name=f"{uploaded_file.name}_등기정리.jsonld",
            mime="application/ld+json",
            key="download_jsonld",
        )

    # 그룹별 표시 (탭/익스팬더)
    group_labels = [f"{g.key} (p{g.start_page}-{g.end_page})" for g in groups]

    if len(groups) <= 8:
        tabs = st.tabs(group_labels)
        for tab, g in zip(tabs, groups):
            with tab:
                _render_group(g)
    else:
        for g in groups:
            with st.expander(f"📌 {g.key} (p{g.start_page}-{g.end_page})", expanded=False):
                _render_group(g)

    if export_jsonld:
        with st.expander("🧠 JSON-LD 미리보기", expanded=False):
            st.json(st.session_state.get("jsonld_obj", {}))


def _render_group(g: ParcelGroup):
    # 표제부
    st.markdown("#### 표제부")
    pyo_df = g.pyo_df if isinstance(g.pyo_df, pd.DataFrame) else pd.DataFrame()
    pyo_cols = ["표시번호", "접수", "소재지번", "지목", "면적", "등기원인 및 기타사항"]
    if pyo_df.empty:
        st.info("표제부 없음")
    else:
        st.dataframe(pyo_df[[c for c in pyo_cols if c in pyo_df.columns]], use_container_width=True, hide_index=True)

    # 갑구
    st.markdown("#### 갑구")
    gab_df = g.gab_df if isinstance(g.gab_df, pd.DataFrame) else pd.DataFrame()
    gab_cols = ["순위번호", "등기목적", "접수", "등기원인", "권리자 및 기타사항"]
    if gab_df.empty:
        st.info("갑구 없음(또는 갑구 표 검출 실패)")
        st.caption("팁: '갑 구' 라벨이 OCR로 안 잡혀도, 이제는 등기목적(소유권/근저당 등) 키워드로 2차 추정합니다. 그래도 비면 표 자체가 tables로 검출되지 않았을 가능성이 큽니다.")
    else:
        st.dataframe(gab_df[[c for c in gab_cols if c in gab_df.columns]], use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()

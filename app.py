# -*- coding: utf-8 -*-
"""
문서 비서 📄  (All Tables Extractor)
-----------------------------------
목표:
- "네이버 일반 OCR"만 사용 (표 OCR X)
- PDF 안에 있는 "모든 표"를 bbox(좌표) 기반으로 최대한 테이블 형태로 복원
- 결과를 Excel(표마다 시트)로 저장
- 동시에 "간단 온톨로지(그래프)" JSON-LD로도 저장/다운로드(선택)

중요:
- OCR 기반 테이블 복원은 완전무결할 수 없습니다(병합셀/회전/표선 없는 레이아웃 등).
- 대신 '최대한 많이' '일관되게' 뽑고, 디버깅 가능한 근거(좌표/인덱스)도 함께 제공합니다.

실행:
  streamlit run app.py

requirements.txt 예시:
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

# (온톨로지/그래프 저장) JSON-LD 빌더
from ontology_tables import build_tables_jsonld, make_jsonld_bytes


# ============================================================
# 0) 설정
# ============================================================
APP_TITLE = "문서 비서📄 dev — 모든 표 추출(Naver OCR)"
APP_VERSION = "v0.3.0"

DEFAULT_PASSWORD = "alohomora"  # 데모용

# 네이버 OCR PDF 요청당 페이지 수(보수적으로 10)
OCR_PAGES_PER_REQUEST = 10

# 테이블 판정/분리 하이퍼파라미터(기본값)
DEFAULT_MIN_TABLE_LINES = 2           # 최소 몇 줄 이상이면 "표"로 인정할지
DEFAULT_MIN_COLS = 2                  # 최소 컬럼 수
DEFAULT_BIG_GAP_MULT = 3.0            # y 간격이 (median_line_height * 이 값)보다 크면 블록 분리
DEFAULT_ANCHOR_TOL_PCT = 0.015        # 컬럼 앵커 clustering tolerance: page_width * pct
DEFAULT_ANCHOR_MIN_SUPPORT_RATIO = 0.35  # 블록 내에서 앵커가 최소 이 비율 이상 라인에서 나타나야 컬럼으로 인정

# Excel sheet name 제한(31)
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


def pdf_reader_from_bytes(pdf_bytes: bytes):
    PdfReader, _ = _import_pypdf()
    return PdfReader(io.BytesIO(pdf_bytes))


def build_pdf_bytes_from_pages(reader, page_indices_0based: List[int]) -> bytes:
    _, PdfWriter = _import_pypdf()
    w = PdfWriter()
    for idx in page_indices_0based:
        w.add_page(reader.pages[idx])
    buf = io.BytesIO()
    w.write(buf)
    return buf.getvalue()


def chunk_list(seq: List[int], size: int) -> List[List[int]]:
    return [seq[i : i + size] for i in range(0, len(seq), size)]


# ============================================================
# 2) 네이버 OCR 호출
# ============================================================
def call_naver_ocr(file_bytes: bytes, file_ext: str, api_url: str, secret_key: str, *, timeout: int = 120) -> Dict[str, Any]:
    request_json = {
        "images": [{"format": file_ext, "name": "upload"}],
        "requestId": str(uuid.uuid4()),
        "version": "V2",
        "timestamp": int(round(time.time() * 1000)),
    }
    payload = {"message": json.dumps(request_json)}
    headers = {"X-OCR-SECRET": secret_key}

    content_type = "application/pdf" if file_ext.lower() == "pdf" else "image/jpeg"
    files = {"file": (f"upload.{file_ext}", file_bytes, content_type)}

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
# 3) OCR JSON -> 토큰
# ============================================================
@dataclass
class Token:
    text: str
    page: int  # 원본 PDF 페이지 번호(1-indexed)
    x0: float
    y0: float
    x1: float
    y1: float

    @property
    def cx(self) -> float:
        return (self.x0 + self.x1) / 2.0

    @property
    def cy(self) -> float:
        return (self.y0 + self.y1) / 2.0

    @property
    def h(self) -> float:
        return max(0.0, self.y1 - self.y0)


@dataclass
class Line:
    page: int
    y: float
    h: float
    tokens: List[Token]


@dataclass
class ExtractedTable:
    table_id: str
    sheet_name: str
    page_start: int
    page_end: int
    bbox: Tuple[float, float, float, float]  # (x0,y0,x1,y1)
    n_rows: int
    n_cols: int
    df: pd.DataFrame
    # optional cell bbox map: (r,c) -> (x0,y0,x1,y1)
    cell_bboxes: Optional[Dict[Tuple[int, int], Tuple[float, float, float, float]]] = None


def ocr_json_to_tokens(ocr_json: Dict[str, Any], page_numbers: List[int]) -> List[Token]:
    tokens: List[Token] = []
    images = ocr_json.get("images", []) if isinstance(ocr_json, dict) else []
    for img_idx, img in enumerate(images):
        page_no = page_numbers[img_idx] if img_idx < len(page_numbers) else (img_idx + 1)
        for f in img.get("fields", []):
            txt = (f.get("inferText") or "").strip()
            if not txt:
                continue
            verts = (f.get("boundingPoly") or {}).get("vertices", [])
            if not verts:
                continue
            xs = [v.get("x", 0) for v in verts]
            ys = [v.get("y", 0) for v in verts]
            tokens.append(
                Token(
                    text=txt,
                    page=page_no,
                    x0=float(min(xs)),
                    y0=float(min(ys)),
                    x1=float(max(xs)),
                    y1=float(max(ys)),
                )
            )
    return tokens


def ocr_pdf_all_pages(
    file_bytes: bytes,
    api_url: str,
    secret_key: str,
    *,
    pages_per_request: int = OCR_PAGES_PER_REQUEST,
    progress_cb: Optional[Callable[[int, int, int, int], None]] = None,
) -> Dict[int, List[Token]]:
    """
    PDF 전체 페이지를 OCR 돌리고 페이지별 Token dict 반환.
    """
    reader = pdf_reader_from_bytes(file_bytes)
    total_pages = len(reader.pages)
    all_indices = list(range(total_pages))
    chunks = chunk_list(all_indices, pages_per_request)

    tokens_by_page: Dict[int, List[Token]] = {i + 1: [] for i in range(total_pages)}

    for ci, chunk in enumerate(chunks, start=1):
        start_p, end_p = min(chunk) + 1, max(chunk) + 1
        if progress_cb:
            progress_cb(ci, len(chunks), start_p, end_p)

        chunk_pdf = build_pdf_bytes_from_pages(reader, chunk)
        result = call_naver_ocr(chunk_pdf, "pdf", api_url, secret_key)

        if not result.get("ok"):
            raise RuntimeError(
                f"OCR 실패 (chunk {ci}/{len(chunks)}; pages {start_p}-{end_p})\n"
                f"status={result.get('status_code')}\n{result.get('text') or result.get('error')}"
            )

        ocr_json = result.get("json")
        if not ocr_json:
            raise RuntimeError(
                f"OCR JSON 파싱 실패 (chunk {ci}/{len(chunks)}; pages {start_p}-{end_p})\n{result.get('text')}"
            )

        page_numbers = [i + 1 for i in chunk]
        toks = ocr_json_to_tokens(ocr_json, page_numbers)

        for tok in toks:
            tokens_by_page.setdefault(tok.page, []).append(tok)

    if progress_cb:
        progress_cb(len(chunks), len(chunks), total_pages, total_pages)

    return tokens_by_page


# ============================================================
# 4) Token -> Line
# ============================================================
def group_lines(tokens: List[Token]) -> List[Line]:
    if not tokens:
        return []

    # 토큰 높이 중앙값 기반 y threshold
    hs = sorted([t.h for t in tokens if t.h > 0])
    base_h = hs[len(hs) // 2] if hs else 12.0
    y_thresh = max(8.0, base_h * 0.65)

    toks = sorted(tokens, key=lambda t: (t.page, t.cy, t.cx))

    lines: List[Line] = []
    cur: List[Token] = []
    cur_page = toks[0].page
    last_y = toks[0].cy
    line_hs: List[float] = []

    def flush():
        nonlocal cur, line_hs, last_y
        if not cur:
            return
        cur_sorted = sorted(cur, key=lambda t: t.cx)
        y = sum(t.cy for t in cur_sorted) / len(cur_sorted)
        h = sorted(line_hs)[len(line_hs) // 2] if line_hs else base_h
        lines.append(Line(page=cur_page, y=y, h=h, tokens=cur_sorted))
        cur = []
        line_hs = []

    for tok in toks:
        if tok.page != cur_page:
            flush()
            cur_page = tok.page
            last_y = tok.cy

        if cur and abs(tok.cy - last_y) > y_thresh:
            flush()

        cur.append(tok)
        line_hs.append(tok.h)
        last_y = tok.cy

    flush()
    return lines


# ============================================================
# 5) 테이블 추출 (페이지 단위, bbox 기반)
# ============================================================
def _page_width(tokens: List[Token]) -> float:
    if not tokens:
        return 1000.0
    return max(t.x1 for t in tokens) - min(t.x0 for t in tokens)


def _cluster_1d(values: List[float], tol: float) -> List[Tuple[float, List[int]]]:
    """
    1D 값들을 tol로 클러스터링.
    반환: [(center, indices), ...] where indices refer to original list positions
    """
    if not values:
        return []
    order = sorted(range(len(values)), key=lambda i: values[i])
    clusters: List[List[int]] = []
    cur = [order[0]]
    cur_center = values[order[0]]

    for idx in order[1:]:
        v = values[idx]
        if abs(v - cur_center) <= tol:
            cur.append(idx)
            cur_center = sum(values[i] for i in cur) / len(cur)
        else:
            clusters.append(cur)
            cur = [idx]
            cur_center = v
    clusters.append(cur)

    out = []
    for c in clusters:
        center = sum(values[i] for i in c) / len(c)
        out.append((center, c))
    return out


def _line_is_tableish(line: Line, page_w: float) -> bool:
    """
    '표 같은 줄'인지 판단.
    - 토큰이 3개 이상이면 우선 tableish
    - 또는 토큰이 2개 이상이고, 큰 gap(컬럼 분리로 보이는)이 있으면 tableish
    """
    toks = line.tokens
    if len(toks) >= 3:
        return True
    if len(toks) >= 2:
        # 큰 gap 체크
        gaps = []
        for a, b in zip(toks, toks[1:]):
            gaps.append(b.x0 - a.x1)
        big_gap = max(gaps) if gaps else 0
        # 페이지 폭 대비 3% 또는 35px 이상이면 컬럼 분리로 간주
        if big_gap > max(35.0, page_w * 0.03):
            return True
    return False


def _build_column_anchors(block_lines: List[Line], page_w: float, *, tol_pct: float, min_support_ratio: float) -> List[float]:
    """
    블록(연속 tableish 라인)에서 컬럼 앵커(x0) 추출.
    - 각 라인의 token x0를 수집해서 1D 클러스터링
    - '여러 라인에서 반복'되는 앵커만 남김
    """
    tol = max(10.0, page_w * tol_pct)
    xs: List[float] = []
    line_id: List[int] = []  # xs[i]가 어느 라인에서 왔는지

    for li, ln in enumerate(block_lines):
        for t in ln.tokens:
            xs.append(t.x0)
            line_id.append(li)

    clusters = _cluster_1d(xs, tol=tol)

    # support 계산: 같은 클러스터에 포함된 토큰들이 몇 개 라인에서 등장했는가
    anchors: List[Tuple[float, int]] = []
    min_support = max(2, int(len(block_lines) * min_support_ratio))

    for center, idxs in clusters:
        support_lines = len(set(line_id[i] for i in idxs))
        if support_lines >= min_support:
            anchors.append((center, support_lines))

    anchors_sorted = [a for a, _ in sorted(anchors, key=lambda x: x[0])]

    # 너무 촘촘하면(노이즈) 가까운 앵커 합치기(보수적으로)
    merged: List[float] = []
    for a in anchors_sorted:
        if not merged:
            merged.append(a)
            continue
        if abs(a - merged[-1]) <= tol * 0.6:
            merged[-1] = (merged[-1] + a) / 2.0
        else:
            merged.append(a)

    return merged


def _assign_token_to_col(x: float, anchors: List[float]) -> int:
    """
    token center x를 anchors 기준 구간에 배정.
    anchors=[a1<a2<...<ak], boundaries are midpoints.
    """
    if not anchors:
        return 0
    if len(anchors) == 1:
        return 0

    # 이진 탐색으로 x가 들어갈 구간 찾기
    # boundaries: b1=(a1+a2)/2, b2=(a2+a3)/2, ...
    lo, hi = 0, len(anchors) - 1
    # quick path
    if x < (anchors[0] + anchors[1]) / 2:
        return 0
    if x >= (anchors[-2] + anchors[-1]) / 2:
        return len(anchors) - 1

    # binary search boundaries
    left = 0
    right = len(anchors) - 2
    while left <= right:
        mid = (left + right) // 2
        b = (anchors[mid] + anchors[mid + 1]) / 2
        if x < b:
            right = mid - 1
        else:
            left = mid + 1
    # left is first boundary index where x < boundary => column = left
    return left


def _join_text(parts: List[str]) -> str:
    # OCR 토큰을 셀 텍스트로 합치기
    s = " ".join(p for p in parts if p and p.strip())
    s = re.sub(r"\s+", " ", s).strip()
    return s


def extract_tables_from_page(
    page_no: int,
    tokens: List[Token],
    *,
    min_table_lines: int,
    min_cols: int,
    big_gap_mult: float,
    anchor_tol_pct: float,
    anchor_min_support_ratio: float,
) -> List[ExtractedTable]:
    """
    한 페이지에서 '표'를 최대한 찾아 ExtractedTable 리스트 반환.
    """
    if not tokens:
        return []

    page_w = _page_width(tokens)
    lines = group_lines(tokens)
    # 해당 페이지만 필터
    lines = [ln for ln in lines if ln.page == page_no]
    if not lines:
        return []

    # line height 중앙값
    line_hs = sorted([ln.h for ln in lines if ln.h > 0])
    base_h = line_hs[len(line_hs) // 2] if line_hs else 12.0
    big_gap = max(20.0, base_h * big_gap_mult)

    # 1) table-ish 라인들을 연속 블록으로 묶기
    blocks: List[List[Line]] = []
    cur: List[Line] = []

    prev_y: Optional[float] = None
    for ln in lines:
        is_tableish = _line_is_tableish(ln, page_w=page_w)
        gap = (ln.y - prev_y) if (prev_y is not None) else 0.0
        prev_y = ln.y

        if not is_tableish:
            if cur:
                blocks.append(cur)
                cur = []
            continue

        if cur and gap > big_gap:
            blocks.append(cur)
            cur = []

        cur.append(ln)

    if cur:
        blocks.append(cur)

    tables: List[ExtractedTable] = []
    table_seq = 0

    # 2) 각 블록을 '테이블'로 재구성 시도
    for block in blocks:
        if len(block) < min_table_lines:
            continue

        anchors = _build_column_anchors(
            block, page_w=page_w, tol_pct=anchor_tol_pct, min_support_ratio=anchor_min_support_ratio
        )
        if len(anchors) < min_cols:
            continue

        # row matrix 만들기
        cols = len(anchors)
        rows: List[List[str]] = []
        cell_bboxes: Dict[Tuple[int, int], Tuple[float, float, float, float]] = {}

        block_x0 = float("inf")
        block_y0 = float("inf")
        block_x1 = 0.0
        block_y1 = 0.0

        for r_idx, ln in enumerate(block):
            # col->texts
            buckets: Dict[int, List[str]] = {i: [] for i in range(cols)}
            bbox_bucket: Dict[int, List[Tuple[float, float, float, float]]] = {i: [] for i in range(cols)}

            for t in ln.tokens:
                c = _assign_token_to_col(t.cx, anchors)
                if c < 0 or c >= cols:
                    continue
                buckets[c].append(t.text)
                bbox_bucket[c].append((t.x0, t.y0, t.x1, t.y1))

                block_x0 = min(block_x0, t.x0)
                block_y0 = min(block_y0, t.y0)
                block_x1 = max(block_x1, t.x1)
                block_y1 = max(block_y1, t.y1)

            row = [""] * cols
            for c in range(cols):
                txt = _join_text(buckets[c])
                row[c] = txt
                if bbox_bucket[c]:
                    xs0 = [b[0] for b in bbox_bucket[c]]
                    ys0 = [b[1] for b in bbox_bucket[c]]
                    xs1 = [b[2] for b in bbox_bucket[c]]
                    ys1 = [b[3] for b in bbox_bucket[c]]
                    cell_bboxes[(r_idx, c)] = (min(xs0), min(ys0), max(xs1), max(ys1))

            # 너무 빈 행은 제외(노이즈)
            if sum(1 for v in row if v) == 0:
                continue
            rows.append(row)

        if len(rows) < min_table_lines:
            continue

        # 3) continuation line 병합(첫 컬럼이 비고 다른 값이 있으면 이전 행에 이어붙이기)
        merged: List[List[str]] = []
        first_col = 0
        for row in rows:
            if merged and (not row[first_col]) and any(row):
                prev = merged[-1]
                for j in range(len(row)):
                    if row[j]:
                        if prev[j]:
                            prev[j] = prev[j] + "\n" + row[j]
                        else:
                            prev[j] = row[j]
            else:
                merged.append(row)

        # col 수 보정(너무 많은 노이즈 컬럼이 생기면 필터)
        # - 실제로 비어있는 컬럼이 거의 대부분이면 제거
        df = pd.DataFrame(merged)
        nonempty_ratio = (df != "").sum(axis=0) / max(1, len(df))
        keep_cols = [i for i, r in enumerate(nonempty_ratio.tolist()) if r >= 0.10]  # 10% 이상 채워진 컬럼만
        if len(keep_cols) >= min_cols:
            df = df.iloc[:, keep_cols]
            # cell_bboxes도 remap(간단히: 지금은 메타에만 쓰므로 생략 가능)
        else:
            # 최소 컬럼은 유지
            pass

        # table metadata
        table_seq += 1
        table_id = f"p{page_no}_t{table_seq}"
        sheet_name = f"p{page_no}_t{table_seq}"
        if len(sheet_name) > MAX_SHEETNAME_LEN:
            sheet_name = sheet_name[:MAX_SHEETNAME_LEN]

        tables.append(
            ExtractedTable(
                table_id=table_id,
                sheet_name=sheet_name,
                page_start=page_no,
                page_end=page_no,
                bbox=(block_x0 if block_x0 != float("inf") else 0.0, block_y0 if block_y0 != float("inf") else 0.0, block_x1, block_y1),
                n_rows=len(df),
                n_cols=df.shape[1],
                df=df,
                cell_bboxes=cell_bboxes,
            )
        )

    return tables


def merge_tables_across_pages(tables: List[ExtractedTable], *, col_similarity_tol: float = 0.12) -> List[ExtractedTable]:
    """
    페이지가 바뀌며 이어지는 표를 '가볍게' 병합.
    - 완전 일반해는 어렵고 오탐 위험도 있어서 보수적으로 적용.
    - 조건:
      1) 연속된 테이블이고 page가 바로 +1
      2) 컬럼 수가 같고
      3) 둘 다 row >= 2
    """
    if not tables:
        return []

    out: List[ExtractedTable] = []
    cur = tables[0]

    def can_merge(a: ExtractedTable, b: ExtractedTable) -> bool:
        if b.page_start != a.page_end + 1:
            return False
        if a.n_cols != b.n_cols:
            return False
        if a.n_rows < 2 or b.n_rows < 2:
            return False
        # 보수적: sheet 이름 패턴만으로도 어느 정도 이어짐 추정 가능
        # 여기선 추가 계산 없이 최소조건만 사용
        return True

    for nxt in tables[1:]:
        if can_merge(cur, nxt):
            merged_df = pd.concat([cur.df, nxt.df], ignore_index=True)
            cur = ExtractedTable(
                table_id=cur.table_id,
                sheet_name=cur.sheet_name,
                page_start=cur.page_start,
                page_end=nxt.page_end,
                bbox=(
                    min(cur.bbox[0], nxt.bbox[0]),
                    min(cur.bbox[1], nxt.bbox[1]),
                    max(cur.bbox[2], nxt.bbox[2]),
                    max(cur.bbox[3], nxt.bbox[3]),
                ),
                n_rows=len(merged_df),
                n_cols=cur.n_cols,
                df=merged_df,
                cell_bboxes=None,  # 병합시 cell bbox는 간단히 포기(원하면 확장 가능)
            )
        else:
            out.append(cur)
            cur = nxt

    out.append(cur)
    return out


# ============================================================
# 6) Excel 만들기
# ============================================================
def make_excel_bytes(tables: List[ExtractedTable], *, include_index: bool = True) -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        if include_index:
            idx_rows = []
            for t in tables:
                idx_rows.append(
                    {
                        "table_id": t.table_id,
                        "sheet_name": t.sheet_name,
                        "page_start": t.page_start,
                        "page_end": t.page_end,
                        "rows": t.n_rows,
                        "cols": t.n_cols,
                        "bbox_x0": round(t.bbox[0], 2),
                        "bbox_y0": round(t.bbox[1], 2),
                        "bbox_x1": round(t.bbox[2], 2),
                        "bbox_y1": round(t.bbox[3], 2),
                    }
                )
            df_index = pd.DataFrame(idx_rows)
            df_index.to_excel(writer, index=False, sheet_name="index")

        # 테이블 시트들
        used_names = set()
        for t in tables:
            name = t.sheet_name
            # 중복 방지
            if name in used_names:
                suffix = 2
                while f"{name}_{suffix}" in used_names:
                    suffix += 1
                name = f"{name}_{suffix}"
                name = name[:MAX_SHEETNAME_LEN]
            used_names.add(name)

            # header/index는 쓰지 않음(원본 표 느낌)
            t.df.to_excel(writer, index=False, header=False, sheet_name=name)

    return output.getvalue()


# ============================================================
# 7) Streamlit UI
# ============================================================
def main():
    st.set_page_config(page_title=APP_TITLE, page_icon="📄", layout="wide")
    st.title(APP_TITLE)
    st.caption(f"{APP_VERSION} | 네이버 일반 OCR + bbox 기반 테이블 복원 + JSON-LD(온톨로지)")

    st.markdown(
        """
이 앱은 **네이버 일반 OCR(표 OCR 아님)**의 bounding box 좌표를 이용해 PDF 내 표를 최대한 복원하여 **엑셀로 저장**합니다.

- ✅ 모든 페이지에 OCR 적용(텍스트 PDF 스킵 없음)
- ✅ 표 후보를 자동 탐지(연속 라인 + 컬럼 앵커 반복성)
- ✅ 표마다 시트 생성 + index 시트 제공
- ✅ (선택) JSON-LD(온톨로지 그래프)로도 저장 가능
"""
    )

    # 데모 비밀번호
    with st.expander("🔐 접근(데모용)", expanded=True):
        password = st.text_input("비밀번호를 입력하세요", type="password")
        if password != DEFAULT_PASSWORD:
            st.warning("비밀번호가 올바르지 않습니다.")
            st.stop()
        st.success("접속 완료")

    # 사이드바 설정
    with st.sidebar:
        st.header("⚙️ API 설정")
        try:
            api_url = st.secrets["NAVER_API_URL"]
            secret_key = st.secrets["NAVER_SECRET_KEY"]
            st.success("st.secrets에서 API 정보를 불러왔습니다.")
        except Exception:
            api_url = st.text_input("NAVER_API_URL")
            secret_key = st.text_input("NAVER_SECRET_KEY", type="password")

        st.divider()
        st.header("🧰 추출 옵션")

        pages_per_req = st.number_input("OCR 요청당 페이지 수", min_value=1, max_value=20, value=OCR_PAGES_PER_REQUEST, step=1)

        min_table_lines = st.number_input("표 최소 라인수", min_value=1, max_value=10, value=DEFAULT_MIN_TABLE_LINES, step=1)
        min_cols = st.number_input("표 최소 컬럼수", min_value=2, max_value=10, value=DEFAULT_MIN_COLS, step=1)

        big_gap_mult = st.slider("라인 블록 분리 민감도(y-gap)", min_value=1.5, max_value=6.0, value=DEFAULT_BIG_GAP_MULT, step=0.1)
        anchor_tol_pct = st.slider("컬럼 앵커 tolerance(% of page width)", min_value=0.005, max_value=0.04, value=DEFAULT_ANCHOR_TOL_PCT, step=0.001)
        anchor_support_ratio = st.slider("컬럼 앵커 최소 반복 비율", min_value=0.15, max_value=0.8, value=DEFAULT_ANCHOR_MIN_SUPPORT_RATIO, step=0.05)

        merge_pages = st.checkbox("페이지 넘어가는 표 병합(보수적)", value=True)

        st.divider()
        st.header("🧠 온톨로지(JSON-LD)")
        export_jsonld = st.checkbox("JSON-LD도 함께 생성", value=True)
        include_cells = st.checkbox("JSON-LD에 셀 단위까지 포함(파일 커짐)", value=False)

    uploaded_file = st.file_uploader("📎 PDF 파일 업로드", type=["pdf"])
    if uploaded_file is None:
        st.info("PDF를 업로드하면 추출을 시작할 수 있어요.")
        st.stop()

    file_bytes = uploaded_file.getvalue()
    file_hash = hashlib.sha256(file_bytes).hexdigest()

    # 파일이 바뀌면 결과 초기화
    if st.session_state.get("file_hash") != file_hash:
        for k in ["tables", "excel_bytes", "jsonld_bytes", "jsonld_obj"]:
            st.session_state.pop(k, None)
        st.session_state["file_hash"] = file_hash

    clicked = st.button("🔍 모든 표 추출 시작", disabled=not bool(api_url and secret_key))
    if clicked:
        if not api_url or not secret_key:
            st.error("API URL / Secret Key 확인 필요")
            st.stop()

        progress_bar = st.progress(0)
        status = st.empty()

        def progress_cb(i, total, start_p, end_p):
            pct = int(i / max(1, total) * 100)
            progress_bar.progress(min(100, pct))
            status.write(f"📄 OCR 진행: {i}/{total} (페이지 {start_p}~{end_p})")

        with st.spinner("OCR 및 표 추출 중..."):
            tokens_by_page = ocr_pdf_all_pages(
                file_bytes, api_url, secret_key, pages_per_request=int(pages_per_req), progress_cb=progress_cb
            )

            # 표 추출
            all_tables: List[ExtractedTable] = []
            for page_no in sorted(tokens_by_page.keys()):
                toks = tokens_by_page.get(page_no, [])
                tables = extract_tables_from_page(
                    page_no,
                    toks,
                    min_table_lines=int(min_table_lines),
                    min_cols=int(min_cols),
                    big_gap_mult=float(big_gap_mult),
                    anchor_tol_pct=float(anchor_tol_pct),
                    anchor_min_support_ratio=float(anchor_support_ratio),
                )
                all_tables.extend(tables)

            # 페이지 병합(선택)
            if merge_pages and all_tables:
                all_tables = merge_tables_across_pages(all_tables)

            # Excel 생성
            excel_bytes = make_excel_bytes(all_tables)

            # JSON-LD(온톨로지) 생성(선택)
            jsonld_obj = None
            jsonld_bytes = b""
            if export_jsonld:
                jsonld_obj = build_tables_jsonld(
                    file_name=uploaded_file.name,
                    file_hash=file_hash,
                    tables=all_tables,
                    include_cells=bool(include_cells),
                    base_iri="urn:dovi:",
                    generator_name="DOVI-TableExtractor",
                    config={
                        "pages_per_request": int(pages_per_req),
                        "min_table_lines": int(min_table_lines),
                        "min_cols": int(min_cols),
                        "big_gap_mult": float(big_gap_mult),
                        "anchor_tol_pct": float(anchor_tol_pct),
                        "anchor_support_ratio": float(anchor_support_ratio),
                        "merge_pages": bool(merge_pages),
                        "include_cells": bool(include_cells),
                    },
                )
                jsonld_bytes = make_jsonld_bytes(jsonld_obj)

            st.session_state["tables"] = all_tables
            st.session_state["excel_bytes"] = excel_bytes
            st.session_state["jsonld_obj"] = jsonld_obj
            st.session_state["jsonld_bytes"] = jsonld_bytes

        progress_bar.progress(100)
        status.write("✅ 완료")

    # 결과 표시
    if "tables" in st.session_state:
        tables: List[ExtractedTable] = st.session_state["tables"]
        excel_bytes: bytes = st.session_state.get("excel_bytes", b"")
        jsonld_bytes: bytes = st.session_state.get("jsonld_bytes", b"")

        st.divider()
        st.subheader(f"✅ 추출된 표 개수: {len(tables)}")

        col1, col2 = st.columns([1, 1])

        with col1:
            if excel_bytes:
                st.download_button(
                    "📥 Excel 다운로드 (표마다 시트)",
                    data=excel_bytes,
                    file_name=f"{uploaded_file.name}_tables.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )

            if export_jsonld and jsonld_bytes:
                st.download_button(
                    "🧠 JSON-LD 다운로드 (온톨로지 그래프)",
                    data=jsonld_bytes,
                    file_name=f"{uploaded_file.name}_tables.jsonld",
                    mime="application/ld+json",
                )

            # index 미리보기
            idx_rows = []
            for t in tables:
                idx_rows.append(
                    {
                        "table_id": t.table_id,
                        "sheet": t.sheet_name,
                        "page": f"{t.page_start}-{t.page_end}" if t.page_end != t.page_start else str(t.page_start),
                        "rows": t.n_rows,
                        "cols": t.n_cols,
                    }
                )
            df_index = pd.DataFrame(idx_rows)
            st.subheader("📌 표 인덱스")
            st.dataframe(df_index, use_container_width=True, hide_index=True)

        with col2:
            st.subheader("🔎 표 미리보기(선택)")
            if tables:
                options = [f"{t.sheet_name} (p{t.page_start}-{t.page_end}, {t.n_rows}x{t.n_cols})" for t in tables]
                sel = st.selectbox("표 선택", options, index=0)
                sel_idx = options.index(sel)
                st.dataframe(tables[sel_idx].df, use_container_width=True, hide_index=True)

            if export_jsonld:
                with st.expander("🧠 JSON-LD 미리보기", expanded=False):
                    st.json(st.session_state.get("jsonld_obj", {}))

    else:
        st.info("추출을 실행하면 결과가 여기에 표시됩니다.")


if __name__ == "__main__":
    main()

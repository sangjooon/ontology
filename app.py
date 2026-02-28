# app.py
# 문서팩 PDF(토지이용계획/등기/대장/건물) → OCR → 페이지분류/세그먼트 → 지번/주소 번들링 → 등기부(갑/을구) 테이블 → 엑셀

import io
import os
import re
import shutil
from typing import Optional, List, Tuple, Dict, Set

import fitz  # PyMuPDF
import pandas as pd
import streamlit as st
from PIL import Image

# OCR (optional)
try:
    import pytesseract  # type: ignore
    HAS_PYTESSERACT = True
except Exception:
    HAS_PYTESSERACT = False


# -----------------------------
# Utility helpers
# -----------------------------
def normalize_ws(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def unify_hyphens(s: str) -> str:
    return (s or "").replace("‐", "-").replace("‑", "-").replace("–", "-").replace("—", "-")


def parse_page_ranges(spec: str, max_page: int) -> Set[int]:
    """'1-3,5,8-10' -> set of pages. Empty -> all pages"""
    spec = (spec or "").strip()
    if not spec:
        return set(range(1, max_page + 1))

    pages: Set[int] = set()
    for part in [p.strip() for p in spec.split(",") if p.strip()]:
        if "-" in part:
            a, b = part.split("-", 1)
            a = a.strip()
            b = b.strip()
            if a.isdigit() and b.isdigit():
                start = max(1, int(a))
                end = min(max_page, int(b))
                pages.update(range(start, end + 1))
        else:
            if part.isdigit():
                p = int(part)
                if 1 <= p <= max_page:
                    pages.add(p)
    return pages


def configure_tesseract_cmd(tesseract_cmd: str) -> None:
    if not HAS_PYTESSERACT:
        return
    cmd = (tesseract_cmd or "").strip()
    if cmd:
        pytesseract.pytesseract.tesseract_cmd = cmd  # type: ignore


def tesseract_available(tesseract_cmd: str = "") -> bool:
    if not HAS_PYTESSERACT:
        return False
    cmd = (tesseract_cmd or "").strip()
    if cmd and os.path.exists(cmd):
        return True
    return shutil.which("tesseract") is not None


# -----------------------------
# PDF helpers
# -----------------------------
@st.cache_data(show_spinner=False)
def get_pdf_page_count(pdf_bytes: bytes) -> int:
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    return doc.page_count


@st.cache_data(show_spinner=False)
def get_page_size(pdf_bytes: bytes, page_no: int) -> Tuple[float, float]:
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    page = doc.load_page(page_no - 1)
    r = page.rect
    return float(r.width), float(r.height)


@st.cache_data(show_spinner=False)
def render_page_png_bytes(pdf_bytes: bytes, page_no: int, zoom: float) -> bytes:
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    page = doc.load_page(page_no - 1)
    mat = fitz.Matrix(zoom, zoom)
    pix = page.get_pixmap(matrix=mat)
    return pix.tobytes("png")


def png_bytes_to_pil(png_bytes: bytes) -> Image.Image:
    return Image.open(io.BytesIO(png_bytes)).convert("RGB")


# -----------------------------
# Token extraction
# -----------------------------
@st.cache_data(show_spinner=False)
def extract_pdf_text_tokens(pdf_bytes: bytes) -> pd.DataFrame:
    """
    Text-based PDF tokens (span-level) with bbox.
    (스캔 PDF는 거의 비어있을 수 있음)
    """
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    rows = []
    for i in range(doc.page_count):
        page_no = i + 1
        page = doc.load_page(i)
        d = page.get_text("dict")
        for b in d.get("blocks", []):
            for line in b.get("lines", []):
                for span in line.get("spans", []):
                    text = (span.get("text") or "").strip()
                    if not text:
                        continue
                    bbox = span.get("bbox")
                    if not bbox or len(bbox) != 4:
                        continue
                    x0, y0, x1, y1 = bbox
                    rows.append(
                        {
                            "page": page_no,
                            "text": text,
                            "x0": float(x0),
                            "y0": float(y0),
                            "x1": float(x1),
                            "y1": float(y1),
                            "conf": None,
                            "source": "pdf_text",
                        }
                    )
    return pd.DataFrame(rows, columns=["page", "text", "x0", "y0", "x1", "y1", "conf", "source"])


@st.cache_data(show_spinner=False)
def ocr_page_tesseract_word_tokens(
    png_bytes: bytes,
    page_no: int,
    zoom: float,
    lang: str,
    psm: int,
    tesseract_cmd: str,
) -> pd.DataFrame:
    """
    OCR word tokens using Tesseract image_to_data.
    bbox: pixel -> PDF coords (divide by zoom).
    """
    if not HAS_PYTESSERACT:
        raise RuntimeError("pytesseract is not installed.")
    configure_tesseract_cmd(tesseract_cmd)
    if not tesseract_available(tesseract_cmd):
        raise RuntimeError("tesseract binary not found (PATH or tesseract_cmd).")

    img = png_bytes_to_pil(png_bytes).convert("L")
    config = f"--psm {int(psm)}"
    data = pytesseract.image_to_data(  # type: ignore
        img,
        lang=lang,
        output_type=pytesseract.Output.DICT,  # type: ignore
        config=config,
    )

    rows = []
    n = len(data.get("text", []))
    for i in range(n):
        text = (data["text"][i] or "").strip()
        if not text:
            continue
        # confidence
        conf_val: Optional[float]
        try:
            c = float(data["conf"][i])
            if c < 0:
                continue
            conf_val = c / 100.0
        except Exception:
            conf_val = None

        left = float(data["left"][i])
        top = float(data["top"][i])
        width = float(data["width"][i])
        height = float(data["height"][i])

        x0 = left / zoom
        y0 = top / zoom
        x1 = (left + width) / zoom
        y1 = (top + height) / zoom

        rows.append(
            {
                "page": int(page_no),
                "text": text,
                "x0": float(x0),
                "y0": float(y0),
                "x1": float(x1),
                "y1": float(y1),
                "conf": conf_val,
                "source": "ocr",
            }
        )

    return pd.DataFrame(rows, columns=["page", "text", "x0", "y0", "x1", "y1", "conf", "source"])


def make_page_stats(page_count: int, df_pdf_text: pd.DataFrame) -> pd.DataFrame:
    pages = pd.DataFrame({"page": list(range(1, page_count + 1))})
    if df_pdf_text.empty:
        pages["pdf_text_tokens"] = 0
        return pages
    c = df_pdf_text.groupby("page").size().rename("pdf_text_tokens").reset_index()
    pages = pages.merge(c, on="page", how="left").fillna({"pdf_text_tokens": 0})
    pages["pdf_text_tokens"] = pages["pdf_text_tokens"].astype(int)
    return pages


# -----------------------------
# Tokens -> lines (and keep line_id per token)
# -----------------------------
def add_line_id(df_tokens: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Add line_id to df_tokens using y clustering per page.
    Return (df_tokens_with_line_id, df_lines).
    """
    if df_tokens.empty:
        df_tokens2 = df_tokens.copy()
        df_tokens2["line_id"] = pd.Series(dtype="int64")
        df_lines = pd.DataFrame(columns=["page", "line_id", "y0", "y1", "text"])
        return df_tokens2, df_lines

    df_tokens2 = df_tokens.copy()
    df_tokens2["cy"] = (df_tokens2["y0"] + df_tokens2["y1"]) / 2.0
    df_tokens2["h"] = (df_tokens2["y1"] - df_tokens2["y0"]).clip(lower=1.0)

    line_rows = []
    out_frames = []

    for page, g in df_tokens2.groupby("page"):
        g = g.copy()
        g = g.sort_values(["cy", "x0"]).reset_index(drop=True)

        med_h = float(g["h"].median()) if len(g) else 10.0
        y_tol = med_h * 0.8

        line_id = 0
        current_idx = []
        current_cy: Optional[float] = None

        def flush():
            nonlocal line_id, current_idx
            if not current_idx:
                return
            gg = g.loc[current_idx].sort_values("x0")
            text = normalize_ws(" ".join(gg["text"].astype(str).tolist()))
            y0 = float(gg["y0"].min())
            y1 = float(gg["y1"].max())
            line_rows.append({"page": int(page), "line_id": int(line_id), "y0": y0, "y1": y1, "text": text})
            g.loc[current_idx, "line_id"] = int(line_id)
            line_id += 1
            current_idx = []

        for idx, r in g.iterrows():
            cy = float(r["cy"])
            if current_cy is None:
                current_idx = [idx]
                current_cy = cy
                continue

            if abs(cy - current_cy) <= y_tol:
                current_idx.append(idx)
                current_cy = (current_cy * (len(current_idx) - 1) + cy) / len(current_idx)
            else:
                flush()
                current_idx = [idx]
                current_cy = cy

        flush()
        out_frames.append(g)

    df_tokens_out = pd.concat(out_frames, ignore_index=True)
    df_tokens_out = df_tokens_out.drop(columns=["cy", "h"], errors="ignore")
    df_tokens_out["line_id"] = df_tokens_out["line_id"].astype(int)
    df_tokens_out = df_tokens_out.sort_values(["page", "line_id", "x0"]).reset_index(drop=True)

    df_lines = (
        pd.DataFrame(line_rows, columns=["page", "line_id", "y0", "y1", "text"])
        .sort_values(["page", "y0"])
        .reset_index(drop=True)
    )
    return df_tokens_out, df_lines


def build_page_text_preview(df_lines: pd.DataFrame, page: int, max_lines: int = 40) -> str:
    g = df_lines[df_lines["page"] == int(page)].sort_values("y0").head(int(max_lines))
    return "\n".join(g["text"].astype(str).tolist())


# -----------------------------
# Page classification
# -----------------------------
DOC_LABEL = {
    "land_use_plan": "토지이용계획확인서",
    "cadastral_map": "지적도 등본",
    "land_registry": "등기부(토지)",
    "building_registry": "등기부(건물)",
    "registry_summary": "주요 등기사항 요약",
    "land_ledger": "토지대장",
    "coowner_list": "공유지 연명부",
    "building_ledger": "일반건축물대장(갑)",
    "building_master": "건축물대장 총괄표제부(갑)",
    "unknown": "미분류",
}

DOC_PATTERNS: List[Tuple[str, List[str]]] = [
    ("land_use_plan", [r"토지\s*이용\s*계획\s*확인\s*서"]),
    ("cadastral_map", [r"지\s*적\s*도\s*등\s*본"]),
    ("registry_summary", [r"주요\s*등기\s*사항\s*요약"]),
    ("land_registry", [r"등기\s*사항\s*전부\s*증명서.*토\s*지", r"\-\s*토\s*지\s*\[제출용\]"]),
    ("building_registry", [r"등기\s*사항\s*전부\s*증명서.*건\s*물", r"\-\s*건\s*물\s*\[제출용\]"]),
    ("land_ledger", [r"토지\s*대\s*장"]),
    ("coowner_list", [r"공유지\s*연명부"]),
    ("building_master", [r"건축물\s*대장\s*총괄\s*표제부"]),
    ("building_ledger", [r"일반\s*건축물\s*대장"]),
]


def classify_text(text: str) -> str:
    t = normalize_ws(text)
    if not t:
        return "unknown"
    for dt, patterns in DOC_PATTERNS:
        for pat in patterns:
            if re.search(pat, t, flags=re.IGNORECASE):
                return dt
    return "unknown"


def compute_segments(page_map: pd.DataFrame) -> pd.DataFrame:
    """Consecutive pages with same doc_type -> one segment."""
    if page_map.empty:
        return pd.DataFrame(columns=["segment_id", "doc_type", "doc_label", "start_page", "end_page", "pages"])

    rows = []
    seg_id = 1
    cur_type = None
    cur_pages: List[int] = []

    def flush():
        nonlocal seg_id, cur_type, cur_pages
        if cur_type is None or not cur_pages:
            return
        rows.append(
            {
                "segment_id": seg_id,
                "doc_type": cur_type,
                "doc_label": DOC_LABEL.get(cur_type, cur_type),
                "start_page": cur_pages[0],
                "end_page": cur_pages[-1],
                "pages": ",".join(map(str, cur_pages)),
            }
        )
        seg_id += 1
        cur_type = None
        cur_pages = []

    for _, r in page_map.sort_values("page").iterrows():
        p = int(r["page"])
        dt = str(r["doc_type"])
        if cur_type is None:
            cur_type = dt
            cur_pages = [p]
        else:
            if dt == cur_type and p == cur_pages[-1] + 1:
                cur_pages.append(p)
            else:
                flush()
                cur_type = dt
                cur_pages = [p]
    flush()
    return pd.DataFrame(rows, columns=["segment_id", "doc_type", "doc_label", "start_page", "end_page", "pages"])


# -----------------------------
# Subject key extraction (bundle grouping)
# -----------------------------
def extract_best_hyphen_number(text: str) -> Optional[str]:
    """
    Find best candidate like 496-10 for '지번', avoiding phone/road numbers.
    Simple scoring heuristic.
    """
    t = normalize_ws(unify_hyphens(text))
    if not t:
        return None

    candidates = list(re.finditer(r"\b\d{1,5}\s*-\s*\d{1,5}\b", t))
    if not candidates:
        return None

    best_val = None
    best_score = -1e9

    for m in candidates:
        cand = re.sub(r"\s+", "", m.group(0))
        start = m.start()
        before = t[max(0, start - 40):start]
        score = 0

        if re.search(r"지\s*번", before):
            score += 6
        if re.search(r"번\s*지", before):
            score += 4

        if re.search(r"(TEL|전화|전\s*화)", before, flags=re.IGNORECASE):
            score -= 8

        if re.search(r"(로|길)\s*$", before[-6:]):
            score -= 6

        if re.search(r"\(\s*$", before[-3:]):
            score -= 2

        try:
            a, b = cand.split("-")
            a_i = int(a)
            b_i = int(b)
            if 1 <= a_i <= 9999 and 0 <= b_i <= 9999:
                score += 1
            if a_i >= 300:
                score += 1
        except Exception:
            pass

        if score > best_score:
            best_score = score
            best_val = cand

    return best_val


def extract_location(text: str) -> Optional[str]:
    """Extract 소재지/대지위치 같은 위치 문자열(best-effort)."""
    t = normalize_ws(text)
    if not t:
        return None

    m = re.search(r"소재지\s*[:：]?\s*(.+?)\s*(?=지\s*번|지번|지\s*목|지목|면\s*적|면적)", t)
    if m:
        return normalize_ws(m.group(1))

    m = re.search(r"대지\s*위치\s*[:：]?\s*(.+?)\s*(?=지\s*번|지번|도로명\s*주소|도로명주소)", t)
    if m:
        return normalize_ws(m.group(1))

    return None


def extract_road_address(text: str) -> Optional[str]:
    t = normalize_ws(text)
    if not t:
        return None
    m = re.search(r"도로명\s*주소\s*[:：]?\s*(.+?)\s*(?=지\s*번|지번|대지\s*면적|대지면적|연면적|건축면적|$)", t)
    if m:
        return normalize_ws(m.group(1))
    return None


def segment_text_from_pages(lines_df: pd.DataFrame, pages: List[int], max_lines_per_page: int = 80) -> str:
    texts = []
    for p in pages:
        g = lines_df[lines_df["page"] == int(p)].sort_values("y0").head(int(max_lines_per_page))
        texts.extend(g["text"].astype(str).tolist())
    return normalize_ws(" ".join(texts))


def assign_bundles_by_subject(segments: pd.DataFrame, seg_meta: pd.DataFrame) -> pd.DataFrame:
    """
    Create bundle_id by subject_key (location|jibeon).
    """
    merged = segments.merge(seg_meta, on="segment_id", how="left")

    def make_key(r):
        loc = r.get("location")
        j = r.get("jibeon")
        loc = normalize_ws(str(loc)) if loc and str(loc) != "nan" else ""
        j = normalize_ws(str(j)) if j and str(j) != "nan" else ""
        if loc and j:
            return f"{loc}|{j}"
        if j:
            return j
        if loc:
            return loc
        return "UNKNOWN"

    merged["subject_key"] = merged.apply(make_key, axis=1)

    key_to_bundle: Dict[str, int] = {}
    bundle_ids = []
    next_id = 1
    for k in merged["subject_key"].tolist():
        if k not in key_to_bundle:
            key_to_bundle[k] = next_id
            next_id += 1
        bundle_ids.append(key_to_bundle[k])

    merged["bundle_id"] = bundle_ids
    return merged


# -----------------------------
# Registry table parsing (A/B)
# -----------------------------
def normalize_rank(s: str) -> Optional[str]:
    s2 = unify_hyphens(str(s or "")).strip()
    s2 = re.sub(r"[^0-9\-]", "", s2)
    if re.match(r"^\d+(-\d+)?$", s2):
        return s2
    return None


def is_registry_header_line(line_text: str) -> bool:
    t = normalize_ws(line_text)
    return ("등기목적" in t) or ("접수" in t and "권리자" in t) or ("순위" in t and "등기" in t)


def detect_registry_section(line_text: str) -> Optional[str]:
    t = normalize_ws(line_text)
    if re.search(r"\[\s*갑\s*구\s*\]|갑\s*구|갑구", t):
        return "A"
    if re.search(r"\[\s*을\s*구\s*\]|을\s*구|을구", t):
        return "B"
    return None


def is_land_list_heading(line_text: str) -> bool:
    t = normalize_ws(line_text)
    return ("대지목록" in t) or re.search(r"\[\s*대\s*지\s*목\s*록\s*\]", t) is not None


def assign_registry_col(x0: float, page_width: float, fracs: Tuple[float, float, float, float]) -> int:
    b1, b2, b3, b4 = fracs
    bounds = [b1 * page_width, b2 * page_width, b3 * page_width, b4 * page_width]
    if x0 < bounds[0]:
        return 1
    if x0 < bounds[1]:
        return 2
    if x0 < bounds[2]:
        return 3
    if x0 < bounds[3]:
        return 4
    return 5


def parse_registry_entries(
    df_tokens: pd.DataFrame,
    df_lines: pd.DataFrame,
    pages: List[int],
    page_widths: Dict[int, float],
    doc_type: str,
    col_fracs: Tuple[float, float, float, float],
) -> pd.DataFrame:
    """
    Parse registry A/B entries using heuristic column boundaries + line-based row starts.
    """
    entries = []
    current = None
    current_section = None

    def flush():
        nonlocal current
        if not current:
            return
        for k in ["purpose", "receipt", "cause", "party", "raw_text"]:
            current[k] = normalize_ws(current.get(k, ""))
        entries.append(current)
        current = None

    for p in pages:
        p = int(p)
        w = page_widths.get(p, 595.0)

        lines_p = df_lines[df_lines["page"] == p].sort_values("y0")
        for _, lr in lines_p.iterrows():
            line_id = int(lr["line_id"])
            line_text = str(lr["text"])

            sec = detect_registry_section(line_text)
            if sec:
                current_section = sec
                continue

            if is_land_list_heading(line_text):
                current_section = None
                flush()
                continue

            if is_registry_header_line(line_text):
                continue

            if current_section not in ("A", "B"):
                continue

            toks = df_tokens[(df_tokens["page"] == p) & (df_tokens["line_id"] == line_id)].copy()
            if toks.empty:
                continue
            toks = toks.sort_values("x0")
            toks["col"] = toks["x0"].apply(lambda x: assign_registry_col(float(x), w, col_fracs))

            col_texts = {}
            for col in [1, 2, 3, 4, 5]:
                gt = toks[toks["col"] == col].sort_values("x0")
                col_texts[col] = normalize_ws(" ".join(gt["text"].astype(str).tolist()))

            if all(not col_texts[c] for c in col_texts):
                continue

            rank = normalize_rank(col_texts[1])

            if rank:
                flush()
                current = {
                    "doc_type": doc_type,
                    "page": p,
                    "section": current_section,
                    "rank_no": rank,
                    "purpose": col_texts[2],
                    "receipt": col_texts[3],
                    "cause": col_texts[4],
                    "party": col_texts[5],
                    "raw_text": normalize_ws(" | ".join([col_texts[c] for c in [1, 2, 3, 4, 5] if col_texts[c]])),
                }
            else:
                if current is None:
                    continue
                if col_texts[2]:
                    current["purpose"] += " " + col_texts[2]
                if col_texts[3]:
                    current["receipt"] += " " + col_texts[3]
                if col_texts[4]:
                    current["cause"] += " " + col_texts[4]
                if col_texts[5]:
                    current["party"] += " " + col_texts[5]
                current["raw_text"] += " || " + normalize_ws(" | ".join([col_texts[c] for c in [1,2,3,4,5] if col_texts[c]]))

    flush()
    return pd.DataFrame(entries, columns=["doc_type", "page", "section", "rank_no", "purpose", "receipt", "cause", "party", "raw_text"])


# -----------------------------
# Segment meta extractors
# -----------------------------
def extract_field(text: str, label_regex: str, max_after: int = 80) -> Optional[str]:
    t = normalize_ws(text)
    if not t:
        return None
    pat = (
        label_regex
        + r"\s*[:：]?\s*(.{0,"
        + str(max_after)
        + r"}?)\s*(?=(지\s*번|지번|지\s*목|지목|면\s*적|면적|도로명\s*주소|도로명주소|$))"
    )
    m = re.search(pat, t)
    if m:
        return normalize_ws(m.group(1))
    return None


def extract_area_value(text: str) -> Optional[str]:
    t = normalize_ws(text)
    if not t:
        return None
    m = re.search(r"면\s*적[^0-9]{0,20}([0-9][0-9,\.]*)\s*㎡", t)
    if m:
        return m.group(1)
    m = re.search(r"면\s*적[^0-9]{0,20}([0-9][0-9,\.]*)", t)
    if m:
        return m.group(1)
    return None


def extract_doc_core_fields(doc_type: str, segment_text: str) -> Dict[str, Optional[str]]:
    t = segment_text
    fields: Dict[str, Optional[str]] = {}
    fields["jibeon"] = extract_best_hyphen_number(t)
    fields["location"] = extract_location(t)
    fields["road_address"] = extract_road_address(t)

    if doc_type in ("land_use_plan", "land_registry", "land_ledger"):
        fields["jimok"] = extract_field(t, r"지\s*목")
        fields["area_sqm"] = extract_area_value(t)
        fields["sojaeji"] = extract_field(t, r"소재지")
    elif doc_type in ("building_ledger", "building_master", "building_registry"):
        fields["building_id"] = extract_field(t, r"건물ID|건물\s*ID", max_after=40)
        fields["unique_no"] = extract_field(t, r"고유번호", max_after=60)
        fields["name"] = extract_field(t, r"명칭", max_after=40)
        fields["total_floor_area"] = extract_field(t, r"연면적", max_after=40)
        fields["building_area"] = extract_field(t, r"건축면적", max_after=40)
        fields["main_use"] = extract_field(t, r"주용도", max_after=60)
        fields["main_structure"] = extract_field(t, r"주구조", max_after=60)

    return fields


# -----------------------------
# Excel export
# -----------------------------
def build_excel_bytes(sheets: Dict[str, pd.DataFrame]) -> bytes:
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as writer:
        for name, df in sheets.items():
            safe_name = name[:31]
            df.to_excel(writer, index=False, sheet_name=safe_name)
    return out.getvalue()


def main():
    st.set_page_config(page_title="문서팩 OCR → 분류/번들/등기 테이블(MVP)", layout="wide")
    st.title("문서팩 PDF → OCR → 분류/번들 → 등기부(갑/을구) 테이블 → 엑셀")

    st.caption(
        "1) OCR로 tokens(text+bbox) 생성 → 2) 페이지별 문서유형 분류/세그먼트 → "
        "3) 지번/주소(subject key)로 번들링 → 4) 등기부(토지/건물) 갑구/을구 표를 테이블로 추출"
    )

    uploaded = st.file_uploader("PDF 업로드", type=["pdf"])
    if not uploaded:
        st.stop()

    pdf_bytes = uploaded.getvalue()
    page_count = get_pdf_page_count(pdf_bytes)
    st.success(f"업로드 완료: {uploaded.name} (pages={page_count}, size={len(pdf_bytes):,} bytes)")

    # Sidebar
    with st.sidebar:
        st.header("OCR 설정")

        zoom = st.slider("렌더링/OCR 해상도(zoom)", 1.0, 4.0, 2.0, 0.25)

        ocr_mode = st.radio(
            "OCR 모드",
            options=["전체 페이지 OCR", "자동(텍스트 없는 페이지만)", "OCR 안함(테스트용)"],
            index=0,
        )
        token_threshold = st.slider("자동 OCR 기준: pdf_text 토큰 수 < N", 0, 500, 10, 5)

        st.markdown("---")
        st.subheader("Tesseract")
        tesseract_cmd = st.text_input("(선택) Windows tesseract.exe 경로", value="")
        ocr_lang = st.text_input("언어", value="kor+eng")
        ocr_psm = st.selectbox("PSM", options=[3, 4, 6, 11, 12, 13], index=2)

        st.markdown("---")
        st.subheader("실행 범위")
        page_spec = st.text_input("처리할 페이지(예: 1-5,10-12) / 비우면 전체", value="")

        st.markdown("---")
        st.subheader("등기부 표 파싱(갑/을구)")
        parse_registry = st.checkbox("등기부(토지/건물) 갑/을구 테이블 추출", value=True)

        st.caption("등기부 표 컬럼 경계(페이지 너비 비율). OCR이 엇갈리면 조정하세요.")
        b1 = st.slider("col1 끝(순위번호)", 0.05, 0.25, 0.12, 0.01)
        b2 = st.slider("col2 끝(등기목적)", 0.20, 0.50, 0.32, 0.01)
        b3 = st.slider("col3 끝(접수)", 0.35, 0.65, 0.47, 0.01)
        b4 = st.slider("col4 끝(등기원인)", 0.45, 0.80, 0.62, 0.01)
        col_fracs = (float(b1), float(b2), float(b3), float(b4))

        st.markdown("---")
        include_raw = st.checkbox("엑셀에 RAW(tokens/lines)도 포함", value=False)

        st.markdown("---")
        if ocr_mode != "OCR 안함(테스트용)":
            if tesseract_available(tesseract_cmd):
                st.success("Tesseract 사용 가능")
            else:
                st.error("Tesseract 사용 불가: packages.txt/설치/PATH 확인 필요")

    # Preview
    left, right = st.columns([1, 1])
    with left:
        st.subheader("페이지 미리보기")
        page_no = st.number_input("미리보기 페이지", 1, page_count, 1, 1)
        png = render_page_png_bytes(pdf_bytes, int(page_no), float(zoom))
        st.image(png, caption=f"Page {page_no}", use_container_width=True)

    # Extract pdf_text tokens
    with st.spinner("PDF 텍스트 토큰 추출(PyMuPDF)..."):
        df_pdf_text = extract_pdf_text_tokens(pdf_bytes)

    page_stats = make_page_stats(page_count, df_pdf_text)

    with right:
        st.subheader("페이지별 pdf_text 토큰 수")
        st.dataframe(page_stats, use_container_width=True, height=350)

    st.markdown("---")
    if not st.button("실행"):
        st.stop()

    selected_pages = sorted(list(parse_page_ranges(page_spec, page_count)))

    # Determine pages to OCR
    if ocr_mode == "OCR 안함(테스트용)":
        pages_to_ocr: List[int] = []
    elif ocr_mode == "전체 페이지 OCR":
        pages_to_ocr = selected_pages
    else:
        auto_pages = page_stats.loc[page_stats["pdf_text_tokens"] < token_threshold, "page"].astype(int).tolist()
        pages_to_ocr = sorted(list(set(auto_pages).intersection(selected_pages)))

    # OCR
    ocr_frames: List[pd.DataFrame] = []
    if pages_to_ocr:
        if not tesseract_available(tesseract_cmd):
            st.error(
                "OCR 수행이 필요하지만 Tesseract가 사용 불가입니다.\n\n"
                "Streamlit Cloud라면:\n"
                "- requirements.txt: pytesseract\n"
                "- packages.txt: tesseract-ocr, tesseract-ocr-kor, tesseract-ocr-eng\n"
                "추가 후 재배포하세요."
            )
            st.stop()

        st.info(f"OCR 대상: {len(pages_to_ocr)} 페이지 / 선택 범위 {len(selected_pages)} 페이지")
        prog = st.progress(0)
        status = st.empty()

        for idx, p in enumerate(pages_to_ocr, start=1):
            status.write(f"OCR 중... {idx}/{len(pages_to_ocr)} (page {p})")
            png_p = render_page_png_bytes(pdf_bytes, int(p), float(zoom))
            try:
                df_ocr = ocr_page_tesseract_word_tokens(
                    png_bytes=png_p,
                    page_no=int(p),
                    zoom=float(zoom),
                    lang=str(ocr_lang),
                    psm=int(ocr_psm),
                    tesseract_cmd=str(tesseract_cmd),
                )
                ocr_frames.append(df_ocr)
            except Exception as e:
                st.warning(f"page {p} OCR 실패: {e}")

            prog.progress(int(idx / len(pages_to_ocr) * 100))
        status.write("OCR 완료")

    # Combine tokens
    if (not df_pdf_text.empty) or ocr_frames:
        df_tokens = pd.concat([df_pdf_text] + ocr_frames, ignore_index=True)
    else:
        df_tokens = pd.DataFrame(columns=["page", "text", "x0", "y0", "x1", "y1", "conf", "source"])

    if not df_tokens.empty:
        df_tokens["text"] = df_tokens["text"].astype(str).map(lambda s: s.strip())
        df_tokens = df_tokens[df_tokens["text"] != ""].copy()
        df_tokens = df_tokens[df_tokens["page"].isin(selected_pages)].copy()
        df_tokens = df_tokens.sort_values(["page", "y0", "x0"]).reset_index(drop=True)

    # Add line_id and build lines
    with st.spinner("토큰 → 라인(행) 구성..."):
        df_tokens2, df_lines = add_line_id(df_tokens)

    # Page map
    with st.spinner("페이지 분류..."):
        page_rows = []
        for p in selected_pages:
            preview = build_page_text_preview(df_lines, p, max_lines=45)
            dt = classify_text(preview)
            page_rows.append({"page": p, "doc_type": dt, "doc_label": DOC_LABEL.get(dt, dt)})

        page_map = pd.DataFrame(page_rows)

        tot = df_tokens2.groupby("page").size().rename("total_tokens").reset_index()
        ocr = df_tokens2[df_tokens2["source"] == "ocr"].groupby("page").size().rename("ocr_tokens").reset_index()

        page_map = page_map.merge(page_stats, on="page", how="left").merge(tot, on="page", how="left").merge(ocr, on="page", how="left")
        page_map = page_map.fillna({"pdf_text_tokens": 0, "total_tokens": 0, "ocr_tokens": 0})
        page_map["pdf_text_tokens"] = page_map["pdf_text_tokens"].astype(int)
        page_map["total_tokens"] = page_map["total_tokens"].astype(int)
        page_map["ocr_tokens"] = page_map["ocr_tokens"].astype(int)
        page_map["ocr_used"] = page_map["ocr_tokens"] > 0

    segments = compute_segments(page_map)

    # Segment meta
    with st.spinner("세그먼트 메타(지번/주소) 추출..."):
        seg_meta_rows = []
        for _, sr in segments.iterrows():
            seg_id = int(sr["segment_id"])
            pages = [int(x) for x in str(sr["pages"]).split(",") if x.strip().isdigit()]
            seg_text = segment_text_from_pages(df_lines, pages, max_lines_per_page=80)
            dt = str(sr["doc_type"])
            core = extract_doc_core_fields(dt, seg_text)
            seg_meta_rows.append({"segment_id": seg_id, "segment_text_sample": seg_text[:200], **core})
        seg_meta = pd.DataFrame(seg_meta_rows)

    seg_bundle = assign_bundles_by_subject(segments, seg_meta)

    bundles = (
        seg_bundle.groupby(["bundle_id", "subject_key"])
        .agg(
            n_segments=("segment_id", "count"),
            segments=("segment_id", lambda x: ",".join(map(str, x.tolist()))),
            doc_types=("doc_type", lambda x: ",".join(x.tolist())),
            pages=("pages", lambda x: ";".join(x.tolist())),
        )
        .reset_index()
        .sort_values("bundle_id")
    )

    st.subheader("page_map (페이지별 문서유형)")
    st.dataframe(page_map, use_container_width=True, height=260)

    st.subheader("segments (연속 페이지 문서 묶음)")
    st.dataframe(segments, use_container_width=True, height=220)

    st.subheader("segments + bundle (지번/주소 기반 번들)")
    st.dataframe(
        seg_bundle[["bundle_id", "segment_id", "doc_type", "start_page", "end_page", "pages", "subject_key", "jibeon", "location", "road_address"]],
        use_container_width=True,
        height=320,
    )

    st.subheader("bundles")
    st.dataframe(bundles, use_container_width=True, height=220)

    # Registry parse
    registry_entries = pd.DataFrame()
    if parse_registry:
        with st.spinner("등기부(토지/건물) 갑/을구 테이블 추출 중..."):
            page_widths = {p: get_page_size(pdf_bytes, p)[0] for p in selected_pages}

            all_entries = []
            for _, r in seg_bundle.iterrows():
                dt = str(r["doc_type"])
                if dt not in ("land_registry", "building_registry"):
                    continue
                seg_id = int(r["segment_id"])
                bundle_id = int(r["bundle_id"])
                pages = [int(x) for x in str(r["pages"]).split(",") if x.strip().isdigit()]

                toks = df_tokens2[df_tokens2["page"].isin(pages)].copy()
                lns = df_lines[df_lines["page"].isin(pages)].copy()

                df_ent = parse_registry_entries(
                    df_tokens=toks,
                    df_lines=lns,
                    pages=pages,
                    page_widths=page_widths,
                    doc_type=dt,
                    col_fracs=col_fracs,
                )
                if not df_ent.empty:
                    df_ent.insert(0, "bundle_id", bundle_id)
                    df_ent.insert(1, "segment_id", seg_id)
                    all_entries.append(df_ent)

            if all_entries:
                registry_entries = pd.concat(all_entries, ignore_index=True)
            else:
                registry_entries = pd.DataFrame(
                    columns=["bundle_id", "segment_id", "doc_type", "page", "section", "rank_no", "purpose", "receipt", "cause", "party", "raw_text"]
                )

    # Excel
    sheets: Dict[str, pd.DataFrame] = {
        "page_map": page_map,
        "segments": segments,
        "segment_meta": seg_meta,
        "segment_bundle": seg_bundle,
        "bundles": bundles,
    }
    if parse_registry:
        sheets["registry_entries"] = registry_entries
    if include_raw:
        sheets["tokens"] = df_tokens2
        sheets["lines"] = df_lines

    excel_bytes = build_excel_bytes(sheets)
    st.download_button(
        "엑셀 다운로드",
        data=excel_bytes,
        file_name="parsed_document_pack.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

    st.info(
        "여기까지 되면: (1) 페이지 분류/세그먼트 (2) 지번/주소 기반 번들링 (3) 등기부 갑/을구 테이블까지 나옵니다.\n"
        "다음 단계는 '온톨로지(스키마)'로 목적/원인/권리자 텍스트를 타입별(소유권, 근저당, 압류 등)로 세분화해서 "
        "정규화된 시트(예: mortgages, seizures, ownerships)로 분해하는 것입니다."
    )


if __name__ == "__main__":
    main()

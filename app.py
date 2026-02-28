# app.py
import io
import re
import shutil
from typing import Optional, Literal

import fitz  # PyMuPDF
import pandas as pd
import streamlit as st
from PIL import Image

# Optional: OCR dependencies (pytesseract + tesseract binary)
try:
    import pytesseract  # type: ignore
    HAS_PYTESSERACT = True
except Exception:
    HAS_PYTESSERACT = False


TokenSource = Literal["pdf_text", "ocr"]


def get_tesseract_ok() -> bool:
    """pytesseract가 import 가능하고 tesseract 실행 파일이 시스템에 존재하면 True."""
    if not HAS_PYTESSERACT:
        return False
    return shutil.which("tesseract") is not None


@st.cache_data(show_spinner=False)
def get_pdf_page_count(pdf_bytes: bytes) -> int:
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    return doc.page_count


@st.cache_data(show_spinner=False)
def render_page_png_bytes(pdf_bytes: bytes, page_no: int, zoom: float) -> bytes:
    """
    PDF의 한 페이지를 PNG bytes로 렌더링.
    page_no는 1부터 시작.
    """
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    page = doc.load_page(page_no - 1)
    mat = fitz.Matrix(zoom, zoom)
    pix = page.get_pixmap(matrix=mat)
    return pix.tobytes("png")


def png_bytes_to_pil(png_bytes: bytes) -> Image.Image:
    return Image.open(io.BytesIO(png_bytes)).convert("RGB")


@st.cache_data(show_spinner=False)
def extract_pdf_text_tokens(pdf_bytes: bytes) -> pd.DataFrame:
    """
    텍스트 기반 PDF에서 text span 단위 토큰 + bbox 추출.
    columns: page, text, x0,y0,x1,y1, conf, source
    """
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    rows = []

    for page_idx in range(doc.page_count):
        page_no = page_idx + 1
        page = doc.load_page(page_idx)

        d = page.get_text("dict")  # blocks -> lines -> spans
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


def ocr_page_tesseract(
    pil_img: Image.Image,
    page_no: int,
    zoom: float,
    lang: str = "kor+eng",
    psm: int = 6,
) -> pd.DataFrame:
    """
    Tesseract OCR로 word-level bbox 토큰 추출.
    렌더링 zoom을 알면, 픽셀좌표를 PDF좌표로 근사 변환(zoom으로 나눔).
    """
    if not get_tesseract_ok():
        raise RuntimeError("Tesseract OCR is not available (missing pytesseract or tesseract binary).")

    img = pil_img.convert("L")  # grayscale
    config = f"--psm {psm}"

    data = pytesseract.image_to_data(
        img,
        lang=lang,
        output_type=pytesseract.Output.DICT,
        config=config,
    )

    rows = []
    n = len(data.get("text", []))
    for i in range(n):
        text = (data["text"][i] or "").strip()
        if not text:
            continue

        conf_val: Optional[float]
        try:
            conf_raw = float(data["conf"][i])
            if conf_raw < 0:
                continue
            conf_val = conf_raw / 100.0
        except Exception:
            conf_val = None

        left = float(data["left"][i])
        top = float(data["top"][i])
        width = float(data["width"][i])
        height = float(data["height"][i])

        # Pixel coords -> approx PDF coords
        x0 = left / zoom
        y0 = top / zoom
        x1 = (left + width) / zoom
        y1 = (top + height) / zoom

        rows.append(
            {
                "page": page_no,
                "text": text,
                "x0": x0,
                "y0": y0,
                "x1": x1,
                "y1": y1,
                "conf": conf_val,
                "source": "ocr",
            }
        )

    return pd.DataFrame(rows, columns=["page", "text", "x0", "y0", "x1", "y1", "conf", "source"])


def make_page_stats(page_count: int, df_pdf_text: pd.DataFrame) -> pd.DataFrame:
    """
    페이지별 pdf_text 토큰 개수 집계표 생성.
    """
    all_pages = pd.DataFrame({"page": list(range(1, page_count + 1))})
    if df_pdf_text.empty:
        return all_pages.assign(n_tokens=0)

    counts = df_pdf_text.groupby("page").size().rename("n_tokens").reset_index()
    stats = all_pages.merge(counts, on="page", how="left").fillna({"n_tokens": 0})
    stats["n_tokens"] = stats["n_tokens"].astype(int)
    return stats


def normalize_whitespace(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()


def tokens_to_lines(df_tokens: pd.DataFrame) -> pd.DataFrame:
    """
    토큰을 대략 '라인(행)' 단위로 묶어서 text를 합침.
    Returns: page, line_id, y0, y1, text
    """
    if df_tokens.empty:
        return pd.DataFrame(columns=["page", "line_id", "y0", "y1", "text"])

    rows = []
    for page, g in df_tokens.groupby("page"):
        g = g.copy()
        g["cy"] = (g["y0"] + g["y1"]) / 2.0
        g["h"] = (g["y1"] - g["y0"]).clip(lower=1.0)

        med_h = float(g["h"].median()) if len(g) else 10.0
        y_tol = med_h * 0.8  # 같은 라인으로 볼 y 허용 오차

        g = g.sort_values(["cy", "x0"]).reset_index(drop=True)

        current = []
        current_cy = None
        line_id = 0

        def flush():
            nonlocal line_id, current
            if not current:
                return
            current_sorted = sorted(current, key=lambda r: r["x0"])
            text = normalize_whitespace(" ".join([str(r["text"]) for r in current_sorted]))
            y0 = float(min([r["y0"] for r in current_sorted]))
            y1 = float(max([r["y1"] for r in current_sorted]))
            if text:
                rows.append({"page": int(page), "line_id": int(line_id), "y0": y0, "y1": y1, "text": text})
                line_id += 1
            current = []

        for _, r in g.iterrows():
            cy = float(r["cy"])
            if current_cy is None:
                current = [r]
                current_cy = cy
                continue

            if abs(cy - current_cy) <= y_tol:
                current.append(r)
                current_cy = (current_cy * (len(current) - 1) + cy) / len(current)
            else:
                flush()
                current = [r]
                current_cy = cy

        flush()

    return (
        pd.DataFrame(rows, columns=["page", "line_id", "y0", "y1", "text"])
        .sort_values(["page", "y0"])
        .reset_index(drop=True)
    )


def build_excel_bytes(df_tokens: pd.DataFrame, df_lines: pd.DataFrame, page_stats: pd.DataFrame) -> bytes:
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as writer:
        page_stats.to_excel(writer, index=False, sheet_name="page_stats")
        df_tokens.to_excel(writer, index=False, sheet_name="tokens")
        df_lines.to_excel(writer, index=False, sheet_name="lines")
    return out.getvalue()


def main():
    st.set_page_config(page_title="PDF → 토큰 추출 & OCR 보충 → 엑셀", layout="wide")
    st.title("PDF → (텍스트 추출 + OCR 보충) → 토큰/라인 엑셀")

    st.caption(
        "혼합형 PDF(일부 텍스트, 일부 스캔)도 처리 가능하게 만든 MVP입니다. "
        "텍스트 추출 → 토큰 부족 페이지는 OCR로 보충 → tokens/lines를 엑셀로 내보냅니다."
    )

    uploaded = st.file_uploader("PDF 파일 업로드", type=["pdf"])
    if not uploaded:
        st.stop()

    pdf_bytes = uploaded.getvalue()
    page_count = get_pdf_page_count(pdf_bytes)

    st.success(f"업로드 완료: {uploaded.name} (페이지 수: {page_count}, 크기: {len(pdf_bytes):,} bytes)")

    # Sidebar controls
    with st.sidebar:
        st.header("설정")

        zoom = st.slider("렌더링/ OCR 해상도(zoom)", min_value=1.0, max_value=4.0, value=2.0, step=0.25)

        st.markdown("---")
        ocr_enabled = st.checkbox("토큰 부족 페이지는 OCR로 보충", value=False)
        token_threshold = st.slider("OCR 대상 기준: 페이지 토큰 수 < N", 0, 300, 20, 5)

        ocr_lang = st.text_input("OCR 언어(Tesseract)", value="kor+eng")
        ocr_psm = st.selectbox(
            "OCR PSM 모드",
            options=[3, 4, 6, 11, 12, 13],
            index=2,
            help="6이 무난합니다. 표/문단 형태에 따라 바꿔보세요.",
        )

        st.markdown("---")
        st.write("OCR 상태:")
        if ocr_enabled:
            if get_tesseract_ok():
                st.success("Tesseract OCR 사용 가능")
            else:
                st.error("Tesseract OCR 사용 불가 (pytesseract 또는 tesseract 설치 필요)")

    # Layout columns
    left, right = st.columns([1, 1])

    # Preview page
    with left:
        st.subheader("페이지 미리보기")
        page_no = st.number_input("페이지", min_value=1, max_value=page_count, value=1, step=1)
        png_bytes = render_page_png_bytes(pdf_bytes, int(page_no), zoom)
        st.image(png_bytes, caption=f"Page {page_no}", use_container_width=True)

    # PDF text extraction
    with st.spinner("PDF 텍스트 토큰 추출 중..."):
        df_pdf_text = extract_pdf_text_tokens(pdf_bytes)

    page_stats = make_page_stats(page_count, df_pdf_text)

    with right:
        st.subheader("페이지별 텍스트 토큰 수 (혼합형 판별)")
        st.dataframe(page_stats, use_container_width=True, height=360)

        scanned_like = page_stats[page_stats["n_tokens"] < 20]
        if len(scanned_like) > 0:
            st.warning(f"토큰이 거의 없는 페이지가 {len(scanned_like)}개 있어요 → 이 페이지들은 OCR이 필요할 수 있습니다.")
        else:
            st.info("모든 페이지에서 토큰이 충분히 나옵니다 → OCR 없이도 구조화가 가능합니다.")

    st.markdown("---")

    # Run extraction button
    run = st.button("토큰 추출/갱신 (OCR 옵션 포함)")
    if run:
        all_frames = []
        all_frames.append(df_pdf_text)

        ocr_pages = []
        if ocr_enabled:
            if not get_tesseract_ok():
                st.error(
                    "OCR을 켰지만 Tesseract가 설치되어 있지 않습니다.\n\n"
                    "✅ Streamlit Cloud라면:\n"
                    "1) requirements.txt에 `pytesseract` 추가\n"
                    "2) packages.txt에 `tesseract-ocr`, `tesseract-ocr-kor` 추가\n\n"
                    "✅ 로컬 Windows라면:\n"
                    "Tesseract 설치 후 PATH 등록이 필요합니다."
                )
            else:
                ocr_pages = page_stats.loc[page_stats["n_tokens"] < token_threshold, "page"].astype(int).tolist()

        with st.spinner("필요한 페이지에 OCR 수행 중..."):
            for p in ocr_pages:
                png_bytes_p = render_page_png_bytes(pdf_bytes, int(p), zoom)
                pil = png_bytes_to_pil(png_bytes_p)
                df_ocr = ocr_page_tesseract(pil, page_no=int(p), zoom=zoom, lang=ocr_lang, psm=int(ocr_psm))
                all_frames.append(df_ocr)

        df_tokens = pd.concat(all_frames, ignore_index=True) if all_frames else pd.DataFrame(
            columns=["page", "text", "x0", "y0", "x1", "y1", "conf", "source"]
        )

        # Clean / sort
        if not df_tokens.empty:
            df_tokens["text"] = df_tokens["text"].astype(str).map(lambda s: s.strip())
            df_tokens = df_tokens[df_tokens["text"] != ""].copy()
            df_tokens = df_tokens.sort_values(["page", "y0", "x0"]).reset_index(drop=True)

        df_lines = tokens_to_lines(df_tokens)

        st.session_state["df_tokens"] = df_tokens
        st.session_state["df_lines"] = df_lines

    # Show results if available
    if "df_tokens" in st.session_state:
        df_tokens = st.session_state["df_tokens"]
        df_lines = st.session_state["df_lines"]

        st.subheader("토큰/라인 결과")
        st.write(f"총 토큰 수: {len(df_tokens):,} (pdf_text + ocr)")

        c1, c2 = st.columns([1, 1])
        with c1:
            page_view = st.number_input(
                "토큰/라인 보기: 페이지",
                min_value=1,
                max_value=page_count,
                value=1,
                step=1,
                key="page_view",
            )
            df_tok_p = df_tokens[df_tokens["page"] == int(page_view)]
            st.write(f"Page {page_view} 토큰: {len(df_tok_p):,}")
            st.dataframe(df_tok_p.head(300), use_container_width=True, height=360)

        with c2:
            df_line_p = df_lines[df_lines["page"] == int(page_view)]
            st.write(f"Page {page_view} 라인: {len(df_line_p):,}")
            st.dataframe(df_line_p.head(200), use_container_width=True, height=360)

        excel_bytes = build_excel_bytes(df_tokens, df_lines, page_stats)
        st.download_button(
            label="엑셀 다운로드 (page_stats + tokens + lines)",
            data=excel_bytes,
            file_name="tokens_lines.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

        st.info(
            "다음 단계(온톨로지 기반 배치): lines 결과를 기반으로 "
            "표제부/갑구/을구 섹션을 탐지하고, 열(순위번호/등기목적/접수/등기원인/권리자및기타사항)로 배치하는 룰을 추가하면 됩니다."
        )


if __name__ == "__main__":
    main()

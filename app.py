
import io
from dataclasses import dataclass
from typing import Literal, Optional

import fitz  # PyMuPDF
import pandas as pd
import streamlit as st
from PIL import Image


@dataclass
class Token:
    page: int
    text: str
    x0: float
    y0: float
    x1: float
    y1: float
    conf: Optional[float]
    source: Literal["pdf_text", "ocr"]


def render_pdf_pages(pdf_bytes: bytes, zoom: float = 2.0) -> list[Image.Image]:
    """Render PDF pages to PIL images (higher zoom = higher resolution)."""
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    images: list[Image.Image] = []
    mat = fitz.Matrix(zoom, zoom)  # controls resolution
    for page in doc:
        pix = page.get_pixmap(matrix=mat)
        img = Image.open(io.BytesIO(pix.tobytes("png")))
        images.append(img)
    return images


def extract_pdf_text_tokens(pdf_bytes: bytes) -> list[Token]:
    """
    Extract text tokens with bounding boxes from a text-based PDF.
    If the PDF is scanned, this may return very few tokens.
    """
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    tokens: list[Token] = []

    for page_idx, page in enumerate(doc):
        d = page.get_text("dict")  # includes bbox for blocks/lines/spans
        # dict structure: blocks -> lines -> spans
        for b in d.get("blocks", []):
            for line in b.get("lines", []):
                for span in line.get("spans", []):
                    text = (span.get("text") or "").strip()
                    if not text:
                        continue
                    x0, y0, x1, y1 = span.get("bbox", [None, None, None, None])
                    if None in (x0, y0, x1, y1):
                        continue
                    tokens.append(
                        Token(
                            page=page_idx + 1,
                            text=text,
                            x0=float(x0),
                            y0=float(y0),
                            x1=float(x1),
                            y1=float(y1),
                            conf=None,
                            source="pdf_text",
                        )
                    )
    return tokens


def tokens_to_dataframe(tokens: list[Token]) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "page": t.page,
                "text": t.text,
                "x0": t.x0,
                "y0": t.y0,
                "x1": t.x1,
                "y1": t.y1,
                "conf": t.conf,
                "source": t.source,
            }
            for t in tokens
        ]
    )


st.set_page_config(page_title="등기부등본 OCR/파서 MVP", layout="wide")
st.title("등기부등본 PDF → 토큰(텍스트+bbox) 추출 MVP")

uploaded = st.file_uploader("등기부등본 PDF 업로드", type=["pdf"])
if not uploaded:
    st.stop()

pdf_bytes = uploaded.getvalue()  # UploadedFile is file-like (BytesIO subclass)
st.success(f"업로드 완료: {uploaded.name} ({len(pdf_bytes):,} bytes)")

with st.spinner("PDF 페이지 렌더링 중..."):
    images = render_pdf_pages(pdf_bytes, zoom=2.0)

col_left, col_right = st.columns([1, 1])

with col_left:
    st.subheader("페이지 미리보기")
    page_no = st.number_input("페이지", min_value=1, max_value=len(images), value=1, step=1)
    st.image(images[page_no - 1], caption=f"Page {page_no}", use_container_width=True)

with col_right:
    st.subheader("PDF 텍스트 토큰 추출 결과 (텍스트 PDF면 이게 잘 나옴)")
    tokens = extract_pdf_text_tokens(pdf_bytes)
    df = tokens_to_dataframe(tokens)

    st.write(f"추출 토큰 수: {len(df):,}")
    st.dataframe(df.head(200), use_container_width=True)

    # 간단 판별: 토큰이 충분히 나오면 '텍스트 기반 PDF'일 가능성이 큼
    if len(df) > 100:
        st.info("이 PDF는 텍스트 기반일 가능성이 큽니다 → OCR 없이도 구조화가 가능합니다.")
    else:
        st.warning("토큰이 거의 없습니다 → 스캔 PDF일 가능성이 큽니다(다음 단계에서 OCR 필요).")

    # RAW 토큰 엑셀 덤프 다운로드
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="raw_tokens")
    st.download_button(
        label="RAW 토큰 엑셀 다운로드",
        data=out.getvalue(),
        file_name="raw_tokens.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

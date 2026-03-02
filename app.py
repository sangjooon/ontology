# -*- coding: utf-8 -*-
"""
문서 비서 📄 (dev)
- 네이버 일반 OCR + 좌표 기반 라벨-값 추출 (표 인식 없이도 핵심 필드 안정 추출)
- PDF 내 "텍스트 페이지"는 OCR 비용 절감 위해 OCR 스킵
- 페이지별 문서 타입 분류
- 한 PDF에 여러 부동산(지번)이 섞여도 행(row) 단위로 정리
- 교차검증(O/X) + 신뢰도 점수
- Excel(다중 시트) 다운로드

실행:
  streamlit run app.py

필수 패키지:
  pip install streamlit requests pandas openpyxl pypdf
"""

from __future__ import annotations

import streamlit as st
import requests
import pandas as pd
import uuid
import time
import json
import re
import io
import hashlib

from dataclasses import dataclass, field
from typing import Callable, Optional, List, Dict, Tuple


# ============================================================
# 0) 설정값
# ============================================================
APP_TITLE = "문서 비서📄 dev"
APP_VERSION = "v0.1.0"

DEFAULT_PASSWORD = "alohomora"  # ⚠️ 데모용. 실제 서비스에선 제거/변경 권장

OCR_MAX_PAGES_PER_REQUEST = 10       # 네이버 OCR PDF 요청당 페이지 수(보수적으로 10)
PDF_TEXT_MIN_CHARS = 40              # 이 이상 텍스트가 있으면 "텍스트 PDF"로 간주하여 OCR 스킵
PDF_TEXT_MIN_HANGUL = 3              # 한글이 이 이상 포함되면 텍스트 페이지 확률↑
LINE_GROUP_MIN_Y_THRESH = 8.0        # 라인 그룹핑 최소 y threshold


# ============================================================
# 1) 공통 유틸
# ============================================================
def normalize_whitespace(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def norm_key(s: str) -> str:
    """라벨 매칭용: 공백/특수문자 제거"""
    return re.sub(r"[\s\W_]+", "", (s or ""))


def count_hangul(s: str) -> int:
    return len(re.findall(r"[가-힣]", s or ""))


def is_meaningful_pdf_text(text: str) -> bool:
    t = normalize_whitespace(text)
    if len(t) < PDF_TEXT_MIN_CHARS:
        return False
    if count_hangul(t) < PDF_TEXT_MIN_HANGUL:
        # 숫자/영문만 잔뜩 있는 경우(페이지번호/머리말) 배제
        return False
    return True


# ============================================================
# 2) PDF 유틸
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


def pdf_extract_text_per_page(reader) -> List[str]:
    texts: List[str] = []
    for p in reader.pages:
        try:
            t = p.extract_text() or ""
        except Exception:
            t = ""
        texts.append(t)
    return texts


def build_pdf_bytes_from_pages(reader, page_indices_0based: List[int]) -> bytes:
    """원본 reader에서 특정 페이지만 뽑아 새 PDF bytes로 만든다(순서는 page_indices 순서)."""
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
# 3) 네이버 OCR 호출
# ============================================================
def call_naver_ocr(file_bytes: bytes, file_ext: str, api_url: str, secret_key: str, *, timeout: int = 90) -> Dict:
    """
    네이버 OCR을 호출해서 JSON 결과를 받아옵니다.
    - file_ext: "pdf" 또는 "jpg" 등
    """
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

        return {
            "ok": ok,
            "status_code": r.status_code,
            "text": (r.text or "")[:2000],
            "json": j,
        }
    except Exception as e:
        return {"ok": False, "error": str(e), "status_code": None, "text": ""}


# ============================================================
# 4) OCR JSON -> 토큰/라인
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
class PageContent:
    page_no: int
    source: str  # "pdf_text" | "ocr"
    raw_text: str = ""
    doc_type: str = "UNKNOWN"
    property_key: str = ""  # 보통 지번
    tokens: List[Token] = field(default_factory=list)
    lines: List[List[Token]] = field(default_factory=list)


def ocr_json_to_tokens(ocr_json: Dict, page_numbers: List[int]) -> List[Token]:
    """
    ocr_json["images"] 순서대로 page_numbers에 매핑하여 Token 리스트 반환.
    page_numbers 길이 == images 길이여야 함.
    """
    tokens: List[Token] = []
    images = ocr_json.get("images", []) if isinstance(ocr_json, dict) else []
    for img_idx, img in enumerate(images):
        page_no = page_numbers[img_idx] if img_idx < len(page_numbers) else (img_idx + 1)
        for f in img.get("fields", []):
            text = (f.get("inferText") or "").strip()
            if not text:
                continue
            verts = (f.get("boundingPoly") or {}).get("vertices", [])
            if not verts:
                continue
            xs = [v.get("x", 0) for v in verts]
            ys = [v.get("y", 0) for v in verts]
            tokens.append(
                Token(
                    text=text,
                    page=page_no,
                    x0=float(min(xs)),
                    y0=float(min(ys)),
                    x1=float(max(xs)),
                    y1=float(max(ys)),
                )
            )
    return tokens


def group_lines_for_page(tokens: List[Token]) -> List[List[Token]]:
    """
    한 페이지 토큰을 y 기준으로 줄(line) 그룹핑 후,
    각 줄은 x 기준 정렬.
    """
    if not tokens:
        return []

    # 토큰 높이 중앙값 기반으로 y threshold 결정
    hs = sorted([t.h for t in tokens if t.h > 0])
    base_h = hs[len(hs) // 2] if hs else 12.0
    y_thresh = max(LINE_GROUP_MIN_Y_THRESH, base_h * 0.6)

    toks = sorted(tokens, key=lambda t: t.cy)
    lines: List[List[Token]] = []
    cur: List[Token] = []
    last_y = toks[0].cy

    for tok in toks:
        if abs(tok.cy - last_y) > y_thresh and cur:
            lines.append(sorted(cur, key=lambda t: t.cx))
            cur = []
        cur.append(tok)
        last_y = tok.cy

    if cur:
        lines.append(sorted(cur, key=lambda t: t.cx))

    return lines


def tokens_to_text(tokens: List[Token]) -> str:
    """디버그/분류용: 토큰을 줄 단위 텍스트로 변환."""
    if not tokens:
        return ""
    lines = group_lines_for_page(tokens)
    return "\n".join(" ".join(t.text for t in line).strip() for line in lines).strip()


# ============================================================
# 5) 문서 타입 분류
# ============================================================
DOC_LAND_USE_PLAN = "LAND_USE_PLAN"          # 토지이용계획확인서
DOC_MAP = "MAP"                              # 지적도 등본 등
DOC_REGISTRY_LAND = "REGISTRY_LAND"          # 등기사항전부증명서(토지)
DOC_REGISTRY_BUILDING = "REGISTRY_BUILDING"  # 등기사항전부증명서(건물)
DOC_REGISTRY_SUMMARY = "REGISTRY_SUMMARY"    # 주요 등기사항 요약
DOC_LAND_REGISTER = "LAND_REGISTER"          # 토지대장
DOC_BUILDING_LEDGER = "BUILDING_LEDGER"      # 건축물대장(일반/총괄표제부 등)
DOC_SHARE_REGISTER = "SHARE_REGISTER"        # 공유지연명부
DOC_UNKNOWN = "UNKNOWN"


def guess_doc_type(text: str) -> str:
    t = norm_key(text)
    if "토지이용계획확인서" in t:
        return DOC_LAND_USE_PLAN
    if "지적도등본" in t or "지적도" in t:
        return DOC_MAP
    if "주요등기사항요약" in t:
        return DOC_REGISTRY_SUMMARY
    if "등기사항전부증명서" in t:
        # 타이틀에 토지/건물이 같이 등장하는 경우 대비
        if "토지" in t and "건물" not in t:
            return DOC_REGISTRY_LAND
        if "건물" in t:
            return DOC_REGISTRY_BUILDING
        return DOC_REGISTRY_LAND
    if "토지대장" in t or ("토지" in t and "대장" in t):
        return DOC_LAND_REGISTER
    if "공유지연명부" in t:
        return DOC_SHARE_REGISTER
    if "일반건축물대장" in t or "건축물대장총괄표제부" in t or ("건축물대장" in t and "표제부" in t):
        return DOC_BUILDING_LEDGER
    return DOC_UNKNOWN


# ============================================================
# 6) 라벨-값 추출(좌표 기반)
# ============================================================
@dataclass(frozen=True)
class FieldSpec:
    field: str
    labels: Tuple[str, ...]
    direction: str = "right"          # "right" | "below"
    allow_multiline: bool = False
    priority: int = 50                # 낮을수록 우선
    normalizer: Optional[Callable[[str], str]] = None
    validator: Optional[Callable[[str], bool]] = None


def find_label_span(line: List[Token], label_variants: Tuple[str, ...], *, max_window: int = 4) -> Optional[Tuple[int, int]]:
    """
    한 줄(line)에서 label이 토큰 여러개로 쪼개져도(window 결합) 찾아서 (start_idx, end_idx) 반환.
    """
    if not line:
        return None
    norm_tokens = [norm_key(t.text) for t in line]
    for lab in label_variants:
        target = norm_key(lab)
        if not target:
            continue
        for w in range(1, max_window + 1):
            for i in range(0, len(line) - w + 1):
                merged = "".join(norm_tokens[i : i + w])
                if merged == target:
                    return (i, i + w - 1)
    return None


def _line_has_any_label(line: List[Token], stop_labels_norm: set) -> bool:
    for tok in line:
        if norm_key(tok.text) in stop_labels_norm:
            return True
    return False


def extract_right_value_from_lines(
    lines: List[List[Token]],
    label_span: Tuple[int, int],
    *,
    line_idx: int,
    stop_labels_norm: set,
    allow_multiline: bool,
) -> str:
    """label이 있는 line_idx 기준으로 오른쪽 값을 추출 (필요 시 다음 줄까지)."""
    line = lines[line_idx]
    s, e = label_span
    label_end_x = max(t.x1 for t in line[s : e + 1])
    # 1) 같은 줄 오른쪽
    vals = []
    for tok in line[e + 1 :]:
        if tok.x0 < label_end_x + 3:
            continue
        if norm_key(tok.text) in stop_labels_norm:
            break
        vals.append(tok.text)

    # 2) 멀티라인이면 다음 줄(최대 2줄)까지 이어붙이기
    if allow_multiline:
        for j in range(line_idx + 1, min(line_idx + 3, len(lines))):
            next_line = lines[j]

            # 다음 줄이 "새 행/새 라벨 행"이면 중단
            if _line_has_any_label(next_line, stop_labels_norm):
                break

            # 다음 줄에서 라벨 오른쪽 영역 토큰만 수집
            ext = []
            for tok in next_line:
                if tok.x0 < label_end_x + 3:
                    continue
                if norm_key(tok.text) in stop_labels_norm:
                    break
                ext.append(tok.text)

            # 다음 줄이 사실상 값이 없으면 중단
            if not ext:
                break

            vals.extend(ext)

    return " ".join(vals).strip()


def extract_field_from_ocr_page(page: PageContent, spec: FieldSpec, *, stop_labels_norm: set) -> List[str]:
    """
    OCR 페이지에서 spec에 해당하는 값 후보들을 추출(여러 개 있을 수 있음).
    """
    if not page.lines:
        page.lines = group_lines_for_page(page.tokens)

    candidates: List[str] = []
    lines = page.lines

    for li, line in enumerate(lines):
        span = find_label_span(line, spec.labels)
        if span is None:
            continue

        if spec.direction == "right":
            v = extract_right_value_from_lines(
                lines,
                span,
                line_idx=li,
                stop_labels_norm=stop_labels_norm,
                allow_multiline=spec.allow_multiline,
            )
        else:
            # below: MVP에선 간단히 다음 줄 전체를 값으로(라벨이 있는 줄은 제외)
            v = ""
            if li + 1 < len(lines) and not _line_has_any_label(lines[li + 1], stop_labels_norm):
                v = " ".join(t.text for t in lines[li + 1]).strip()

        v = normalize_whitespace(v)
        if not v:
            continue
        if spec.normalizer:
            v = spec.normalizer(v)
        if spec.validator and not spec.validator(v):
            continue
        candidates.append(v)

    # 중복 제거(순서 유지)
    uniq: List[str] = []
    seen = set()
    for v in candidates:
        if v not in seen:
            uniq.append(v)
            seen.add(v)
    return uniq


def extract_field_from_text(text: str, spec: FieldSpec) -> List[str]:
    """
    텍스트 PDF 페이지에서 라벨 기반으로 값 후보 추출(간단 regex).
    - "지번\n496-10" 처럼 라벨/값이 줄바꿈으로 갈라져도 잡히도록 설계
    """
    if not text:
        return []
    candidates: List[str] = []
    for lab in spec.labels:
        # 예: "지번 496-10" / "지번: 496-10" / "지번\n496-10"
        pat = re.compile(rf"{re.escape(lab)}\s*[: ]?\s*([^\n\r]+)")
        for m in pat.finditer(text):
            v = normalize_whitespace(m.group(1))
            if spec.normalizer:
                v = spec.normalizer(v)
            if spec.validator and not spec.validator(v):
                continue
            if v:
                candidates.append(v)

    # 중복 제거
    uniq: List[str] = []
    seen = set()
    for v in candidates:
        if v not in seen:
            uniq.append(v)
            seen.add(v)
    return uniq


# ============================================================
# 7) 값 정규화/검증(핵심 필드)
# ============================================================
LOT_RE = re.compile(r"^\d{1,4}(?:-\d{1,4})?$")


def normalize_lot(v: str) -> str:
    v = re.sub(r"[^\d\-]", "", v or "")
    v = v.strip("-")
    return v


def valid_lot(v: str) -> bool:
    return bool(LOT_RE.match(normalize_lot(v)))


def parse_area_sqm(v: str) -> Optional[float]:
    m = re.search(r"(\d+(?:,\d+)*(?:\.\d+)?)", v or "")
    if not m:
        return None
    try:
        return float(m.group(1).replace(",", ""))
    except Exception:
        return None


def format_area_sqm(num: float) -> str:
    if num is None:
        return ""
    if abs(num - round(num)) < 1e-6:
        return f"{int(round(num))} ㎡"
    # 소수점이 있는 경우
    s = f"{num:.3f}".rstrip("0").rstrip(".")
    return f"{s} ㎡"


def normalize_area(v: str) -> str:
    num = parse_area_sqm(v)
    return format_area_sqm(num) if num is not None else normalize_whitespace(v)


def valid_area(v: str) -> bool:
    return parse_area_sqm(v) is not None


def normalize_address(v: str) -> str:
    return normalize_whitespace(v)


OWNER_RE = re.compile(r"소유자\s*([가-힣]{2,5})\s*\d{6}-\*{4,7}")


def extract_owner_from_text(text: str) -> str:
    """
    등기/대장 등에서 흔히 나오는 패턴:
      '소유자 홍길동 900101-*******'
    """
    if not text:
        return ""
    matches = OWNER_RE.findall(text)
    if matches:
        return matches[-1].strip()
    return ""


def extract_issue_date(text: str) -> str:
    """
    발급일: 2025년 11월 3일 형태 등을 포착.
    """
    if not text:
        return ""
    # 2025년 11월 3일
    m = re.search(r"발급일\s*[: ]\s*(\d{4}\s*년\s*\d{1,2}\s*월\s*\d{1,2}\s*일)", text)
    if m:
        return normalize_whitespace(m.group(1))
    # 2025.11.03 또는 2025/11/03
    m = re.search(r"발급일\s*[: ]\s*(\d{4}[./-]\d{1,2}[./-]\d{1,2})", text)
    if m:
        return m.group(1)
    return ""


def extract_doc_confirm_no(text: str) -> str:
    """
    문서확인번호/발급확인번호: XXXXX 형태를 포착.
    """
    if not text:
        return ""
    m = re.search(r"(?:문서확인번호|발급확인번호)\s*[: ]\s*([A-Z0-9\-]+)", text, re.I)
    if m:
        return m.group(1).strip()
    return ""


# ============================================================
# 8) 필드 스펙(문서타입별)
# ============================================================
# 공통 필드
FS_LOT = FieldSpec("지번", ("지번",), normalizer=normalize_lot, validator=valid_lot, priority=1)
FS_SITE = FieldSpec(
    "대지위치",
    ("대지위치", "소재지", "소재지번", "소재지(번)"),
    allow_multiline=True,
    normalizer=normalize_address,
    priority=5,
)
FS_ROAD_ADDR = FieldSpec("도로명주소", ("도로명주소",), allow_multiline=True, normalizer=normalize_address, priority=5)

FS_LAND_CATEGORY = FieldSpec("지목", ("지목",), normalizer=normalize_whitespace, priority=10)
FS_LAND_AREA = FieldSpec("토지면적", ("면적", "대지면적"), normalizer=normalize_area, validator=valid_area, priority=10)

FS_FLOOR_AREA = FieldSpec("연면적", ("연면적",), normalizer=normalize_area, validator=valid_area, priority=20)

FS_ZONING = FieldSpec("용도지역", ("용도지역", "지역", "지구", "구역"), allow_multiline=True, normalizer=normalize_whitespace, priority=30)

FS_OWNER = FieldSpec("최종소유자", ("소유자",), normalizer=normalize_whitespace, priority=2)

FS_ISSUE_DATE = FieldSpec("발급일", ("발급일",), normalizer=normalize_whitespace, priority=100)
FS_DOC_NO = FieldSpec("문서확인번호", ("문서확인번호", "발급확인번호"), normalizer=normalize_whitespace, priority=100)

DOC_SPECS: Dict[str, List[FieldSpec]] = {
    DOC_LAND_USE_PLAN: [FS_SITE, FS_LOT, FS_ZONING, FS_DOC_NO, FS_ISSUE_DATE],
    DOC_MAP: [FS_SITE, FS_LOT, FS_DOC_NO, FS_ISSUE_DATE],
    DOC_REGISTRY_LAND: [FS_SITE, FS_LOT, FS_LAND_CATEGORY, FS_LAND_AREA, FS_OWNER],
    DOC_REGISTRY_BUILDING: [FS_SITE, FS_LOT, FS_ROAD_ADDR, FS_FLOOR_AREA, FS_OWNER],
    DOC_REGISTRY_SUMMARY: [FS_OWNER],
    DOC_LAND_REGISTER: [FS_SITE, FS_LOT, FS_LAND_CATEGORY, FS_LAND_AREA, FS_OWNER, FS_DOC_NO, FS_ISSUE_DATE],
    DOC_BUILDING_LEDGER: [FS_SITE, FS_LOT, FS_ROAD_ADDR, FS_LAND_AREA, FS_FLOOR_AREA, FS_OWNER, FS_DOC_NO, FS_ISSUE_DATE],
    DOC_SHARE_REGISTER: [FS_SITE, FS_LOT, FS_OWNER],
    DOC_UNKNOWN: [],
}


def stop_labels_for_doc(doc_type: str) -> set:
    """해당 문서타입에서 라벨-값 추출 시, 값 수집을 끊기 위한 라벨 후보군."""
    labels = []
    for spec in DOC_SPECS.get(doc_type, []):
        labels.extend(list(spec.labels))
    # 너무 공격적으로 끊기지 않도록 핵심 라벨만 포함(필요 시 커스터마이즈)
    base = {
        "지번",
        "대지위치",
        "도로명주소",
        "지목",
        "면적",
        "대지면적",
        "연면적",
        "소유자",
        "발급일",
        "문서확인번호",
        "발급확인번호",
    }
    return {norm_key(x) for x in set(labels).union(base) if norm_key(x)}


# ============================================================
# 9) 페이지 단위 필드 추출 + 후보(Candidate) 수집
# ============================================================
@dataclass
class Candidate:
    property_key: str
    field: str
    value: str
    doc_type: str
    page_no: int
    source: str
    priority: int


@dataclass
class PropertyAggregate:
    property_key: str
    candidates: Dict[str, List[Candidate]] = field(default_factory=dict)
    seen_doc_types: set = field(default_factory=set)

    def add(self, cand: Candidate):
        self.seen_doc_types.add(cand.doc_type)
        self.candidates.setdefault(cand.field, []).append(cand)


def extract_lot_from_page(page: PageContent) -> str:
    """
    페이지에서 지번(=property_key) 후보를 찾는다.
    1) 라벨 기반(지번)
    2) 텍스트 기반 정규식
    3) 지역명(리/동/읍/면) + 숫자 패턴 fallback
    """
    # 1) 라벨 기반
    spec = FS_LOT
    if page.source == "ocr" and page.tokens:
        stop = stop_labels_for_doc(page.doc_type)
        vals = extract_field_from_ocr_page(page, spec, stop_labels_norm=stop)
        if vals:
            return normalize_lot(vals[0])

    # 2) 텍스트 기반(라벨)
    vals = extract_field_from_text(page.raw_text, spec)
    if vals:
        return normalize_lot(vals[0])

    # 3) fallback: '계향리 496-10' 같은 패턴
    m = re.search(r"(?:[가-힣]{1,10}(?:리|동|읍|면))\s*([0-9]{1,4}(?:-[0-9]{1,4})?)", page.raw_text)
    if m:
        v = normalize_lot(m.group(1))
        if valid_lot(v):
            return v

    # 4) 마지막 fallback: 페이지에서 가장 그럴듯한 'N-N' 패턴(주의)
    m = re.search(r"\b(\d{1,4}-\d{1,4})\b", page.raw_text)
    if m:
        v = normalize_lot(m.group(1))
        if valid_lot(v):
            return v

    return ""


def extract_page_candidates(page: PageContent) -> List[Candidate]:
    """
    페이지에서 (필드, 값) 후보들을 수집.
    """
    doc_type = page.doc_type
    specs = DOC_SPECS.get(doc_type, [])
    stop = stop_labels_for_doc(doc_type)

    out: List[Candidate] = []

    # (A) 라벨 기반 추출
    for spec in specs:
        values: List[str] = []
        if page.source == "ocr" and page.tokens:
            values = extract_field_from_ocr_page(page, spec, stop_labels_norm=stop)
        else:
            values = extract_field_from_text(page.raw_text, spec)

        # (B) 특정 필드(소유자)는 regex가 더 잘 먹히는 케이스가 많아서 보강
        if spec.field == "최종소유자" and (not values):
            owner = extract_owner_from_text(page.raw_text)
            if owner:
                values = [owner]

        # (C) 발급일/문서확인번호는 라벨이 흔들릴 수 있어 전용 regex 보강
        if spec.field == "발급일" and (not values):
            d = extract_issue_date(page.raw_text)
            if d:
                values = [d]
        if spec.field == "문서확인번호" and (not values):
            no = extract_doc_confirm_no(page.raw_text)
            if no:
                values = [no]

        for v in values:
            if not v:
                continue
            out.append(
                Candidate(
                    property_key=page.property_key,
                    field=spec.field,
                    value=v,
                    doc_type=doc_type,
                    page_no=page.page_no,
                    source=page.source,
                    priority=spec.priority,
                )
            )

    # (D) 문서 타입이 등기인데, 소유자 라벨이 깨질 수 있으니 추가 보강(중복 허용)
    if doc_type in (DOC_REGISTRY_LAND, DOC_REGISTRY_BUILDING, DOC_REGISTRY_SUMMARY):
        owner = extract_owner_from_text(page.raw_text)
        if owner:
            out.append(
                Candidate(
                    property_key=page.property_key,
                    field="최종소유자",
                    value=normalize_whitespace(owner),
                    doc_type=doc_type,
                    page_no=page.page_no,
                    source=page.source,
                    priority=2,
                )
            )

    # 중복 제거(동일 필드/값/페이지)
    uniq: List[Candidate] = []
    seen = set()
    for c in out:
        key = (c.property_key, c.field, c.value, c.doc_type, c.page_no)
        if key in seen:
            continue
        seen.add(key)
        uniq.append(c)

    return uniq


# ============================================================
# 10) 후보 -> 최종값 선택 + 교차검증/신뢰도
# ============================================================
def choose_best_value(cands: List[Candidate]) -> str:
    """
    동일 field 후보 중 최종값 선정.
    - priority 낮은 것 우선
    - 같은 priority면 더 "긴" 값(정보량) 우선
    - 그 다음 page_no가 작은 것(대체로 표제부/헤더) 우선
    """
    if not cands:
        return ""
    cands_sorted = sorted(cands, key=lambda c: (c.priority, -len(c.value), c.page_no))
    return cands_sorted[0].value


def consistency_flag(values: List[str]) -> str:
    """여러 문서에서 나온 값들의 일치 여부."""
    vals = [v for v in values if v]
    uniq = sorted(set(vals))
    if len(uniq) <= 1:
        return "O"
    return "X"


def compute_confidence(final_row: Dict, field_candidates: Dict[str, List[Candidate]]) -> int:
    """
    간단한 신뢰도 점수(0~100).
    - 핵심 필드 채워질수록 +점
    - 필드 일치성(X)이면 -점
    """
    score = 0
    core_fields = ["지번", "대지위치", "도로명주소", "지목", "토지면적", "최종소유자"]
    for f in core_fields:
        if final_row.get(f):
            score += 12  # 6개면 72점

    # 보너스: 문서확인번호/발급일
    if final_row.get("발급일"):
        score += 6
    if final_row.get("문서확인번호"):
        score += 6

    # 패널티: 불일치
    if final_row.get("지번_일치여부") == "X":
        score -= 20
    if final_row.get("소유자_일치여부") == "X":
        score -= 20
    if final_row.get("면적_일치여부") == "X":
        score -= 10

    return max(0, min(100, score))


def finalize_property_records(aggregates: Dict[str, PropertyAggregate]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    property_key별 최종 레코드 DF + 후보 상세 DF 반환
    """
    rows: List[Dict] = []
    cand_rows: List[Dict] = []

    for pk, agg in aggregates.items():
        final: Dict[str, str] = {
            "지번": pk,
            "대지위치": "",
            "도로명주소": "",
            "지목": "",
            "토지면적": "",
            "연면적": "",
            "용도지역": "",
            "최종소유자": "",
            "발급일": "",
            "문서확인번호": "",
            "문서종류": ", ".join(sorted(agg.seen_doc_types)),
        }

        # 후보 상세 DF용
        for field_name, cands in agg.candidates.items():
            for c in cands:
                cand_rows.append(
                    {
                        "지번": c.property_key,
                        "필드": c.field,
                        "값": c.value,
                        "문서타입": c.doc_type,
                        "페이지": c.page_no,
                        "소스": c.source,
                        "우선순위": c.priority,
                    }
                )

        # 최종값 선정
        for field_name, cands in agg.candidates.items():
            final[field_name] = choose_best_value(cands)

        # 교차검증(일치여부)
        lot_vals = [c.value for c in agg.candidates.get("지번", [])] + [pk]
        owner_vals = [c.value for c in agg.candidates.get("최종소유자", [])]
        area_vals = [c.value for c in agg.candidates.get("토지면적", [])]

        final["지번_일치여부"] = consistency_flag([normalize_lot(v) for v in lot_vals if v])
        final["소유자_일치여부"] = consistency_flag([v.strip() for v in owner_vals if v])
        final["면적_일치여부"] = consistency_flag([normalize_area(v) for v in area_vals if v])

        # 신뢰도
        final["신뢰도(0-100)"] = compute_confidence(final, agg.candidates)

        rows.append(final)

    df_final = pd.DataFrame(rows).sort_values(by=["지번"], kind="stable") if rows else pd.DataFrame(
        columns=[
            "지번",
            "대지위치",
            "도로명주소",
            "지목",
            "토지면적",
            "연면적",
            "용도지역",
            "최종소유자",
            "발급일",
            "문서확인번호",
            "문서종류",
            "지번_일치여부",
            "소유자_일치여부",
            "면적_일치여부",
            "신뢰도(0-100)",
        ]
    )

    df_cands = (
        pd.DataFrame(cand_rows).sort_values(by=["지번", "필드", "우선순위", "페이지"], kind="stable")
        if cand_rows
        else pd.DataFrame(columns=["지번", "필드", "값", "문서타입", "페이지", "소스", "우선순위"])
    )
    return df_final, df_cands


# ============================================================
# 11) Excel bytes 생성(다중시트)
# ============================================================
def make_excel_bytes(df_final: pd.DataFrame, df_candidates: pd.DataFrame, df_pages: pd.DataFrame) -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df_final.to_excel(writer, index=False, sheet_name="extracted")
        df_candidates.to_excel(writer, index=False, sheet_name="candidates")
        df_pages.to_excel(writer, index=False, sheet_name="pages")
    return output.getvalue()


# ============================================================
# 12) 전체 처리 파이프라인
# ============================================================
def process_pdf(file_bytes: bytes, api_url: str, secret_key: str, progress_cb: Optional[Callable] = None):
    """
    1) PDF 텍스트 추출로 OCR 스킵 가능한 페이지를 판단
    2) OCR 필요한 페이지만 모아(최대 10페이지씩) 네이버 OCR 호출
    3) 페이지별 문서 타입 분류
    4) 페이지별 지번(property_key) 추정
    5) 라벨-값 기반 후보 수집 -> 지번별 최종값 선택
    6) Excel/디버그용 데이터 생성
    """
    reader = pdf_reader_from_bytes(file_bytes)
    total_pages = len(reader.pages)

    # (1) PDF 텍스트 추출
    pdf_texts = pdf_extract_text_per_page(reader)

    pages: List[PageContent] = []
    ocr_needed_indices: List[int] = []

    for i in range(total_pages):
        t = pdf_texts[i] or ""
        if is_meaningful_pdf_text(t):
            pages.append(PageContent(page_no=i + 1, source="pdf_text", raw_text=t))
        else:
            pages.append(PageContent(page_no=i + 1, source="ocr", raw_text=""))
            ocr_needed_indices.append(i)

    # (2) OCR 필요한 페이지들만 묶어서 요청
    ocr_chunks = chunk_list(ocr_needed_indices, OCR_MAX_PAGES_PER_REQUEST)

    all_tokens_by_page: Dict[int, List[Token]] = {}

    for idx, chunk in enumerate(ocr_chunks, start=1):
        if progress_cb:
            progress_cb(idx, max(1, len(ocr_chunks)), min(chunk) + 1, max(chunk) + 1)

        chunk_pdf = build_pdf_bytes_from_pages(reader, chunk)
        result = call_naver_ocr(chunk_pdf, "pdf", api_url, secret_key)

        if not result.get("ok"):
            raise RuntimeError(
                f"OCR 실패 (chunk {idx}/{len(ocr_chunks)}; pages {min(chunk)+1}-{max(chunk)+1})\n"
                f"status={result.get('status_code')}\n{result.get('text') or result.get('error')}"
            )

        ocr_json = result.get("json")
        if not ocr_json:
            raise RuntimeError(
                f"OCR JSON 파싱 실패 (chunk {idx}/{len(ocr_chunks)}; pages {min(chunk)+1}-{max(chunk)+1})\n"
                f"{result.get('text')}"
            )

        page_numbers = [i + 1 for i in chunk]  # 원본 페이지 번호로 매핑
        tokens = ocr_json_to_tokens(ocr_json, page_numbers=page_numbers)

        for pno in page_numbers:
            all_tokens_by_page.setdefault(pno, [])

        for tok in tokens:
            all_tokens_by_page[tok.page].append(tok)

    # (3) OCR 페이지 raw_text/lines 채우기 + 문서 타입 분류
    for p in pages:
        if p.source == "ocr":
            p.tokens = all_tokens_by_page.get(p.page_no, [])
            p.lines = group_lines_for_page(p.tokens)
            p.raw_text = tokens_to_text(p.tokens)

        p.doc_type = guess_doc_type(p.raw_text)

    # (4) 페이지별 지번(property_key) 추정 + 스캔하면서 carry-forward
    current_pk = ""
    for p in pages:
        pk = extract_lot_from_page(p)
        if pk:
            current_pk = pk

        # 주요 문서 타입일 때만 carry-forward(UNKNOWN까지 carry하면 오염됨)
        if not pk and current_pk and p.doc_type in (
            DOC_LAND_USE_PLAN,
            DOC_MAP,
            DOC_REGISTRY_LAND,
            DOC_REGISTRY_BUILDING,
            DOC_REGISTRY_SUMMARY,
            DOC_LAND_REGISTER,
            DOC_BUILDING_LEDGER,
            DOC_SHARE_REGISTER,
        ):
            pk = current_pk

        p.property_key = pk

    # (5) 후보 수집
    aggregates: Dict[str, PropertyAggregate] = {}
    page_index_rows: List[Dict] = []

    for p in pages:
        page_index_rows.append(
            {
                "페이지": p.page_no,
                "소스": p.source,
                "문서타입": p.doc_type,
                "지번(추정)": p.property_key,
                "텍스트길이": len(p.raw_text or ""),
            }
        )

        if not p.property_key:
            continue

        agg = aggregates.setdefault(p.property_key, PropertyAggregate(property_key=p.property_key))
        for c in extract_page_candidates(p):
            if not c.value:
                continue
            agg.add(c)

    df_pages = pd.DataFrame(page_index_rows)

    # (6) 최종 레코드/후보 DF
    df_final, df_candidates = finalize_property_records(aggregates)

    # (7) 원본 텍스트(디버그) 합치기
    raw_text = "\n\n".join(
        f"===== PAGE {p.page_no} | {p.doc_type} | {p.source} | pk={p.property_key or '-'} =====\n{p.raw_text}".strip()
        for p in pages
        if p.raw_text
    )

    excel_bytes = make_excel_bytes(df_final, df_candidates, df_pages)
    return raw_text, df_final, df_candidates, df_pages, excel_bytes


# ============================================================
# 13) Streamlit UI
# ============================================================
def main():
    st.set_page_config(page_title=APP_TITLE, page_icon="📄", layout="wide")
    st.title(APP_TITLE)
    st.caption(f"{APP_VERSION} | 네이버 일반 OCR + 좌표 기반 핵심필드 추출")

    st.markdown(
        """
- **PDF 텍스트가 있는 페이지는 OCR을 스킵**해서 비용을 줄입니다.
- 스캔 페이지(이미지)는 **네이버 일반 OCR**을 사용하되, **bbox 좌표를 활용한 라벨-값 추출**로 표 인식 없이도 핵심필드를 뽑습니다.
- 한 PDF에 여러 지번이 섞여도 **지번별로 행(row)로 정리**합니다.
"""
    )

    # 임시 비밀번호
    with st.expander("🔐 접근(데모용)", expanded=True):
        password = st.text_input("비밀번호를 입력하세요", type="password")
        if password != DEFAULT_PASSWORD:
            st.warning("비밀번호가 올바르지 않습니다.")
            st.stop()
        st.success("환영합니다! 문서 비서를 시작합니다.")

    # 사이드바 API 설정
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
        st.caption("⚠️ API 키는 절대 코드/깃에 커밋하지 마세요.")

    uploaded_file = st.file_uploader("📎 PDF 문서파일을 업로드하세요", type=["pdf"], key="uploader_pdf")
    if uploaded_file is None:
        st.info("PDF를 업로드하면, '데이터 추출 시작' 버튼이 활성화됩니다.")
        st.stop()

    file_bytes = uploaded_file.getvalue()
    file_hash = hashlib.sha256(file_bytes).hexdigest()

    # 파일이 바뀌면 결과만 초기화(키 입력값은 유지)
    if st.session_state.get("file_hash") != file_hash:
        for k in ["raw_text", "df_final", "df_candidates", "df_pages", "excel_bytes"]:
            st.session_state.pop(k, None)
        st.session_state["file_hash"] = file_hash

    clicked = st.button("🔍 데이터 추출 시작", key="extract_btn", disabled=not bool(api_url and secret_key))

    if clicked:
        if not api_url or not secret_key:
            st.error("API URL / Secret Key 확인 필요")
            st.stop()

        progress_bar = st.progress(0)
        status = st.empty()

        def progress_cb(i, total, start_p, end_p):
            pct = int(i / max(1, total) * 100)
            progress_bar.progress(min(100, pct))
            status.write(f"📄 OCR 진행: {i}/{total} 묶음 (페이지 {start_p}~{end_p})")

        with st.spinner("OCR 및 데이터 추출 중..."):
            try:
                raw_text, df_final, df_candidates, df_pages, excel_bytes = process_pdf(
                    file_bytes, api_url, secret_key, progress_cb=progress_cb
                )
            except Exception as e:
                st.error(str(e))
                st.stop()

        progress_bar.progress(100)
        status.write("✅ 처리 완료")

        st.session_state["raw_text"] = raw_text
        st.session_state["df_final"] = df_final
        st.session_state["df_candidates"] = df_candidates
        st.session_state["df_pages"] = df_pages
        st.session_state["excel_bytes"] = excel_bytes

    # 결과 표시
    if "df_final" in st.session_state:
        df_final: pd.DataFrame = st.session_state["df_final"]
        df_candidates: pd.DataFrame = st.session_state["df_candidates"]
        df_pages: pd.DataFrame = st.session_state["df_pages"]
        raw_text: str = st.session_state["raw_text"]
        excel_bytes: bytes = st.session_state["excel_bytes"]

        st.divider()

        col1, col2 = st.columns([1, 1])

        with col1:
            st.subheader("✅ 최종 추출 결과(지번별)")
            st.dataframe(df_final, use_container_width=True, hide_index=True)

            st.download_button(
                label="📥 엑셀 파일 다운로드 (extracted/candidates/pages)",
                data=excel_bytes,
                file_name=f"규칙추출_{uploaded_file.name if uploaded_file else 'result'}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="download_excel_btn",
            )

            st.subheader("🧭 페이지 인덱스(문서타입/지번 추정)")
            st.dataframe(df_pages, use_container_width=True, hide_index=True)

        with col2:
            st.subheader("🧪 후보 상세(디버그)")
            st.dataframe(df_candidates, use_container_width=True, hide_index=True)

            st.subheader("📄 페이지별 텍스트(디버그)")
            st.text_area("RAW TEXT", raw_text, height=520)

    else:
        st.info("버튼을 눌러 추출을 시작하면 결과가 여기에 유지됩니다.")


if __name__ == "__main__":
    main()

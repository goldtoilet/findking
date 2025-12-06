#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
from datetime import datetime, timedelta, timezone

import streamlit as st
import pandas as pd
import requests

from supabase import create_client, Client
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# ============================
# 기본 설정
# ============================
st.set_page_config(
    page_title="YouTube 검색기 (Streamlit)",
    page_icon="🔍",
    layout="wide",
)

st.title("🔍 YouTube 검색기 (Streamlit)")

# 모바일에서 보기 쉽게 상단 여백 조금만
st.markdown(
    """
    <style>
    .block-container { padding-top: 1rem !important; }
    </style>
    """,
    unsafe_allow_html=True,
)

# ============================
# 시간 / 상수
# ============================
KST = timezone(timedelta(hours=9))
ENV_KEY_NAME = "YOUTUBE_API_KEY"

WEEKDAY_KO = ["월요일","화요일","수요일","목요일","금요일","토요일","일요일"]

COUNTRY_LANG_MAP = {
    "한국": ("KR", "ko"),
    "일본": ("JP", "ja"),
    "미국": ("US", "en"),
    "영국": ("GB", "en"),
    "독일": ("DE", "de"),
    "프랑스": ("FR", "fr"),
    "스페인": ("ES", "es"),
    "이탈리아": ("IT", "it"),
    "브라질": ("BR", "pt"),
    "인도": ("IN", "en"),
    "호주": ("AU", "en"),
}
COUNTRY_LIST = list(COUNTRY_LANG_MAP.keys())

# ============================
# Supabase 클라이언트
# ============================
@st.cache_resource
def get_supabase_client() -> Client | None:
    url = st.secrets.get("SUPABASE_URL")
    key = st.secrets.get("SUPABASE_SERVICE_KEY")
    if not url or not key:
        return None
    return create_client(url, key)

SUPABASE_BUCKET = st.secrets.get("SUPABASE_BUCKET", "yts-config")
supabase = get_supabase_client()

# ============================
# Supabase JSON I/O
# ============================
def _load_json(filename: str, default):
    """Supabase Storage에서 filename을 JSON으로 로드"""
    if supabase is None:
        return default
    try:
        res = supabase.storage.from_(SUPABASE_BUCKET).download(filename)
        if res is None:
            return default
        if isinstance(res, bytes):
            text = res.decode("utf-8")
        else:
            text = str(res)
        return json.loads(text)
    except Exception:
        return default


def _save_json(filename: str, data):
    """Supabase Storage에 filename을 JSON으로 저장 (upsert)"""
    if supabase is None:
        return
    try:
        payload = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
        supabase.storage.from_(SUPABASE_BUCKET).upload(
            path=filename,
            file=payload,
            file_options={"content-type": "application/json", "x-upsert": "true"},
        )
    except Exception as e:
        st.warning(f"Supabase 저장 오류({filename}): {e}")


CONFIG_PATH       = "yts_config.json"
KEYWORD_LOG_PATH  = "yts_keyword_log.json"
QUOTA_PATH        = "yts_quota_usage.json"

# ============================
# API 키 관리
# ============================
_DEFAULT_API_KEYS = [
    "YOUR_YT_API_KEY_1",
    "YOUR_YT_API_KEY_2",
]

def _load_api_keys_config():
    data = _load_json(CONFIG_PATH, {})
    keys = [k.strip() for k in data.get("api_keys", []) if k.strip()]
    if not keys:
        keys = _DEFAULT_API_KEYS[:]
    sel = data.get("selected_index", 0)
    sel = max(0, min(sel, len(keys)-1))
    return {"api_keys": keys, "selected_index": sel}

def _save_api_keys_config(keys: list[str], selected_index: int):
    keys = [k.strip() for k in keys if k.strip()]
    selected_index = max(0, min(selected_index, len(keys)-1)) if keys else 0
    _save_json(CONFIG_PATH, {"api_keys": keys, "selected_index": selected_index})

if "api_keys_state" not in st.session_state:
    cfg = _load_api_keys_config()
    st.session_state.api_keys_state = {
        "keys": cfg["api_keys"],
        "index": cfg["selected_index"],
    }

def _apply_env_key(key: str):
    if key:
        os.environ[ENV_KEY_NAME] = key
    else:
        os.environ.pop(ENV_KEY_NAME, None)

def get_current_api_key() -> str:
    keys = st.session_state.api_keys_state["keys"]
    idx  = st.session_state.api_keys_state["index"]
    if not keys:
        return ""
    return keys[idx]

def set_current_api_index(idx: int):
    keys = st.session_state.api_keys_state["keys"]
    if not keys:
        return
    idx = max(0, min(idx, len(keys)-1))
    st.session_state.api_keys_state["index"] = idx
    _apply_env_key(keys[idx])
    _save_api_keys_config(keys, idx)

def save_api_keys_from_user(text: str):
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    if not lines:
        return
    st.session_state.api_keys_state["keys"] = lines
    st.session_state.api_keys_state["index"] = 0
    _apply_env_key(lines[0])
    _save_api_keys_config(lines, 0)

# 첫 로드 시 환경변수 적용
_apply_env_key(get_current_api_key())

# ============================
# 쿼터 관리
# ============================
def _load_quota_map():
    return _load_json(QUOTA_PATH, {})

def _save_quota_map(data: dict):
    _save_json(QUOTA_PATH, data)

def quota_today_key():
    return datetime.now(KST).strftime("%Y-%m-%d")

def get_today_quota_total() -> int:
    data = _load_quota_map()
    return int(data.get(quota_today_key(), 0))

def add_quota_usage(units: int):
    if units <= 0:
        return
    data = _load_quota_map()
    key = quota_today_key()
    data[key] = int(data.get(key, 0)) + int(units)
    _save_quota_map(data)

# ============================
# 키워드 로그
# ============================
def _load_keyword_log():
    return _load_json(KEYWORD_LOG_PATH, [])

def _save_keyword_log(entries: list):
    _save_json(KEYWORD_LOG_PATH, entries)

def append_keyword_log(query: str):
    q = (query or "").strip()
    if not q:
        return
    entries = _load_keyword_log()
    now = datetime.now(KST).isoformat(timespec="seconds")
    entries.append({"ts": now, "q": q})
    _save_keyword_log(entries)

def get_recent_keywords(limit: int = 30):
    entries = _load_keyword_log()
    out = []
    for item in entries:
        ts = item.get("ts")
        q  = item.get("q")
        if not ts or not q:
            continue
        try:
            dt = datetime.fromisoformat(ts)
        except Exception:
            continue
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=KST)
        out.append((dt, q))
    out.sort(key=lambda x: x[0], reverse=True)
    return out[:limit]

# ============================
# 시간/유틸
# ============================
def format_k_datetime(dt_aw: datetime) -> str:
    if dt_aw.tzinfo is None:
        dt_aw = dt_aw.replace(tzinfo=KST)
    dt = dt_aw.astimezone(KST)
    wd = WEEKDAY_KO[dt.weekday()]
    h24 = dt.hour
    ampm = "오전" if h24 < 12 else "오후"
    h12 = h24 % 12 or 12
    return f"{dt.month}월{dt.day}일 {wd} {ampm}{h12}시 {dt.minute}분"

def parse_published_at_to_kst(published_iso: str) -> datetime:
    dt_utc = datetime.fromisoformat(published_iso.replace("Z", "+00:00"))
    return dt_utc.astimezone(KST)

def human_elapsed_days_hours(later: datetime, earlier: datetime) -> tuple[int, int]:
    delta = later - earlier
    if delta.total_seconds() < 0:
        return 0, 0
    days = delta.days
    hours = delta.seconds // 3600
    return days, hours

def published_after_from_label(label: str):
    label = label.strip()
    now_utc = datetime.now(timezone.utc)
    if label == "제한없음":
        return None
    if label.endswith("일"):
        days = int(label[:-1])
        dt = now_utc - timedelta(days=days)
    else:
        return None
    return dt.isoformat(timespec="seconds").replace("+00:00", "Z")

def parse_duration_iso8601(iso_dur: str) -> int:
    h = m = s = 0
    if not iso_dur or not iso_dur.startswith("PT"):
        return 0
    num = ""
    for ch in iso_dur[2:]:
        if ch.isdigit():
            num += ch
        else:
            if ch == "H" and num:
                h = int(num); num = ""
            elif ch == "M" and num:
                m = int(num); num = ""
            elif ch == "S" and num:
                s = int(num); num = ""
    return h*3600 + m*60 + s

def format_duration_hms(seconds: int) -> str:
    if seconds <= 0:
        return "0:00"
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    if h > 0:
        return f"{h}:{m:02d}:{s:02d}"
    return f"{m}:{s:02d}"

def duration_filter_ok(seconds: int, label: str) -> bool:
    if label == "전체": return True
    if label == "쇼츠": return seconds < 60
    if label == "롱폼": return seconds >= 60
    if label == "1~20분": return 60 <= seconds < 20*60
    if label == "20~40분": return 20*60 <= seconds < 40*60
    if label == "40~60분": return 40*60 <= seconds < 60*60
    if label == "60분이상": return seconds >= 60*60
    return True

def parse_min_views(text: str) -> int:
    digits = text.replace(",", "").replace(" ", "").replace("만", "0000")
    try:
        return int(digits)
    except Exception:
        return 0

# ============================
# YouTube 클라이언트
# ============================
def get_youtube_client():
    key = get_current_api_key()
    if not key:
        raise RuntimeError("YouTube API 키가 없습니다. 좌측에서 키를 입력해주세요.")
    try:
        return build("youtube", "v3", developerKey=key, cache_discovery=False)
    except TypeError:
        return build("youtube", "v3", developerKey=key)

# ============================
# YouTube 검색 함수
# ============================
def search_videos(
    query: str,
    min_views: int,
    api_period_label: str,
    duration_label: str,
    max_fetch: int,
    region_code: str | None,
    lang_code: str | None,
):
    youtube = get_youtube_client()
    published_after = published_after_from_label(api_period_label)

    cost_used = 0
    breakdown = {"search.list": 0, "videos.list": 0}
    max_fetch = max(1, min(int(max_fetch or 100), 5000))

    results_tmp = []
    next_token = None
    fetched = 0

    while fetched < max_fetch:
        take = min(50, max_fetch - fetched)
        kwargs = dict(
            q=query,
            part="id",
            type="video",
            maxResults=take,
        )
        if published_after:
            kwargs["publishedAfter"] = published_after
        if region_code:
            kwargs["regionCode"] = region_code
        if lang_code:
            kwargs["relevanceLanguage"] = lang_code
        if next_token:
            kwargs["pageToken"] = next_token

        try:
            search_response = youtube.search().list(**kwargs).execute()
            cost_used += 100; breakdown["search.list"] += 100
        except HttpError as e:
            raise RuntimeError(f"Search API 오류: {e}")

        page_ids = [
            it["id"]["videoId"]
            for it in search_response.get("items", [])
            if "id" in it and "videoId" in it["id"]
        ]
        if not page_ids:
            break

        try:
            video_response = youtube.videos().list(
                part="snippet,statistics,contentDetails",
                id=",".join(page_ids),
            ).execute()
            cost_used += 1; breakdown["videos.list"] += 1
        except HttpError as e:
            raise RuntimeError(f"Videos API 오류: {e}")

        items = video_response.get("items", [])
        for item in items:
            vid = item.get("id", "")
            snip = item.get("snippet", {}) or {}
            stats = item.get("statistics", {}) or {}
            cdet = item.get("contentDetails", {}) or {}

            title = snip.get("title", "")
            published_at_iso = snip.get("publishedAt", "")
            view_count = int(stats.get("viewCount", 0))
            url = f"https://www.youtube.com/watch?v={vid}"
            duration_sec = parse_duration_iso8601(cdet.get("duration", ""))

            if view_count < min_views:
                continue
            if not duration_filter_ok(duration_sec, duration_label):
                continue

            results_tmp.append({
                "title": title,
                "views": view_count,
                "published_at_iso": published_at_iso,
                "url": url,
                "duration_sec": duration_sec,
                "channel_title": snip.get("channelTitle", ""),
            })

        fetched += len(page_ids)
        next_token = search_response.get("nextPageToken")
        if not next_token:
            break

    return results_tmp, cost_used, breakdown

# ============================
# 점수/등급 계산
# ============================
def calc_grade(clicks_per_hour: int) -> str:
    v = clicks_per_hour
    if v >= 5000: return "S"
    if v >= 2000: return "A+"
    if v >= 1000: return "A"
    if v >= 500:  return "B"
    if v >= 300:  return "C"
    if v >= 100:  return "D"
    if v >= 50:   return "E"
    return "F"

# ============================
# UI - sidebar (모바일 압축)
# ============================
st.sidebar.header("⚙️ 설정 / 필터")

# --- API 키 설정 ---
with st.sidebar.expander("🔑 YouTube API 키", expanded=True):
    keys = st.session_state.api_keys_state["keys"]
    idx  = st.session_state.api_keys_state["index"]

    if keys:
        masked = [f"{i+1}. {k[:6]}...{k[-5:]}" for i,k in enumerate(keys)]
        sel = st.selectbox("사용할 키 선택", range(len(keys)), format_func=lambda i: masked[i], index=idx)
        if sel != idx:
            set_current_api_index(sel)

    key_text = st.text_area(
        "API 키들을 한 줄에 하나씩 입력 후 [저장] 클릭",
        value="\n".join(keys) if keys and keys != _DEFAULT_API_KEYS else "",
        height=80,
    )
    if st.button("API 키 저장", use_container_width=True):
        save_api_keys_from_user(key_text)
        st.success("API 키를 저장하고 1번 키를 활성화했습니다.")

# --- 검색 옵션 ---
st.sidebar.markdown("---")
query = st.sidebar.text_input("🔍 검색어", "")
channel_query = st.sidebar.text_input("📺 채널명 검색 (선택)", "")

api_period = st.sidebar.selectbox(
    "서버 검색기간",
    ["제한없음","90일","150일","365일","730일","1095일","1825일","3650일"],
    index=1,
)

upload_period = st.sidebar.selectbox(
    "업로드 기간(클라이언트 필터)",
    ["제한없음","1일","3일","7일","14일","30일","60일","90일","180일","365일"],
    index=6,
)

min_views_label = st.sidebar.selectbox(
    "최소 조회수",
    ["5,000","10,000","25,000","50,000","100,000","200,000","500,000","1,000,000"],
    index=0,
)

duration_label = st.sidebar.selectbox(
    "영상 길이",
    ["전체","쇼츠","롱폼","1~20분","20~40분","40~60분","60분이상"],
    index=0,
)

max_fetch = st.sidebar.number_input("가져올 최대 개수", 1, 5000, 50, step=10)

country_name = st.sidebar.selectbox("국가/언어", COUNTRY_LIST, index=0)
region_code, lang_code = COUNTRY_LANG_MAP[country_name]

# 최근 키워드
with st.sidebar.expander("⏱ 최근 검색 키워드", expanded=False):
    recents = get_recent_keywords(30)
    if not recents:
        st.write("최근 검색 없음")
    else:
        for dt, q in recents:
            st.write(f"- {dt.strftime('%m-%d %H:%M')} — `{q}`")

# ============================
# 메인 영역
# ============================
col_btn, col_quota = st.columns([2,1])

with col_btn:
    do_search = st.button("검색 실행", type="primary", use_container_width=True)
with col_quota:
    st.metric("오늘 사용한 쿼터", f"{get_today_quota_total():,} units")

status_placeholder = st.empty()

if "results_df" not in st.session_state:
    st.session_state.results_df = None
    st.session_state.last_search_time = None

# ============================
# 검색 실행
# ============================
def apply_client_filters(df: pd.DataFrame) -> pd.DataFrame:
    # 업로드 기간
    if upload_period != "제한없음":
        days = int(upload_period.replace("일",""))
        cutoff = datetime.now(KST) - timedelta(days=days)
        df = df[df["업로드시각"] >= cutoff]
    # 최소 조회수 (추가 필터)
    min_views = parse_min_views(min_views_label)
    df = df[df["영상조회수"] >= min_views]
    return df

if do_search:
    if not query.strip() and not channel_query.strip():
        st.warning("검색어 또는 채널명을 입력해주세요.")
    else:
        try:
            if query.strip():
                base_query = query.strip()
                append_keyword_log(base_query)
                status_placeholder.info("일반 검색 실행 중...")
                raw_results, cost_used, breakdown = search_videos(
                    query=base_query,
                    min_views=parse_min_views(min_views_label),
                    api_period_label=api_period,
                    duration_label=duration_label,
                    max_fetch=max_fetch,
                    region_code=region_code,
                    lang_code=lang_code,
                )
            else:
                st.warning("채널 전용 검색은 간단 버전에서 아직 지원하지 않습니다.\n일단 일반 검색만 사용해주세요.")
                raw_results, cost_used, breakdown = [], 0, {}

        except Exception as e:
            st.error(f"검색 중 오류: {e}")
            raw_results, cost_used, breakdown = [], 0, {}

        add_quota_usage(cost_used)

        if not raw_results:
            st.session_state.results_df = None
            status_placeholder.info("서버 결과 0건")
        else:
            search_dt = datetime.now(KST)
            rows = []
            for r in raw_results:
                pub_kst = parse_published_at_to_kst(r["published_at_iso"])
                d, h = human_elapsed_days_hours(search_dt, pub_kst)
                total_hours = max(1, d*24 + h)
                cph = int(round(r["views"] / total_hours))

                rows.append({
                    "채널명": r["channel_title"],
                    "등급": calc_grade(cph),
                    "영상조회수": r["views"],
                    "시간당클릭": cph,
                    "영상길이": format_duration_hms(r["duration_sec"]),
                    "업로드시각": pub_kst,
                    "경과시간": f"{d}일 {h}시간",
                    "제목": r["title"],
                    "URL": r["url"],
                })

            df = pd.DataFrame(rows)
            if not df.empty:
                df = apply_client_filters(df)

            st.session_state.results_df = df
            st.session_state.last_search_time = search_dt

            status_placeholder.success(f"서버 결과: {len(raw_results):,}건 / 필터 후: {len(df):,}건 "
                                       f"(이번 쿼터 사용량: {cost_used})")

# ============================
# 결과 표시
# ============================
df = st.session_state.results_df
if df is None or df.empty:
    st.info("아직 검색 결과가 없습니다. 좌측에서 조건을 설정하고 **[검색 실행]** 버튼을 눌러주세요.")
else:
    # URL 컬럼을 링크로 표시
    df_display = df.copy()
    df_display["링크"] = df_display["URL"].apply(lambda u: f"[열기]({u})")
    df_display = df_display.drop(columns=["URL"])

    st.subheader("📊 결과 리스트")
    st.dataframe(
        df_display,
        use_container_width=True,
        height=500,
    )

    st.caption("열기 링크를 누르면 새 탭에서 영상이 열립니다.")

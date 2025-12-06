#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
from datetime import datetime, timedelta, timezone

import streamlit as st
import pandas as pd

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from supabase import create_client, Client

# -----------------------------
# 기본 설정
# -----------------------------
st.set_page_config(
    page_title="youtube검색기",
    page_icon="🔍",
    layout="wide",
)

# 제목을 조금 작게
st.markdown("### youtube검색기")

# 여백 살짝 조정
st.markdown(
    """
    <style>
    .block-container { padding-top: 2rem !important; }
    </style>
    """,
    unsafe_allow_html=True,
)

# -----------------------------
# 공통 상수/환경
# -----------------------------
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

# -----------------------------
# Supabase 클라이언트
# -----------------------------
@st.cache_resource
def get_supabase_client() -> Client | None:
    url = st.secrets.get("SUPABASE_URL")
    key = st.secrets.get("SUPABASE_SERVICE_KEY")
    if not url or not key:
        return None
    return create_client(url, key)

SUPABASE_BUCKET = st.secrets.get("SUPABASE_BUCKET", "yts-config")
supabase = get_supabase_client()

def _load_json(filename: str, default):
    """Supabase Storage에서 JSON 로드"""
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
    """Supabase Storage에 JSON 저장 (upsert)"""
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

# -----------------------------
# 로그인 상태
# -----------------------------
LOGIN_ID_ENV = st.secrets.get("LOGIN_ID", "")
LOGIN_PW_ENV = st.secrets.get("LOGIN_PW", "")

if "logged_in" not in st.session_state:
    st.session_state.logged_in = False

def login_form():
    st.sidebar.subheader("로그인")
    with st.sidebar.form("login_form"):
        user_id = st.text_input("아이디", value="", key="login_id")
        user_pw = st.text_input("비밀번호", value="", type="password", key="login_pw")
        submitted = st.form_submit_button("로그인")
    if submitted:
        if (user_id == LOGIN_ID_ENV) and (user_pw == LOGIN_PW_ENV):
            st.session_state.logged_in = True
            st.success("로그인 성공!")
            st.experimental_rerun()
        else:
            st.error("아이디 또는 비밀번호가 잘못되었습니다.")

def logout_button():
    if st.sidebar.button("로그아웃", use_container_width=True):
        st.session_state.logged_in = False
        st.experimental_rerun()

# -----------------------------
# API 키 설정 (secrets 기반)
# -----------------------------
DEFAULT_KEYS_FROM_SECRETS = st.secrets.get("YOUTUBE_API_KEYS", [])
if isinstance(DEFAULT_KEYS_FROM_SECRETS, str):
    DEFAULT_KEYS_FROM_SECRETS = [DEFAULT_KEYS_FROM_SECRETS]
_DEFAULT_API_KEYS = list(DEFAULT_KEYS_FROM_SECRETS)

def _load_api_keys_config():
    data = _load_json(CONFIG_PATH, {})
    keys = [k.strip() for k in data.get("api_keys", []) if k.strip()]
    if not keys:
        keys = _DEFAULT_API_KEYS[:]
    sel = data.get("selected_index", 0)
    sel = max(0, min(sel, len(keys)-1)) if keys else 0
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

# 앱 시작 시 현재 인덱스 키를 환경변수에 반영
_apply_env_key(get_current_api_key())

# -----------------------------
# 쿼터 관리
# -----------------------------
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

# -----------------------------
# 키워드 로그
# -----------------------------
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

def get_recent_keywords_simple(limit: int = 7):
    """최근 키워드 문자열만 반환 (날짜 없이, 최신순)"""
    entries = _load_keyword_log()
    out = []
    for item in entries:
        q = item.get("q")
        if q:
            out.append(q)
    # 뒤에서부터 최근이므로 역순
    out = out[::-1]
    return out[:limit]

# -----------------------------
# 시간/포맷 유틸
# -----------------------------
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

# -----------------------------
# 등급 계산 (A~H로 변경)
# -----------------------------
def calc_grade(clicks_per_hour: int) -> str:
    v = clicks_per_hour
    if v >= 5000: return "A"
    if v >= 2000: return "B"
    if v >= 1000: return "C"
    if v >= 500:  return "D"
    if v >= 300:  return "E"
    if v >= 100:  return "F"
    if v >= 50:   return "G"
    return "H"

# -----------------------------
# YouTube Client
# -----------------------------
def get_youtube_client():
    key = get_current_api_key()
    if not key:
        raise RuntimeError("YouTube API 키가 없습니다. 관리자에게 문의하세요.")
    try:
        return build("youtube", "v3", developerKey=key, cache_discovery=False)
    except TypeError:
        return build("youtube", "v3", developerKey=key)

# -----------------------------
# 검색 함수들
# -----------------------------
def search_videos(
    query: str,
    min_views: int,
    api_period_label: str,
    duration_label: str,
    max_fetch: int,
    region_code: str | None,
    lang_code: str | None,
):
    """일반 키워드로 영상 검색"""
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

            thumbs = snip.get("thumbnails", {}) or {}
            thumb_url = (
                (thumbs.get("maxres") or {}).get("url") or
                (thumbs.get("standard") or {}).get("url") or
                (thumbs.get("high") or {}).get("url") or
                (thumbs.get("medium") or {}).get("url") or
                (thumbs.get("default") or {}).get("url") or
                ""
            )

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
                "thumb_url": thumb_url,
            })

        fetched += len(page_ids)
        next_token = search_response.get("nextPageToken")
        if not next_token:
            break

    return results_tmp, cost_used, breakdown

def search_trending_videos(
    max_fetch: int,
    region_code: str | None,
):
    """트렌드(인기 동영상) 검색 - chart=mostPopular"""
    youtube = get_youtube_client()
    max_fetch = max(1, min(int(max_fetch or 50), 50))  # YouTube 제한 50
    kwargs = dict(
        part="snippet,statistics,contentDetails",
        chart="mostPopular",
        maxResults=max_fetch,
    )
    if region_code:
        kwargs["regionCode"] = region_code

    try:
        resp = youtube.videos().list(**kwargs).execute()
        cost_used = 1
    except HttpError as e:
        raise RuntimeError(f"트렌드 검색 API 오류: {e}")

    results = []
    for item in resp.get("items", []):
        vid = item.get("id", "")
        snip = item.get("snippet", {}) or {}
        stats = item.get("statistics", {}) or {}
        cdet = item.get("contentDetails", {}) or {}

        title = snip.get("title", "")
        published_at_iso = snip.get("publishedAt", "")
        view_count = int(stats.get("viewCount", 0))
        url = f"https://www.youtube.com/watch?v={vid}"
        duration_sec = parse_duration_iso8601(cdet.get("duration", ""))

        thumbs = snip.get("thumbnails", {}) or {}
        thumb_url = (
            (thumbs.get("maxres") or {}).get("url") or
            (thumbs.get("standard") or {}).get("url") or
            (thumbs.get("high") or {}).get("url") or
            (thumbs.get("medium") or {}).get("url") or
            (thumbs.get("default") or {}).get("url") or
            ""
        )

        results.append({
            "title": title,
            "views": view_count,
            "published_at_iso": published_at_iso,
            "url": url,
            "duration_sec": duration_sec,
            "channel_title": snip.get("channelTitle", ""),
            "thumb_url": thumb_url,
        })

    return results, cost_used

def search_channels_by_keyword(
    keyword: str,
    max_results: int,
    region_code: str | None,
    lang_code: str | None,
):
    """키워드로 채널 찾기"""
    youtube = get_youtube_client()
    take = max(1, min(max_results, 50))
    kwargs = dict(
        q=keyword,
        part="id",
        type="channel",
        maxResults=take,
    )
    if region_code:
        kwargs["regionCode"] = region_code
    if lang_code:
        kwargs["relevanceLanguage"] = lang_code

    try:
        search_response = youtube.search().list(**kwargs).execute()
        cost_used = 100
    except HttpError as e:
        raise RuntimeError(f"Channel search API 오류: {e}")

    ch_ids = [
        it["id"]["channelId"]
        for it in search_response.get("items", [])
        if "id" in it and "channelId" in it["id"]
    ]
    if not ch_ids:
        return [], cost_used

    try:
        ch_resp = youtube.channels().list(
            part="snippet,statistics",
            id=",".join(ch_ids),
        ).execute()
        cost_used += 1
    except HttpError as e:
        raise RuntimeError(f"Channels API 오류: {e}")

    results = []
    for c in ch_resp.get("items", []):
        cid = c.get("id", "")
        sn = c.get("snippet", {}) or {}
        stt = c.get("statistics", {}) or {}
        subs = int(stt.get("subscriberCount", 0)) if stt.get("subscriberCount") is not None else None
        total_views = int(stt.get("viewCount", 0))
        videos = int(stt.get("videoCount", 0))
        url = f"https://www.youtube.com/channel/{cid}" if cid else ""

        thumbs = sn.get("thumbnails", {}) or {}
        thumb_url = (
            (thumbs.get("high") or {}).get("url") or
            (thumbs.get("medium") or {}).get("url") or
            (thumbs.get("default") or {}).get("url") or
            ""
        )

        results.append({
            "channel_title": sn.get("title", ""),
            "subs": subs,
            "total_views": total_views,
            "videos": videos,
            "url": url,
            "thumb_url": thumb_url,
        })

    results.sort(key=lambda r: (r["subs"] or 0), reverse=True)
    return results, cost_used

def search_videos_in_channel(
    channel_name: str,
    api_period_label: str,
    duration_label: str,
    max_fetch: int,
    region_code: str | None,
    lang_code: str | None,
):
    """채널 이름으로 채널을 찾고, 그 채널의 영상을 검색"""
    youtube = get_youtube_client()
    # 1) 채널 찾기
    kwargs_ch = dict(
        q=channel_name,
        part="id",
        type="channel",
        maxResults=1,
    )
    if region_code:
        kwargs_ch["regionCode"] = region_code
    if lang_code:
        kwargs_ch["relevanceLanguage"] = lang_code

    try:
        ch_resp = youtube.search().list(**kwargs_ch).execute()
        cost_used = 100
    except HttpError as e:
        raise RuntimeError(f"채널 검색 오류: {e}")

    items = ch_resp.get("items", [])
    if not items:
        return [], cost_used

    channel_id = items[0]["id"]["channelId"]
    published_after = published_after_from_label(api_period_label)

    # 2) 해당 채널의 영상 검색
    max_fetch = max(1, min(int(max_fetch or 100), 5000))
    results_tmp = []
    next_token = None
    fetched = 0

    while fetched < max_fetch:
        take = min(50, max_fetch - fetched)
        kwargs = dict(
            part="id",
            type="video",
            channelId=channel_id,
            maxResults=take,
            order="date",
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
            v_search = youtube.search().list(**kwargs).execute()
            cost_used += 100
        except HttpError as e:
            raise RuntimeError(f"채널 영상 검색 오류: {e}")

        page_ids = [
            it["id"]["videoId"]
            for it in v_search.get("items", [])
            if "id" in it and "videoId" in it["id"]
        ]
        if not page_ids:
            break

        try:
            video_response = youtube.videos().list(
                part="snippet,statistics,contentDetails",
                id=",".join(page_ids),
            ).execute()
            cost_used += 1
        except HttpError as e:
            raise RuntimeError(f"Videos API 오류: {e}")

        for item in video_response.get("items", []):
            vid = item.get("id", "")
            snip = item.get("snippet", {}) or {}
            stats = item.get("statistics", {}) or {}
            cdet = item.get("contentDetails", {}) or {}

            title = snip.get("title", "")
            published_at_iso = snip.get("publishedAt", "")
            view_count = int(stats.get("viewCount", 0))
            url = f"https://www.youtube.com/watch?v={vid}"
            duration_sec = parse_duration_iso8601(cdet.get("duration", ""))

            thumbs = snip.get("thumbnails", {}) or {}
            thumb_url = (
                (thumbs.get("maxres") or {}).get("url") or
                (thumbs.get("standard") or {}).get("url") or
                (thumbs.get("high") or {}).get("url") or
                (thumbs.get("medium") or {}).get("url") or
                (thumbs.get("default") or {}).get("url") or
                ""
            )

            if not duration_filter_ok(duration_sec, duration_label):
                continue

            results_tmp.append({
                "title": title,
                "views": view_count,
                "published_at_iso": published_at_iso,
                "url": url,
                "duration_sec": duration_sec,
                "channel_title": snip.get("channelTitle", ""),
                "thumb_url": thumb_url,
            })

        fetched += len(page_ids)
        next_token = v_search.get("nextPageToken")
        if not next_token:
            break

    return results_tmp, cost_used

# -----------------------------
# 세션 상태 초기화
# -----------------------------
if "results_df" not in st.session_state:
    st.session_state.results_df = None
    st.session_state.last_search_time = None
    st.session_state.search_mode = None   # "normal", "trend", "channel_videos", "channel_keyword"

# ============================================================
# 🔐 로그인 처리
# ============================================================
if not st.session_state.logged_in:
    login_form()
    st.stop()

# ============================================================
# 🧭 사이드바 UI
# ============================================================
# 1) 검색어 입력들
st.sidebar.subheader("검색")

query = st.sidebar.text_input("일반 검색어", "")
st.sidebar.markdown("---")

# 트렌드, 채널영상, 키워드채널 검색은 disclosure(접기) 방식
with st.sidebar.expander("트렌드 검색", expanded=False):
    use_trend = st.checkbox("트렌드 검색 실행", value=False, key="use_trend_flag")

with st.sidebar.expander("채널영상검색", expanded=False):
    channel_name_for_videos = st.text_input("채널 이름", key="channel_name_for_videos")

with st.sidebar.expander("키워드채널검색", expanded=False):
    channel_keyword = st.text_input("채널 키워드", key="channel_keyword_for_search")

st.sidebar.markdown("---")

# 2) 세부 필터
with st.sidebar.expander("세부 필터", expanded=False):
    api_period = st.selectbox(
        "서버 검색기간",
        ["제한없음","90일","150일","365일","730일","1095일","1825일","3650일"],
        index=1,
    )
    upload_period = st.selectbox(
        "업로드 기간(클라이언트 필터)",
        ["제한없음","1일","3일","7일","14일","30일","60일","90일","180일","365일"],
        index=6,
    )
    min_views_label = st.selectbox(
        "최소 조회수",
        ["5,000","10,000","25,000","50,000","100,000","200,000","500,000","1,000,000"],
        index=0,
    )
    duration_label = st.selectbox(
        "영상 길이",
        ["전체","쇼츠","롱폼","1~20분","20~40분","40~60분","60분이상"],
        index=0,
    )
    max_fetch = st.number_input("가져올 최대 개수", 1, 5000, 50, step=10)
    country_name = st.selectbox("국가/언어", COUNTRY_LIST, index=0)
    region_code, lang_code = COUNTRY_LANG_MAP[country_name]

# 3) 썸네일 / 뷰 옵션
st.sidebar.markdown("---")
show_thumbs = st.sidebar.checkbox("썸네일 보기", value=True, key="show_thumbnails")
grid_view = st.sidebar.checkbox("그리드 보기", value=False, key="grid_view")
shorts_view = st.sidebar.checkbox("쇼츠 보기", value=False, key="shorts_view")

# 4) 오늘 쿼터 (아래쪽에 작게)
st.sidebar.markdown("---")
st.sidebar.caption(f"오늘 사용한 쿼터: {get_today_quota_total():,} units")

# 5) 최근 키워드 (맨 아래 근처)
with st.sidebar.expander("최근 키워드", expanded=False):
    recent_qs = get_recent_keywords_simple(limit=7)
    if not recent_qs:
        st.caption("최근 검색 없음")
    else:
        for q in recent_qs:
            st.caption(f"- {q}")

# 로그아웃 버튼
logout_button()

# ============================================================
# 🔍 검색 실행 버튼 (일반검색용)
# ============================================================
col_left, col_right = st.columns([2, 1])

with col_left:
    do_search = st.button("검색 실행", type="primary", use_container_width=True)
with col_right:
    status_placeholder = st.empty()

# ============================================================
# 🧪 클라이언트 필터 함수
# ============================================================
def apply_client_filters(df: pd.DataFrame, upload_period: str, min_views_label: str) -> pd.DataFrame:
    # 업로드 기간
    if upload_period != "제한없음" and "업로드시각" in df.columns:
        days = int(upload_period.replace("일",""))
        cutoff = datetime.now(KST) - timedelta(days=days)
        df = df[df["업로드시각"] >= cutoff]
    # 최소 조회수
    min_views = parse_min_views(min_views_label)
    if "영상조회수" in df.columns:
        df = df[df["영상조회수"] >= min_views]
    return df

# ============================================================
# 🔁 검색 로직
# ============================================================
if do_search:
    base_query = (query or "").strip()
    ch_keyword = (channel_keyword or "").strip()
    ch_name_for_videos = (channel_name_for_videos or "").strip()

    # 어떤 모드로 검색할지 우선순위:
    # 1) 트렌드 검색 체크
    # 2) 채널영상검색 (채널 이름)
    # 3) 키워드채널검색
    # 4) 일반 검색 (query)
    try:
        if st.session_state.get("use_trend_flag", False):
            # 트렌드 검색
            append_keyword_log("[trend]")
            status_placeholder.info("트렌드 검색 실행 중...")
            raw_results, cost_used = search_trending_videos(
                max_fetch=max_fetch,
                region_code=region_code,
            )
            search_dt = datetime.now(KST)
            rows = []
            for r in raw_results:
                pub_kst = parse_published_at_to_kst(r["published_at_iso"])
                d, h = human_elapsed_days_hours(search_dt, pub_kst)
                total_hours = max(1, d*24 + h)
                cph = int(round(r["views"] / total_hours)) if total_hours > 0 else 0
                rows.append({
                    "썸네일URL": r["thumb_url"],
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
                df = apply_client_filters(df, upload_period, min_views_label)
            st.session_state.results_df = df
            st.session_state.last_search_time = search_dt
            st.session_state.search_mode = "trend"
            status_placeholder.success(
                f"트렌드 서버 결과: {len(raw_results):,}건 / 필터 후: {len(df):,}건 (이번 쿼터 사용량: {cost_used})"
            )
            add_quota_usage(cost_used)

        elif ch_name_for_videos:
            # 채널영상검색
            append_keyword_log(f"[channel_videos]{ch_name_for_videos}")
            status_placeholder.info("채널 영상 검색 실행 중...")
            raw_results, cost_used = search_videos_in_channel(
                channel_name=ch_name_for_videos,
                api_period_label=api_period,
                duration_label=duration_label,
                max_fetch=max_fetch,
                region_code=region_code,
                lang_code=lang_code,
            )
            if not raw_results:
                st.session_state.results_df = None
                st.session_state.search_mode = "channel_videos"
                status_placeholder.info("채널 영상 결과 0건")
            else:
                search_dt = datetime.now(KST)
                rows = []
                for r in raw_results:
                    pub_kst = parse_published_at_to_kst(r["published_at_iso"])
                    d, h = human_elapsed_days_hours(search_dt, pub_kst)
                    total_hours = max(1, d*24 + h)
                    cph = int(round(r["views"] / total_hours)) if total_hours > 0 else 0
                    rows.append({
                        "썸네일URL": r["thumb_url"],
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
                    df = apply_client_filters(df, upload_period, min_views_label)
                st.session_state.results_df = df
                st.session_state.last_search_time = search_dt
                st.session_state.search_mode = "channel_videos"
                status_placeholder.success(
                    f"채널 영상 서버 결과: {len(raw_results):,}건 / 필터 후: {len(df):,}건 (이번 쿼터 사용량: {cost_used})"
                )
            add_quota_usage(cost_used)

        elif ch_keyword:
            # 키워드채널검색
            append_keyword_log(f"[channel]{ch_keyword}")
            status_placeholder.info("키워드로 채널 검색 중...")
            ch_results, cost_used = search_channels_by_keyword(
                keyword=ch_keyword,
                max_results=max_fetch,
                region_code=region_code,
                lang_code=lang_code,
            )
            rows = []
            for r in ch_results:
                subs = r["subs"]
                subs_text = f"{subs:,}" if isinstance(subs, int) else "-"
                rows.append({
                    "썸네일URL": r["thumb_url"],
                    "채널명": r["channel_title"],
                    "구독자수": subs_text,
                    "채널조회수": f"{r['total_views']:,}",
                    "채널영상수": f"{r['videos']:,}",
                    "URL": r["url"],
                })
            df = pd.DataFrame(rows)
            st.session_state.results_df = df
            st.session_state.last_search_time = datetime.now(KST)
            st.session_state.search_mode = "channel_keyword"
            status_placeholder.success(
                f"키워드 채널 결과: {len(ch_results):,}건 (이번 쿼터 사용량: {cost_used})"
            )
            add_quota_usage(cost_used)

        else:
            # 일반 검색
            if not base_query:
                st.warning("일반 검색어를 입력하거나, 다른 검색 모드를 선택해주세요.")
            else:
                append_keyword_log(base_query)
                status_placeholder.info("일반 영상 검색 실행 중...")
                raw_results, cost_used, breakdown = search_videos(
                    query=base_query,
                    min_views=parse_min_views(min_views_label),
                    api_period_label=api_period,
                    duration_label=duration_label,
                    max_fetch=max_fetch,
                    region_code=region_code,
                    lang_code=lang_code,
                )
                if not raw_results:
                    st.session_state.results_df = None
                    st.session_state.search_mode = "normal"
                    status_placeholder.info("서버 결과 0건")
                else:
                    search_dt = datetime.now(KST)
                    rows = []
                    for r in raw_results:
                        pub_kst = parse_published_at_to_kst(r["published_at_iso"])
                        d, h = human_elapsed_days_hours(search_dt, pub_kst)
                        total_hours = max(1, d*24 + h)
                        cph = int(round(r["views"] / total_hours)) if total_hours > 0 else 0
                        rows.append({
                            "썸네일URL": r["thumb_url"],
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
                        df = apply_client_filters(df, upload_period, min_views_label)
                    st.session_state.results_df = df
                    st.session_state.last_search_time = search_dt
                    st.session_state.search_mode = "normal"
                    status_placeholder.success(
                        f"일반 검색 서버 결과: {len(raw_results):,}건 / 필터 후: {len(df):,}건 (이번 쿼터 사용량: {cost_used})"
                    )
                add_quota_usage(cost_used)

    except Exception as e:
        st.error(f"검색 중 오류: {e}")
        st.session_state.results_df = None

# ============================================================
# 📊 결과 표시 (테이블 / 그리드 / 쇼츠)
# ============================================================
df = st.session_state.results_df
mode = st.session_state.search_mode

if df is None or df.empty:
    st.info("아직 검색 결과가 없습니다. 좌측에서 조건을 설정하고 **[검색 실행]** 버튼을 눌러주세요.")
else:
    # 어떤 뷰 모드인지 결정
    view_mode = "table"   # 기본: 테이블
    if show_thumbs:
        if shorts_view:
            view_mode = "shorts"
        elif grid_view:
            view_mode = "grid"

    # 모드별 제목
    if mode == "normal":
        title_text = "📊 일반 검색 결과 리스트"
    elif mode == "trend":
        title_text = "📊 트렌드 검색 결과 리스트"
    elif mode == "channel_videos":
        title_text = "📊 채널 영상 리스트"
    elif mode == "channel_keyword":
        title_text = "📊 채널검색 리스트"
    else:
        title_text = "📊 결과 리스트"

    st.subheader(title_text)

    # ===== 1) 테이블 뷰 =====
    if view_mode == "table":
        df_display = df.copy()

        # 썸네일URL은 테이블 모드에서는 일단 숨기고 싶다면 drop 가능
        if "썸네일URL" in df_display.columns:
            df_display = df_display.drop(columns=["썸네일URL"])

        # URL 컬럼을 그대로 두어 모바일에서도 링크 탭으로 이동 가능
        # 편집은 안되도록 dataframe 사용
        st.dataframe(
            df_display,
            use_container_width=True,
        )
        st.caption("URL 컬럼을 클릭하면 새 탭에서 영상 또는 채널을 열 수 있습니다.")

    # ===== 2) 그리드 뷰 =====
    elif view_mode == "grid":
        # 영상/채널 공통
        if "썸네일URL" not in df.columns:
            st.warning("썸네일 정보가 없습니다. 테이블 보기로 확인해주세요.")
        else:
            # 영상/채널에 따라 설명 텍스트 다르게 구성
            if mode in ("normal", "trend", "channel_videos"):
                st.caption("카드를 눌러도 아무 동작도 하지 않고, 아래 링크를 통해 열 수 있습니다.")
                cols_per_row = 3
                cols = st.columns(cols_per_row)
                for i, row in df.iterrows():
                    thumb = row.get("썸네일URL", "")
                    title = row.get("제목", "")
                    ch = row.get("채널명", "")
                    url = row.get("URL", "")
                    views = row.get("영상조회수", "")
                    grade = row.get("등급", "")
                    with cols[i % cols_per_row]:
                        if thumb:
                            st.image(thumb, use_column_width=True)
                        if title:
                            st.markdown(f"**{title}**")
                        if ch:
                            st.caption(f"채널: {ch}")
                        extra = []
                        if grade:
                            extra.append(f"등급 {grade}")
                        if isinstance(views, (int, float)):
                            extra.append(f"조회수 {int(views):,}")
                        elif isinstance(views, str) and views:
                            extra.append(f"조회수 {views}")
                        if extra:
                            st.caption(" · ".join(extra))
                        if url:
                            st.markdown(f"[열기]({url})")
            else:
                # 키워드채널검색: 채널 프로필 썸네일
                st.caption("키워드로 찾은 채널들입니다. 카드를 눌러도 아무 동작도 하지 않고, 아래 링크를 통해 열 수 있습니다.")
                cols_per_row = 3
                cols = st.columns(cols_per_row)
                for i, row in df.iterrows():
                    thumb = row.get("썸네일URL", "")
                    name = row.get("채널명", "")
                    subs = row.get("구독자수", "")
                    total_views = row.get("채널조회수", "")
                    videos = row.get("채널영상수", "")
                    url = row.get("URL", "")
                    with cols[i % cols_per_row]:
                        if thumb:
                            st.image(thumb, use_column_width=True)
                        if name:
                            st.markdown(f"**{name}**")
                        detail = []
                        if subs: detail.append(f"구독자 {subs}")
                        if total_views: detail.append(f"조회수 {total_views}")
                        if videos: detail.append(f"영상 {videos}")
                        if detail:
                            st.caption(" · ".join(detail))
                        if url:
                            st.markdown(f"[채널 열기]({url})")

    # ===== 3) 쇼츠 뷰 =====
    elif view_mode == "shorts":
        # 쇼츠 느낌: 더 많은 썸네일을 한 화면에 (4열 정도)
        if "썸네일URL" not in df.columns:
            st.warning("썸네일 정보가 없습니다. 테이블 보기로 확인해주세요.")
        else:
            if mode in ("normal", "trend", "channel_videos"):
                st.caption("쇼츠 보기: 세로 스크롤로 많은 영상을 한 번에 훑어보는 레이아웃입니다.")
                cols_per_row = 4
                cols = st.columns(cols_per_row)
                for i, row in df.iterrows():
                    thumb = row.get("썸네일URL", "")
                    title = row.get("제목", "")
                    url = row.get("URL", "")
                    with cols[i % cols_per_row]:
                        if thumb:
                            # 실제 썸네일은 16:9 이지만, 모바일에서도 촘촘히 보이도록 폭만 맞춰서 표시
                            st.image(thumb, use_column_width=True)
                        # 제목은 1~2줄 정도만 보이도록 짧게
                        if title:
                            short_title = title if len(title) <= 40 else title[:37] + "..."
                            st.caption(short_title)
                        if url:
                            st.markdown(f"[열기]({url})")
            else:
                # 키워드채널검색의 쇼츠뷰: 채널 프로필 그리드
                st.caption("키워드로 찾은 채널들의 쇼츠형 그리드입니다.")
                cols_per_row = 4
                cols = st.columns(cols_per_row)
                for i, row in df.iterrows():
                    thumb = row.get("썸네일URL", "")
                    name = row.get("채널명", "")
                    url = row.get("URL", "")
                    with cols[i % cols_per_row]:
                        if thumb:
                            st.image(thumb, use_column_width=True)
                        if name:
                            short_name = name if len(name) <= 24 else name[:21] + "..."
                            st.caption(short_name)
                        if url:
                            st.markdown(f"[채널 열기]({url})")

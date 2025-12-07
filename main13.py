#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import random
from datetime import datetime, timedelta, timezone

import streamlit as st
import pandas as pd

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from supabase import create_client, Client

# -----------------------------
# Streamlit 기본 설정
# -----------------------------
st.set_page_config(
    page_title="YouTube검색기",
    page_icon="🔍",
    layout="wide",
)

# 상단 여백 조정 + DataEditor 아이콘(3점 메뉴 등) 숨기기
st.markdown(
    """
    <style>
    .block-container { padding-top: 3rem !important; }

    /* DataEditor 내 아이콘 버튼(3점 메뉴 등) 숨기기 */
    [data-testid="stDataFrame"] button[kind="icon"] {
        display: none !important;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# -----------------------------
# 로그인 설정
# -----------------------------
LOGIN_ID_ENV = os.getenv("LOGIN_ID", "")
LOGIN_PW_ENV = os.getenv("LOGIN_PW", "")

st.session_state.setdefault("logged_in", False)
st.session_state.setdefault("login_id", LOGIN_ID_ENV or "")
st.session_state.setdefault("login_pw", LOGIN_PW_ENV or "")

def check_login(id_text: str, pw_text: str) -> bool:
    if LOGIN_ID_ENV and LOGIN_PW_ENV:
        return (id_text == LOGIN_ID_ENV) and (pw_text == LOGIN_PW_ENV)
    return True  # 환경변수 없으면 개발용으로 통과

if not st.session_state["logged_in"]:
    st.markdown("### 🔒 YouTube검색기 로그인")

    id_input = st.text_input("로그인 ID", value=st.session_state["login_id"])
    pw_input = st.text_input("비밀번호", type="password", value=st.session_state["login_pw"])

    col_l, _ = st.columns([1, 3])
    with col_l:
        login_btn = st.button("로그인", type="primary", use_container_width=True)

    if login_btn:
        st.session_state["login_id"] = id_input
        st.session_state["login_pw"] = pw_input
        if check_login(id_input, pw_input):
            st.session_state["logged_in"] = True
            st.success("로그인 성공!")
            st.rerun()
        else:
            st.error("로그인 실패. ID 또는 비밀번호를 확인해주세요.")

    st.stop()

# 로그인 이후 화면 제목 (작게)
st.markdown("### 🔍 YouTube검색기")

# -----------------------------
# 공통 상수/환경
# -----------------------------
KST = timezone(timedelta(hours=9))
WEEKDAY_KO = ["월요일","화요일","수요일","목요일","금요일","토요일","일요일"]

# YouTube 수익 상위권 20개국(대략적인 순서)
COUNTRY_LANG_MAP = {
    "미국": ("US", "en"),
    "영국": ("GB", "en"),
    "한국": ("KR", "ko"),
    "일본": ("JP", "ja"),
    "인도": ("IN", "en"),
    "브라질": ("BR", "pt"),
    "캐나다": ("CA", "en"),
    "독일": ("DE", "de"),
    "프랑스": ("FR", "fr"),
    "멕시코": ("MX", "es"),
    "호주": ("AU", "en"),
    "스페인": ("ES", "es"),
    "이탈리아": ("IT", "it"),
    "네덜란드": ("NL", "nl"),
    "터키": ("TR", "tr"),
    "인도네시아": ("ID", "id"),
    "태국": ("TH", "th"),
    "사우디아라비아": ("SA", "ar"),
    "아랍에미리트": ("AE", "ar"),
}

COUNTRY_LIST = list(COUNTRY_LANG_MAP.keys())

# 트렌드용 카테고리 (videos.list videoCategoryId)
TREND_CATEGORIES = {
    "전체": None,
    "음악": "10",
    "게임": "20",
    "스포츠": "17",
    "엔터테인먼트": "24",
    "뉴스/시사": "25",
    "사람/블로그": "22",
    "코미디": "23",
    "교육": "27",
    "과학/기술": "28",
}

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

# -----------------------------
# 경로/파일명 정의 (Supabase용)
# -----------------------------
KEYWORD_LOG_PATH  = "yts_keyword_log.json"
QUOTA_PATH        = "yts_quota_usage.json"

# -----------------------------
# API 키: secrets에서만 사용
# -----------------------------
def get_current_api_key() -> str:
    keys = st.secrets.get("YOUTUBE_API_KEYS")
    if isinstance(keys, list) and keys:
        return str(keys[0]).strip()
    if isinstance(keys, str) and keys.strip():
        first = keys.strip().splitlines()[0]
        return first.strip()

    single = st.secrets.get("YOUTUBE_API_KEY")
    if isinstance(single, str) and single.strip():
        return single.strip()

    return ""

def get_youtube_client():
    key = get_current_api_key()
    if not key:
        raise RuntimeError(
            "YouTube API 키가 없습니다.\n"
            "▶ .streamlit/secrets.toml 에 YOUTUBE_API_KEYS 또는 YOUTUBE_API_KEY 를 설정하세요."
        )
    try:
        return build("youtube", "v3", developerKey=key, cache_discovery=False)
    except TypeError:
        return build("youtube", "v3", developerKey=key)

# -----------------------------
# 쿼터 기록
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
# 최근 검색 키워드
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

# -----------------------------
# 시간/형식 유틸
# -----------------------------
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
# 등급 계산 (시간당 클릭수 기준, A~H)
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

GRADE_ORDER = {"A": 1, "B": 2, "C": 3, "D": 4, "E": 5, "F": 6, "G": 7, "H": 8}

# -----------------------------
# YouTube API 호출 함수들
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
                (thumbs.get("maxres") or {}).get("url")
                or (thumbs.get("standard") or {}).get("url")
                or (thumbs.get("high") or {}).get("url")
                or (thumbs.get("medium") or {}).get("url")
                or (thumbs.get("default") or {}).get("url")
                or ""
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
                "thumbnail_url": thumb_url,
            })

        fetched += len(page_ids)
        next_token = search_response.get("nextPageToken")
        if not next_token:
            break

    return results_tmp, cost_used, breakdown

def search_channels_by_keyword(
    keyword: str,
    max_results: int,
    region_code: str | None,
    lang_code: str | None,
):
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
        return [], cost_used, {"search.list": 100, "channels.list": 0}

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
        subs = int(stt.get("subscriberCount", 0)) if stt.get("subscriberCount") is not None else 0
        total_views = int(stt.get("viewCount", 0))
        videos = int(stt.get("videoCount", 0))
        url = f"https://www.youtube.com/channel/{cid}" if cid else ""

        thumbs = sn.get("thumbnails", {}) or {}
        thumb_url = (
            (thumbs.get("high") or {}).get("url")
            or (thumbs.get("medium") or {}).get("url")
            or (thumbs.get("default") or {}).get("url")
            or ""
        )

        results.append({
            "channel_title": sn.get("title", ""),
            "subs": subs,
            "total_views": total_views,
            "videos": videos,
            "url": url,
            "thumbnail_url": thumb_url,
        })

    results.sort(key=lambda r: r["subs"], reverse=True)
    return results, cost_used, {"search.list": 100, "channels.list": 1}

def search_videos_in_channel_by_name(
    channel_name: str,
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

    # 1) 채널 ID 찾기
    kwargs_ch = dict(
        q=channel_name,
        part="id,snippet",
        type="channel",
        maxResults=1,
    )
    if region_code:
        kwargs_ch["regionCode"] = region_code
    if lang_code:
        kwargs_ch["relevanceLanguage"] = lang_code

    try:
        ch_resp = youtube.search().list(**kwargs_ch).execute()
        cost_used += 100; breakdown["search.list"] += 100
    except HttpError as e:
        raise RuntimeError(f"채널 검색 오류: {e}")

    items = ch_resp.get("items", [])
    if not items:
        return [], cost_used, breakdown

    channel_id = items[0]["id"]["channelId"]

    # 2) 해당 채널 영상 검색
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
            cost_used += 100; breakdown["search.list"] += 100
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
            cost_used += 1; breakdown["videos.list"] += 1
        except HttpError as e:
            raise RuntimeError(f"Videos API 오류: {e}")

        items2 = video_response.get("items", [])
        for item in items2:
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
                (thumbs.get("maxres") or {}).get("url")
                or (thumbs.get("standard") or {}).get("url")
                or (thumbs.get("high") or {}).get("url")
                or (thumbs.get("medium") or {}).get("url")
                or (thumbs.get("default") or {}).get("url")
                or ""
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
                "thumbnail_url": thumb_url,
            })

        fetched += len(page_ids)
        next_token = v_search.get("nextPageToken")
        if not next_token:
            break

    return results_tmp, cost_used, breakdown

def search_trending_videos(
    max_results: int,
    region_code: str | None,
    video_category_id: str | None,
):
    youtube = get_youtube_client()
    take = max(1, min(max_results, 50))

    base_kwargs = dict(
        part="snippet,statistics,contentDetails",
        chart="mostPopular",
        maxResults=take,
    )
    if region_code:
        base_kwargs["regionCode"] = region_code

    kwargs = base_kwargs.copy()
    if video_category_id:
        kwargs["videoCategoryId"] = video_category_id

    try:
        resp = youtube.videos().list(**kwargs).execute()
        cost_used = 1
    except HttpError as e:
        if e.resp.status == 404 and video_category_id:
            resp = youtube.videos().list(**base_kwargs).execute()
            cost_used = 1
        else:
            raise RuntimeError(f"트렌드 API 오류: {e}")

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
            (thumbs.get("maxres") or {}).get("url")
            or (thumbs.get("standard") or {}).get("url")
            or (thumbs.get("high") or {}).get("url")
            or (thumbs.get("medium") or {}).get("url")
            or (thumbs.get("default") or {}).get("url")
            or ""
        )

        results.append({
            "title": title,
            "views": view_count,
            "published_at_iso": published_at_iso,
            "url": url,
            "duration_sec": duration_sec,
            "channel_title": snip.get("channelTitle", ""),
            "thumbnail_url": thumb_url,
        })
    return results, cost_used, {"videos.list": 1}

# -----------------------------
# Sidebar UI
# -----------------------------
st.sidebar.subheader("검색")

# 랜덤 트렌드 검색
random_trend_clicked = st.sidebar.button("🎲 랜덤 트렌드 검색", use_container_width=True)

# 공통 검색어 입력
search_query = st.sidebar.text_input("검색어", "")

# 3가지 검색 버튼
btn_general = st.sidebar.button("일반검색", use_container_width=True)
btn_channel_videos = st.sidebar.button("채널영상검색", use_container_width=True)
btn_channel_keyword = st.sidebar.button("키워드채널검색", use_container_width=True)

st.sidebar.markdown("---")

# 트렌드 검색 (disclosure)
with st.sidebar.expander("🔥 트렌드 검색", expanded=False):
    trend_category_label = st.selectbox(
        "카테고리",
        list(TREND_CATEGORIES.keys()),
        index=0,
        key="trend_category_label",
    )
    btn_trend = st.button("트렌드 검색 실행", use_container_width=True, key="btn_trend")

st.sidebar.markdown("---")

# 보기 모드 (기본: 그리드 뷰)
view_mode = st.sidebar.radio(
    "보기 모드",
    ["그리드 뷰", "리스트 뷰", "쇼츠 뷰"],
    index=0,
    key="view_mode_radio",
)

# 정렬 방식
st.session_state.setdefault("sort_key", "등급")
st.session_state.setdefault("sort_order", "오름차순")

with st.sidebar.expander("정렬 방식", expanded=True):
    sort_options = [
        "등급",
        "영상조회수",
        "시간당클릭",
        "업로드시각",
        "채널명",
        "제목",
        "구독자수",
        "채널조회수",
        "채널영상수",
    ]
    default_index = sort_options.index(st.session_state["sort_key"]) if st.session_state["sort_key"] in sort_options else 0
    sort_key = st.selectbox("정렬 기준", sort_options, index=default_index)
    sort_order = st.selectbox("정렬 방향", ["오름차순", "내림차순"], index=0)
    st.session_state["sort_key"] = sort_key
    st.session_state["sort_order"] = sort_order

st.sidebar.markdown("---")

# 세부 필터 (항상 펼쳐진 상태)
with st.sidebar.expander("⚙ 세부 필터", expanded=True):
    api_period = st.selectbox(
        "서버 검색기간 (YouTube API)",
        ["제한없음","90일","150일","365일","730일","1095일","1825일","3650일"],
        index=1,
        key="api_period",
    )
    upload_period = st.selectbox(
        "업로드 기간(클라이언트 필터)",
        ["제한없음","1일","3일","7일","14일","30일","60일","90일","180일","365일"],
        index=6,
        key="upload_period",
    )
    min_views_label = st.selectbox(
        "최소 조회수",
        ["5,000","10,000","25,000","50,000","100,000","200,000","500,000","1,000,000"],
        index=0,
        key="min_views_label",
    )
    duration_label = st.selectbox(
        "영상 길이",
        ["전체","쇼츠","롱폼","1~20분","20~40분","40~60분","60분이상"],
        index=0,
        key="duration_label",
    )
    max_fetch = st.number_input(
        "모든 검색에서 가져올 최대 개수",
        1, 5000, 50, step=10,
        key="max_fetch",
    )
    country_name = st.selectbox("검색용 국가/언어", COUNTRY_LIST, index=0, key="country_for_search")
    region_code, lang_code = COUNTRY_LANG_MAP[country_name]

status_placeholder = st.empty()

# -----------------------------
# 세션 상태 초기화
# -----------------------------
if "results_df" not in st.session_state:
    st.session_state.results_df = None
    st.session_state.last_search_time = None
    st.session_state.search_mode = None

def apply_client_filters(df: pd.DataFrame, upload_period: str, min_views_label: str) -> pd.DataFrame:
    if upload_period != "제한없음" and "업로드시각" in df.columns:
        days = int(upload_period.replace("일",""))
        cutoff = datetime.now(KST) - timedelta(days=days)
        df = df[df["업로드시각"] >= cutoff]
    min_views = parse_min_views(min_views_label)
    if "영상조회수" in df.columns:
        df = df[df["영상조회수"] >= min_views]
    return df

def sort_results_df(df: pd.DataFrame, mode: str, sort_key: str, sort_order: str) -> pd.DataFrame:
    col = sort_key
    if col not in df.columns:
        return df
    ascending = (sort_order == "오름차순")
    if col == "등급":
        tmp = df.copy()
        tmp["_grade_order"] = tmp["등급"].map(GRADE_ORDER).fillna(999)
        tmp = tmp.sort_values("_grade_order", ascending=ascending, ignore_index=True)
        return tmp.drop(columns=["_grade_order"])
    try:
        return df.sort_values(col, ascending=ascending, ignore_index=True)
    except Exception:
        return df

# -----------------------------
# 검색 실행 로직
# -----------------------------
try:
    mode_triggered = None
    is_random_trend = False

    if random_trend_clicked:
        mode_triggered = "trend"
        is_random_trend = True
    elif btn_general:
        mode_triggered = "general"
    elif btn_channel_videos:
        mode_triggered = "channel_videos"
    elif btn_channel_keyword:
        mode_triggered = "channel_list"
    elif btn_trend:
        mode_triggered = "trend"

    if mode_triggered is not None:
        search_dt = datetime.now(KST)

        # 일반 검색
        if mode_triggered == "general":
            base_query = (search_query or "").strip()
            if not base_query:
                st.warning("검색어를 입력해주세요.")
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
                add_quota_usage(cost_used)

                if not raw_results:
                    st.session_state.results_df = None
                    st.session_state.search_mode = "general"
                    status_placeholder.info("서버 결과 0건")
                else:
                    rows = []
                    for r in raw_results:
                        pub_kst = parse_published_at_to_kst(r["published_at_iso"])
                        d, h = human_elapsed_days_hours(search_dt, pub_kst)
                        total_hours = max(1, d*24 + h)
                        cph = int(round(r["views"] / total_hours))
                        rows.append({
                            "썸네일": r.get("thumbnail_url", ""),
                            "채널명": r["channel_title"],
                            "등급": calc_grade(cph),
                            "영상조회수": r["views"],
                            "시간당클릭": cph,
                            "영상길이": format_duration_hms(r["duration_sec"]),
                            "업로드시각": pub_kst,
                            "경과시간": f"{d}일 {h}시간",
                            "제목": r["title"],
                            "링크URL": r["url"],
                        })
                    df = pd.DataFrame(rows)
                    if not df.empty:
                        df = apply_client_filters(df, upload_period, min_views_label)
                    st.session_state.results_df = df
                    st.session_state.last_search_time = search_dt
                    st.session_state.search_mode = "general"
                    status_placeholder.success(
                        f"[일반 검색] 서버 결과: {len(raw_results):,}건 / "
                        f"필터 후: {len(df):,}건 (이번 쿼터 사용량: {cost_used})"
                    )

        # 트렌드 검색 (일반 + 랜덤)
        elif mode_triggered == "trend":
            if is_random_trend:
                rand_country_name = random.choice(COUNTRY_LIST)
                rand_region_code, _ = COUNTRY_LANG_MAP[rand_country_name]
                cat_labels = [k for k in TREND_CATEGORIES.keys() if k != "전체"]
                rand_cat_label = random.choice(cat_labels)
                cat_id = TREND_CATEGORIES[rand_cat_label]
                append_keyword_log(f"[trend-random]{rand_country_name}/{rand_cat_label}")
                status_placeholder.info(f"랜덤 트렌드 검색 중... ({rand_country_name} · {rand_cat_label})")
                trend_region_code = rand_region_code
            else:
                trend_region_code = region_code
                cat_label = trend_category_label
                cat_id = TREND_CATEGORIES.get(cat_label)
                append_keyword_log(f"[trend]{country_name}/{cat_label}")
                status_placeholder.info("트렌드 검색 실행 중...")

            raw_results, cost_used, breakdown = search_trending_videos(
                max_results=max_fetch,
                region_code=trend_region_code,
                video_category_id=cat_id,
            )
            add_quota_usage(cost_used)

            if not raw_results:
                st.session_state.results_df = None
                st.session_state.search_mode = "trend"
                status_placeholder.info("트렌드 결과 0건")
            else:
                rows = []
                for r in raw_results:
                    pub_kst = parse_published_at_to_kst(r["published_at_iso"])
                    d, h = human_elapsed_days_hours(search_dt, pub_kst)
                    total_hours = max(1, d*24 + h)
                    cph = int(round(r["views"] / total_hours))
                    rows.append({
                        "썸네일": r.get("thumbnail_url", ""),
                        "채널명": r["channel_title"],
                        "등급": calc_grade(cph),
                        "영상조회수": r["views"],
                        "시간당클릭": cph,
                        "영상길이": format_duration_hms(r["duration_sec"]),
                        "업로드시각": pub_kst,
                        "경과시간": f"{d}일 {h}시간",
                        "제목": r["title"],
                        "링크URL": r["url"],
                    })
                df = pd.DataFrame(rows)
                if not df.empty:
                    df = apply_client_filters(df, upload_period, min_views_label)
                st.session_state.results_df = df
                st.session_state.last_search_time = search_dt
                st.session_state.search_mode = "trend"
                status_placeholder.success(
                    f"[트렌드] 결과: {len(df):,}건 (이번 쿼터 사용량: {cost_used})"
                )

        # 채널 영상 검색
        elif mode_triggered == "channel_videos":
            ch_name = (search_query or "").strip()
            if not ch_name:
                st.warning("검색어에 채널 이름을 입력해주세요.")
            else:
                append_keyword_log(f"[channel_videos]{ch_name}")
                status_placeholder.info("채널 영상 검색 실행 중...")
                raw_results, cost_used, breakdown = search_videos_in_channel_by_name(
                    channel_name=ch_name,
                    min_views=parse_min_views(min_views_label),
                    api_period_label=api_period,
                    duration_label=duration_label,
                    max_fetch=max_fetch,
                    region_code=region_code,
                    lang_code=lang_code,
                )
                add_quota_usage(cost_used)

                if not raw_results:
                    st.session_state.results_df = None
                    st.session_state.search_mode = "channel_videos"
                    status_placeholder.info("채널 영상 결과 0건")
                else:
                    rows = []
                    for r in raw_results:
                        pub_kst = parse_published_at_to_kst(r["published_at_iso"])
                        d, h = human_elapsed_days_hours(search_dt, pub_kst)
                        total_hours = max(1, d*24 + h)
                        cph = int(round(r["views"] / total_hours))
                        rows.append({
                            "썸네일": r.get("thumbnail_url", ""),
                            "채널명": r["channel_title"],
                            "등급": calc_grade(cph),
                            "영상조회수": r["views"],
                            "시간당클릭": cph,
                            "영상길이": format_duration_hms(r["duration_sec"]),
                            "업로드시각": pub_kst,
                            "경과시간": f"{d}일 {h}시간",
                            "제목": r["title"],
                            "링크URL": r["url"],
                        })
                    df = pd.DataFrame(rows)
                    if not df.empty:
                        df = apply_client_filters(df, upload_period, min_views_label)
                    st.session_state.results_df = df
                    st.session_state.last_search_time = search_dt
                    st.session_state.search_mode = "channel_videos"
                    status_placeholder.success(
                        f"[채널 영상] 서버 결과: {len(raw_results):,}건 / "
                        f"필터 후: {len(df):,}건 (이번 쿼터 사용량: {cost_used})"
                    )

        # 키워드 채널 검색
        elif mode_triggered == "channel_list":
            ch_kw = (search_query or "").strip()
            if not ch_kw:
                st.warning("검색어에 채널 키워드를 입력해주세요.")
            else:
                append_keyword_log(f"[channel]{ch_kw}")
                status_placeholder.info("채널 목록 검색 실행 중...")
                ch_results, cost_used, breakdown = search_channels_by_keyword(
                    keyword=ch_kw,
                    max_results=max_fetch,
                    region_code=region_code,
                    lang_code=lang_code,
                )
                add_quota_usage(cost_used)

                if not ch_results:
                    st.session_state.results_df = None
                    st.session_state.search_mode = "channel_list"
                    status_placeholder.info("채널 결과 0건")
                else:
                    df_rows = []
                    for r in ch_results:
                        df_rows.append({
                            "썸네일": r.get("thumbnail_url", ""),
                            "채널명": r["channel_title"],
                            "구독자수": r["subs"],
                            "채널조회수": r["total_views"],
                            "채널영상수": r["videos"],
                            "링크URL": r["url"],
                        })
                    df = pd.DataFrame(df_rows)
                    st.session_state.results_df = df
                    st.session_state.last_search_time = search_dt
                    st.session_state.search_mode = "channel_list"
                    status_placeholder.success(
                        f"[채널 목록] 결과: {len(df):,}건 (이번 쿼터 사용량: {cost_used})"
                    )

except Exception as e:
    st.error(f"검색 중 오류: {e}")
    st.session_state.results_df = None

# -----------------------------
# 결과 표시 (뷰 모드별)
# -----------------------------
df = st.session_state.results_df
mode = st.session_state.search_mode

if df is None or df.empty:
    st.info("아직 검색 결과가 없습니다. 좌측에서 조건을 설정하고 버튼을 눌러 검색하세요.")
else:
    df_display = df.copy()

    if "링크URL" in df_display.columns:
        df_display["링크"] = df_display["링크URL"]
        df_display = df_display.drop(columns=["링크URL"])

    if mode == "general":
        st.subheader("📊 일반 검색 결과 리스트")
    elif mode == "trend":
        st.subheader("🔥 트렌드 검색 결과 리스트")
    elif mode == "channel_videos":
        st.subheader("🎬 채널 영상 리스트")
    elif mode == "channel_list":
        st.subheader("📺 채널검색 리스트")
    else:
        st.subheader("📊 검색 결과 리스트")

    df_display = sort_results_df(df_display, mode, sort_key, sort_order)

    # ---------- 쇼츠 뷰 ----------
    if view_mode == "쇼츠 뷰":
        if mode == "channel_list":
            # 채널 아이콘 4열 그리드 (100x100 느낌)
            n_cols = 4
            cols = st.columns(n_cols)
            for idx, (_, row) in enumerate(df_display.iterrows()):
                col = cols[idx % n_cols]
                with col:
                    thumb = row.get("썸네일", "")
                    title = row.get("채널명", "")
                    link = row.get("링크", "")
                    if thumb:
                        st.image(thumb, width=100)
                    st.caption(title[:12])
                    if link:
                        st.markdown(f"[채널 열기]({link})")
                if (idx + 1) % n_cols == 0 and (idx + 1) < len(df_display):
                    cols = st.columns(n_cols)
        else:
            # 영상 쇼츠 뷰: 4열로 더 촘촘하게 배치
            n_cols = 4
            cols = st.columns(n_cols)
            for idx, (_, row) in enumerate(df_display.iterrows()):
                col = cols[idx % n_cols]
                with col:
                    thumb = row.get("썸네일", "")
                    link = row.get("링크", "")
                    if thumb:
                        # 가로 150px 정도의 세로 이미지
                        st.image(thumb, width=150)
                    if link:
                        st.markdown(f"[열기]({link})", help="새 탭에서 열기")
                if (idx + 1) % n_cols == 0 and (idx + 1) < len(df_display):
                    cols = st.columns(n_cols)

        st.caption("👉 이미지를 눌러 새 탭에서 영상(또는 채널)을 열 수 있습니다.")

    # ---------- 그리드 뷰 ----------
    elif view_mode == "그리드 뷰":
        if mode == "channel_list":
            n_cols = 3
        else:
            n_cols = 3

        cols = st.columns(n_cols)

        for idx, (_, row) in enumerate(df_display.iterrows()):
            col = cols[idx % n_cols]
            with col:
                if "썸네일" in df_display.columns and isinstance(row["썸네일"], str) and row["썸네일"]:
                    st.image(row["썸네일"], use_column_width=True)
                if mode == "channel_list":
                    title = row.get("채널명", "")
                    subs = row.get("구독자수", 0)
                    total_views = row.get("채널조회수", 0)
                    video_count = row.get("채널영상수", 0)
                    link = row.get("링크", "")
                    st.markdown(f"**{title}**")
                    st.caption(f"구독자: {subs:,} · 조회수: {total_views:,} · 영상수: {video_count:,}")
                    if link:
                        st.markdown(f"[채널 열기]({link})")
                else:
                    title = row.get("제목", "")
                    ch = row.get("채널명", "")
                    views = row.get("영상조회수", 0)
                    grade = row.get("등급", "")
                    link = row.get("링크", "")
                    st.markdown(f"**{title}**")
                    st.caption(f"{ch} · 조회수 {views:,} · 등급 {grade}")
                    if link:
                        st.markdown(f"[영상 열기]({link})")

            if (idx + 1) % n_cols == 0 and (idx + 1) < len(df_display):
                cols = st.columns(n_cols)

        st.caption("👉 카드를 클릭해서 링크를 열 수 있습니다.")

    # ---------- 리스트 뷰 ----------
    else:  # "리스트 뷰"
        if mode in ("general", "trend", "channel_videos"):
            base_order = [
                "등급",
                "썸네일",
                "채널명",
                "영상조회수",
                "시간당클릭",
                "영상길이",
                "업로드시각",
                "경과시간",
                "제목",
                "링크",
            ]
        else:
            base_order = [
                "썸네일",
                "채널명",
                "구독자수",
                "채널조회수",
                "채널영상수",
                "링크",
            ]
        column_order = [c for c in base_order if c in df_display.columns]

        column_config = {}
        if "링크" in df_display.columns:
            column_config["링크"] = st.column_config.LinkColumn(
                "열기",
                display_text="열기",
            )
        if "썸네일" in df_display.columns:
            column_config["썸네일"] = st.column_config.ImageColumn(
                "썸네일",
                help="썸네일 이미지",
                width="small",
            )

        if mode == "general":
            editor_key = "video_results_editor_general"
        elif mode == "trend":
            editor_key = "video_results_editor_trend"
        elif mode == "channel_videos":
            editor_key = "video_results_editor_channel_videos"
        elif mode == "channel_list":
            editor_key = "channel_results_editor_keyword"
        else:
            editor_key = "results_editor_default"

        st.data_editor(
            df_display,
            use_container_width=True,
            height=680,
            hide_index=True,
            column_order=column_order if column_order else None,
            column_config=column_config,
            key=editor_key,
            disabled=True,
            num_rows="fixed",
        )

        st.caption("👉 '열기' 링크를 누르면 새 탭에서 영상 또는 채널이 열립니다.")

# -----------------------------
# 사이드바 하단: 쿼터 / 최근 키워드 / 로그아웃
# -----------------------------
st.sidebar.markdown("---")

quota_today = get_today_quota_total()
st.sidebar.caption(f"오늘 사용 쿼터: {quota_today:,} units")

recents = get_recent_keywords(7)
if recents:
    keywords = [q for _, q in recents]
    labels = [f"`{k}`" for k in keywords]
    st.sidebar.caption("최근 키워드: " + " · ".join(labels))
else:
    st.sidebar.caption("최근 키워드: 없음")

if st.sidebar.button("로그아웃", use_container_width=True):
    st.session_state["logged_in"] = False
    st.rerun()

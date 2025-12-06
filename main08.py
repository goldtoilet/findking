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
# Streamlit 기본 설정
# -----------------------------
st.set_page_config(
    page_title="YouTube검색기",
    page_icon="🔍",
    layout="wide",
)

# 상단 여백 조정 + DataEditor 아이콘(3점 메뉴 등) 숨기기 (가능한 선에서)
st.markdown(
    """
    <style>
    .block-container { padding-top: 3rem !important; }
    /* DataEditor 내 아이콘 버튼(3점 메뉴 등) 숨기기 시도 */
    [data-testid="stDataEditor"] button[kind="icon"] {
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
    # 환경변수에 값이 있으면 그것과 비교
    if LOGIN_ID_ENV and LOGIN_PW_ENV:
        return (id_text == LOGIN_ID_ENV) and (pw_text == LOGIN_PW_ENV)
    # 환경변수가 비어 있으면 그냥 통과(개발용)
    return True

if not st.session_state["logged_in"]:
    st.markdown("### 🔒 YouTube검색기 로그인")

    id_input = st.text_input("로그인 ID", value=st.session_state["login_id"])
    pw_input = st.text_input("비밀번호", type="password", value=st.session_state["login_pw"])

    col_l, col_r = st.columns([1, 3])
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
    """
    우선순위:
    1) YOUTUBE_API_KEYS (리스트 또는 줄바꿈 문자열) 의 첫 번째
    2) YOUTUBE_API_KEY (단일 문자열)
    """
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
# 등급 계산 (시간당 클릭수 기준) A~H 재매핑
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

            # 썸네일 URL
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
    """채널 키워드로 채널 목록 검색 (채널 아이콘 썸네일까지)"""
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
        subs = int(stt.get("subscriberCount", 0)) if stt.get("subscriberCount") is not None else None
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
            "thumbnail_url": thumb_url,  # 채널 아이콘
        })

    results.sort(key=lambda r: (r["subs"] or 0), reverse=True)
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
    """채널 이름으로 채널을 찾고, 해당 채널의 영상들을 검색"""
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
):
    """
    단순 트렌드(인기 동영상) 가져오기.
    YouTube Data API: videos().list(chart="mostPopular")
    """
    youtube = get_youtube_client()
    take = max(1, min(max_results, 50))
    kwargs = dict(
        part="snippet,statistics,contentDetails",
        chart="mostPopular",
        maxResults=take,
    )
    if region_code:
        kwargs["regionCode"] = region_code

    try:
        resp = youtube.videos().list(**kwargs).execute()
        cost_used = 1
    except HttpError as e:
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
st.sidebar.header("검색")

# ① 일반 검색어 + 버튼
general_query = st.sidebar.text_input("🔍 일반 검색어", "")
do_general_search = st.sidebar.button("일반 검색 실행", type="primary", use_container_width=True)

# 보기 모드: 리스트 / 그리드 / 쇼츠 (단일 선택)
view_mode = st.sidebar.selectbox(
    "보기 모드",
    ["리스트뷰", "그리드뷰", "쇼츠뷰"],
    index=1,   # 기본값: 그리드뷰
)

# 정렬 방식 (리스트뷰/그리드뷰에서 사용)
with st.sidebar.expander("정렬 방식", expanded=False):
    sort_target = st.selectbox(
        "정렬 기준",
        ["자동", "등급", "영상조회수", "시간당클릭", "업로드시각", "제목", "채널명", "구독자수"],
        key="sort_target",
    )
    sort_order = st.radio(
        "정렬 방향",
        ["내림차순", "오름차순"],
        index=0,
        horizontal=True,
        key="sort_order",
    )

st.sidebar.markdown("---")

# ② 트렌드 검색
with st.sidebar.expander("🔥 트렌드 검색", expanded=False):
    trend_region_name = st.selectbox(
        "트렌드 지역 (국가)",
        COUNTRY_LIST,
        index=0,
        key="trend_region",
    )
    do_trend_search = st.button("트렌드 불러오기", use_container_width=True, key="btn_trend")

# ③ 채널영상검색
with st.sidebar.expander("🎬 채널영상검색", expanded=False):
    channel_name_for_videos = st.text_input("채널 이름", key="channel_name_videos")
    do_channel_videos_search = st.button(
        "채널 영상 검색 실행",
        use_container_width=True,
        key="btn_channel_videos"
    )

# ④ 키워드채널검색
with st.sidebar.expander("📈 키워드채널검색", expanded=False):
    channel_keyword = st.text_input("채널 키워드", key="channel_keyword")
    do_channel_list_search = st.button(
        "채널 목록 검색 실행",
        use_container_width=True,
        key="btn_channel_list"
    )

st.sidebar.markdown("---")

# 세부 필터 (항상 펼쳐진 상태로 시작)
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
    st.session_state.search_mode = None  # "general", "trend", "channel_videos", "channel_list"

# 클라이언트 필터
def apply_client_filters(df: pd.DataFrame, upload_period: str, min_views_label: str) -> pd.DataFrame:
    if upload_period != "제한없음" and "업로드시각" in df.columns:
        days = int(upload_period.replace("일",""))
        cutoff = datetime.now(KST) - timedelta(days=days)
        df = df[df["업로드시각"] >= cutoff]
    min_views = parse_min_views(min_views_label)
    if "영상조회수" in df.columns:
        df = df[df["영상조회수"] >= min_views]
    return df

# -----------------------------
# 검색 실행 로직
# -----------------------------
try:
    # 어떤 버튼이 눌렸는지 판단
    mode_triggered = None
    if do_general_search:
        mode_triggered = "general"
    elif do_trend_search:
        mode_triggered = "trend"
    elif do_channel_videos_search:
        mode_triggered = "channel_videos"
    elif do_channel_list_search:
        mode_triggered = "channel_list"

    if mode_triggered is not None:
        search_dt = datetime.now(KST)

        # ------ 일반 검색 ------
        if mode_triggered == "general":
            base_query = (general_query or "").strip()
            if not base_query:
                st.warning("일반 검색어를 입력해주세요.")
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
                            "등급": calc_grade(cph),
                            "썸네일": r.get("thumbnail_url", ""),
                            "채널명": r["channel_title"],
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

        # ------ 트렌드 검색 ------
        elif mode_triggered == "trend":
            trend_rc, _ = COUNTRY_LANG_MAP[trend_region_name]
            append_keyword_log(f"[trend]{trend_region_name}")
            status_placeholder.info("트렌드(인기 동영상) 불러오는 중...")
            raw_results, cost_used, breakdown = search_trending_videos(
                max_results=max_fetch,
                region_code=trend_rc,
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
                        "등급": calc_grade(cph),
                        "썸네일": r.get("thumbnail_url", ""),
                        "채널명": r["channel_title"],
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

        # ------ 채널영상검색 ------
        elif mode_triggered == "channel_videos":
            ch_name = (channel_name_for_videos or "").strip()
            if not ch_name:
                st.warning("채널 이름을 입력해주세요.")
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
                            "등급": calc_grade(cph),
                            "썸네일": r.get("thumbnail_url", ""),
                            "채널명": r["channel_title"],
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

        # ------ 키워드채널검색 ------
        elif mode_triggered == "channel_list":
            ch_kw = (channel_keyword or "").strip()
            if not ch_kw:
                st.warning("채널 키워드를 입력해주세요.")
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
                        subs = r["subs"]
                        subs_text = f"{subs:,}" if isinstance(subs, int) else "-"
                        df_rows.append({
                            "썸네일": r.get("thumbnail_url", ""),  # 채널 아이콘
                            "채널명": r["channel_title"],
                            "구독자수": subs_text,
                            "채널조회수": f"{r['total_views']:,}",
                            "채널영상수": f"{r['videos']:,}",
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
# 결과 표시 (리스트 / 그리드 / 쇼츠)
# -----------------------------
df = st.session_state.results_df
mode = st.session_state.search_mode

if df is None or df.empty:
    st.info("아직 검색 결과가 없습니다. 좌측에서 조건을 설정하고 버튼을 눌러 검색하세요.")
else:
    df_display = df.copy()

    # URL 컬럼 이름 정리
    if "링크URL" in df_display.columns:
        df_display["링크"] = df_display["링크URL"]
        df_display = df_display.drop(columns=["링크URL"])

    # ----- 정렬 적용 (리스트뷰 / 그리드뷰일 때만) -----
    if view_mode in ("리스트뷰", "그리드뷰") and not df_display.empty:
        if sort_target != "자동":
            col_map = {
                "등급": "등급",
                "영상조회수": "영상조회수",
                "시간당클릭": "시간당클릭",
                "업로드시각": "업로드시각",
                "제목": "제목",
                "채널명": "채널명",
                "구독자수": "구독자수",
            }
            sort_col = col_map.get(sort_target)
            if sort_col and sort_col in df_display.columns:
                ascending = (sort_order == "오름차순")
                df_display = df_display.sort_values(
                    by=sort_col,
                    ascending=ascending,
                    kind="mergesort"  # 안정 정렬
                )

    # 모드별 제목
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

    # ==================================================
    # 1) 쇼츠뷰 (이미지만 촘촘하게)
    # ==================================================
    if view_mode == "쇼츠뷰":
        if mode == "channel_list":
            # 채널 아이콘 그리드 (100x100, 많이 보기) - 4열
            n_cols = 4
            cols = st.columns(n_cols)
            for idx, (_, row) in enumerate(df_display.iterrows()):
                col = cols[idx % n_cols]
                with col:
                    thumb = row.get("썸네일", "")
                    if isinstance(thumb, str) and thumb:
                        st.image(thumb, width=100)
                    # 캡션은 작게
                    ch_name = row.get("채널명", "")
                    if ch_name:
                        st.caption(ch_name)
                if (idx + 1) % n_cols == 0 and (idx + 1) < len(df_display):
                    cols = st.columns(n_cols)
            st.caption("📺 채널 아이콘을 한눈에 보는 쇼츠 뷰입니다.")
        else:
            # 일반/트렌드/채널영상검색: 9:16 비율, 작은 썸네일을 촘촘하게 (3열)
            n_cols = 3
            cols = st.columns(n_cols)
            for idx, (_, row) in enumerate(df_display.iterrows()):
                col = cols[idx % n_cols]
                with col:
                    thumb = row.get("썸네일", "")
                    if isinstance(thumb, str) and thumb:
                        # 9:16 비율, 가로 110px, 세로 ~196px, 여백 최소
                        html = f"""
                        <div style="width:110px;height:196px;overflow:hidden;border-radius:8px;margin:2px auto;">
                          <img src="{thumb}" style="width:100%;height:100%;object-fit:cover;" />
                        </div>
                        """
                        st.markdown(html, unsafe_allow_html=True)
                if (idx + 1) % n_cols == 0 and (idx + 1) < len(df_display):
                    cols = st.columns(n_cols)
            st.caption("🎞 쇼츠 전용 뷰: 9:16 비율로 세로 썸네일을 화면에 많이 보여줍니다.")

    # ==================================================
    # 2) 그리드뷰 (카드 형식) - 기본뷰
    # ==================================================
    elif view_mode == "그리드뷰":
        if mode == "channel_list":
            n_cols = 3
        else:
            n_cols = 3

        cols = st.columns(n_cols)

        for idx, (_, row) in enumerate(df_display.iterrows()):
            col = cols[idx % n_cols]
            with col:
                thumb = row.get("썸네일", "")
                grade = row.get("등급", "")
                if grade and mode != "channel_list":
                    st.markdown(f"**등급: {grade}**")
                if isinstance(thumb, str) and thumb:
                    st.image(thumb, use_column_width=True)

                if mode == "channel_list":
                    # 채널 카드
                    title = row.get("채널명", "")
                    subs = row.get("구독자수", "")
                    total_views = row.get("채널조회수", "")
                    video_count = row.get("채널영상수", "")
                    link = row.get("링크", "")
                    st.markdown(f"**{title}**")
                    st.caption(f"구독자: {subs} · 조회수: {total_views} · 영상수: {video_count}")
                    if link:
                        st.markdown(f"[채널 열기]({link})")
                else:
                    # 영상 카드
                    title = row.get("제목", "")
                    ch = row.get("채널명", "")
                    views = row.get("영상조회수", "")
                    link = row.get("링크", "")
                    st.markdown(f"**{title}**")
                    if isinstance(views, (int, float)):
                        views_text = f"{views:,}"
                    else:
                        views_text = str(views)
                    st.caption(f"{ch} · 조회수 {views_text}")
                    if link:
                        st.markdown(f"[영상 열기]({link})")

            if (idx + 1) % n_cols == 0 and (idx + 1) < len(df_display):
                cols = st.columns(n_cols)

        st.caption("👉 카드의 링크 텍스트를 눌러서 새 탭에서 열 수 있습니다.")

    # ==================================================
    # 3) 리스트뷰 (테이블)
    # ==================================================
    else:
        # 컬럼 순서 (등급을 썸네일 왼쪽으로)
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

        # 컬럼 설정 (썸네일은 항상 표시)
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

        # 모드별 key (정렬/눈아이콘 상태 분리)
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
            height=650,    # ✅ 테이블 높이 증가(한 화면에 더 많은 행 보이게)
            hide_index=True,
            column_order=column_order if column_order else None,
            column_config=column_config,
            key=editor_key,
            disabled=True,          # 편집 불가
            num_rows="fixed",       # 행 추가/삭제 불가
        )

        st.caption("👉 '열기' 링크를 누르면 새 탭에서 영상 또는 채널이 열립니다.")

# -----------------------------
# 사이드바 하단: 쿼터 / 최근 키워드 / 로그아웃
# -----------------------------
st.sidebar.markdown("---")

# 오늘 사용한 쿼터 (작게)
quota_today = get_today_quota_total()
st.sidebar.caption(f"오늘 사용 쿼터: {quota_today:,} units")

# 최근 키워드 (최대 7개, 날짜 없이)
recents = get_recent_keywords(7)
if recents:
    keywords = [q for _, q in recents]
    labels = [f"`{k}`" for k in keywords]
    st.sidebar.caption("최근 키워드: " + " · ".join(labels))
else:
    st.sidebar.caption("최근 키워드: 없음")

# 로그아웃 버튼
if st.sidebar.button("로그아웃", use_container_width=True):
    st.session_state["logged_in"] = False
    st.rerun()

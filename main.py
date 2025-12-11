#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import random
from datetime import datetime, timedelta, timezone

import streamlit as st
import pandas as pd
import altair as alt

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

st.set_page_config(
    page_title="YouTube검색기",
    page_icon="🔍",
    layout="wide",
)

st.markdown(
    """
<a id="page_top"></a>
<style>
.block-container { padding-top: 3rem !important; }
[data-testid="stDataFrame"] button[kind="icon"] {
    display: none !important;
}
</style>
""",
    unsafe_allow_html=True,
)

CONFIG_PATH = "config.json"

def load_config():
    if not os.path.exists(CONFIG_PATH):
        return {"quota_usage": {}, "keyword_log": []}
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        data = {}
    if "quota_usage" not in data:
        data["quota_usage"] = {}
    if "keyword_log" not in data:
        data["keyword_log"] = []
    return data

def save_config(data: dict):
    try:
        with open(CONFIG_PATH, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        st.warning(f"config.json 저장 오류: {e}")

CONFIG_DATA = load_config()

KST = timezone(timedelta(hours=9))

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

TREND_CATEGORY_MAP = {
    "전체": None,
    "영화/애니메이션": "1",
    "자동차/교통": "2",
    "음악": "10",
    "스포츠": "17",
    "게임": "20",
    "인물/블로그": "22",
    "코미디": "23",
    "엔터테인먼트": "24",
    "뉴스/정치": "25",
    "노하우/스타일": "26",
    "교육": "27",
    "과학/기술": "28",
}

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

def quota_today_key():
    return datetime.now(KST).strftime("%Y-%m-%d")

def _load_quota_map():
    return CONFIG_DATA.get("quota_usage", {})

def _save_quota_map(data: dict):
    CONFIG_DATA["quota_usage"] = data
    save_config(CONFIG_DATA)

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

def _load_keyword_log():
    return CONFIG_DATA.get("keyword_log", [])

def _save_keyword_log(entries: list):
    CONFIG_DATA["keyword_log"] = entries
    save_config(CONFIG_DATA)

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
        q = item.get("q")
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
                h = int(num)
                num = ""
            elif ch == "M" and num:
                m = int(num)
                num = ""
            elif ch == "S" and num:
                s = int(num)
                num = ""
    return h * 3600 + m * 60 + s

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
    if label == "전체":
        return True
    if label == "쇼츠":
        return seconds < 60
    if label == "롱폼":
        return seconds >= 60
    if label == "1~20분":
        return 60 <= seconds < 20 * 60
    if label == "20~40분":
        return 20 * 60 <= seconds < 40 * 60
    if label == "40~60분":
        return 40 * 60 <= seconds < 60 * 60
    if label == "60분이상":
        return seconds >= 60 * 60
    return True

def parse_min_views(text: str) -> int:
    digits = text.replace(",", "").replace(" ", "").replace("만", "0000")
    try:
        return int(digits)
    except Exception:
        return 0

def calc_grade(clicks_per_hour: int) -> str:
    v = clicks_per_hour
    if v >= 5000:
        return "A"
    if v >= 2000:
        return "B"
    if v >= 1000:
        return "C"
    if v >= 500:
        return "D"
    if v >= 300:
        return "E"
    if v >= 100:
        return "F"
    if v >= 50:
        return "G"
    return "H"

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
            cost_used += 100
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
            cost_used += 1
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

            results_tmp.append(
                {
                    "title": title,
                    "views": view_count,
                    "published_at_iso": published_at_iso,
                    "url": url,
                    "duration_sec": duration_sec,
                    "channel_title": snip.get("channelTitle", ""),
                    "thumbnail_url": thumb_url,
                }
            )

        fetched += len(page_ids)
        next_token = search_response.get("nextPageToken")
        if not next_token:
            break

    return results_tmp, cost_used

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
        subs = (
            int(stt.get("subscriberCount", 0))
            if stt.get("subscriberCount") is not None
            else None
        )
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

        results.append(
            {
                "channel_title": sn.get("title", ""),
                "subs": subs,
                "total_views": total_views,
                "videos": videos,
                "url": url,
                "thumbnail_url": thumb_url,
            }
        )

    results.sort(key=lambda r: (r["subs"] or 0), reverse=True)
    return results, cost_used

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
        cost_used += 100
    except HttpError as e:
        raise RuntimeError(f"채널 검색 오류: {e}")

    items = ch_resp.get("items", [])
    if not items:
        return [], cost_used

    channel_id = items[0]["id"]["channelId"]

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

            results_tmp.append(
                {
                    "title": title,
                    "views": view_count,
                    "published_at_iso": published_at_iso,
                    "url": url,
                    "duration_sec": duration_sec,
                    "channel_title": snip.get("channelTitle", ""),
                    "thumbnail_url": thumb_url,
                }
            )

        fetched += len(page_ids)
        next_token = v_search.get("nextPageToken")
        if not next_token:
            break

    return results_tmp, cost_used

def search_trending_videos(
    max_results: int,
    region_code: str | None,
    video_category_id: str | None,
):
    youtube = get_youtube_client()
    take = max(1, min(int(max_results or 50), 50))
    kwargs = dict(
        part="snippet,statistics,contentDetails",
        chart="mostPopular",
        maxResults=take,
    )
    if region_code:
        kwargs["regionCode"] = region_code
    if video_category_id:
        kwargs["videoCategoryId"] = video_category_id

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

        results.append(
            {
                "title": title,
                "views": view_count,
                "published_at_iso": published_at_iso,
                "url": url,
                "duration_sec": duration_sec,
                "channel_title": snip.get("channelTitle", ""),
                "thumbnail_url": thumb_url,
            }
        )
    return results, cost_used

st.sidebar.caption("🔍 YouTube검색기")

st.session_state.setdefault("sort_key", "등급")
st.session_state.setdefault("sort_asc", True)
st.session_state.setdefault("view_mode_label", "그리드 뷰")

with st.sidebar.expander("정렬 방식", expanded=True):
    sort_key = st.selectbox(
        "정렬 기준",
        ["등급", "영상조회수", "시간당클릭", "업로드시각", "구독자수", "채널조회수", "채널영상수"],
        index=0,
        key="sort_key_ui",
    )
    sort_dir = st.radio(
        "정렬 방향",
        ["오름차순", "내림차순"],
        index=0 if st.session_state["sort_asc"] else 1,
        horizontal=True,
        key="sort_dir_ui",
    )
    st.session_state["sort_key"] = sort_key
    st.session_state["sort_asc"] = sort_dir == "오름차순"

st.sidebar.markdown("---")

with st.sidebar.expander("⚙ 세부 필터", expanded=True):
    api_period = st.selectbox(
        "서버 검색기간 (YouTube API)",
        ["제한없음", "7일", "30일", "90일", "180일", "365일", "730일"],
        index=1,
        key="api_period",
    )
    upload_period = st.selectbox(
        "업로드 기간(클라이언트 필터)",
        ["제한없음", "1일", "3일", "7일", "30일", "90일", "180일", "365일"],
        index=6,
        key="upload_period",
    )
    min_views_label = st.selectbox(
        "최소 조회수",
        [
            "5,000",
            "10,000",
            "25,000",
            "50,000",
            "100,000",
            "200,000",
            "500,000",
            "1,000,000",
        ],
        index=0,
        key="min_views_label",
    )
    duration_label = st.selectbox(
        "영상 길이",
        ["전체", "쇼츠", "롱폼", "1~20분", "20~40분", "40~60분", "60분이상"],
        index=0,
        key="duration_label",
    )
    max_fetch = st.number_input(
        "모든 검색에서 가져올 최대 개수",
        1,
        5000,
        50,
        step=10,
        key="max_fetch",
    )
    country_name = st.selectbox(
        "검색용 국가/언어", COUNTRY_LIST, index=0, key="country_for_search"
    )
    region_code, lang_code = COUNTRY_LANG_MAP[country_name]

quota_today = get_today_quota_total()
st.sidebar.caption(f"오늘 사용 쿼터: {quota_today:,} units")

recents = get_recent_keywords(7)
if recents:
    keywords = [q for _, q in recents]
    labels = [f"`{k}`" for k in keywords]
    st.sidebar.caption("최근 키워드: " + " · ".join(labels))
else:
    st.sidebar.caption("최근 키워드: 없음")

status_placeholder = st.empty()

if "results_df" not in st.session_state:
    st.session_state.results_df = None
    st.session_state.last_search_time = None
    st.session_state.search_mode = None

mode_options = ["일반", "채널영상", "키워드 채널", "트렌드", "랜덤 트렌드"]
st.session_state.setdefault("search_mode_value", mode_options[0])
st.session_state.setdefault("search_query", "")
st.session_state.setdefault("trend_category_label", list(TREND_CATEGORY_MAP.keys())[0])

do_search = False

with st.expander("검색", expanded=True):
    mode_col, _ = st.columns([1.8, 2.2])
    with mode_col:
        search_mode_label = st.radio(
            "검색 모드",
            options=mode_options,
            index=mode_options.index(st.session_state["search_mode_value"]),
            key="search_mode_value",
            horizontal=True,
        )

    if search_mode_label in ("일반", "채널영상", "키워드 채널"):
        q_col, _ = st.columns([3, 1])
        with q_col:
            search_query = st.text_input(
                "검색어 / 채널명",
                value=st.session_state["search_query"],
                placeholder="검색어 또는 채널명을 입력하세요.",
                key="search_query_input",
            )
            st.session_state["search_query"] = search_query
    else:
        st.session_state["search_query"] = ""

    if search_mode_label == "트렌드":
        default_label = st.session_state.get(
            "trend_category_label", list(TREND_CATEGORY_MAP.keys())[0]
        )
        options = list(TREND_CATEGORY_MAP.keys())
        try:
            idx = options.index(default_label)
        except ValueError:
            idx = 0
        trend_category_label = st.selectbox(
            "트렌드 카테고리",
            options,
            index=idx,
            key="trend_category_label_widget",
        )
        st.session_state["trend_category_label"] = trend_category_label

    do_search = st.button("🔍 검색 실행", use_container_width=True)

view_mode_label = st.radio(
    "보기 모드",
    options=["그리드 뷰", "리스트 뷰", "쇼츠 뷰"],
    index=["그리드 뷰", "리스트 뷰", "쇼츠 뷰"].index(
        st.session_state.get("view_mode_label", "그리드 뷰")
    )
    if st.session_state.get("view_mode_label", "그리드 뷰")
    in ["그리드 뷰", "리스트 뷰", "쇼츠 뷰"]
    else 0,
    key="view_mode_label",
    horizontal=True,
)

if view_mode_label == "그리드 뷰":
    view_mode = "grid"
elif view_mode_label == "리스트 뷰":
    view_mode = "list"
else:
    view_mode = "shorts"

def apply_client_filters(df: pd.DataFrame, upload_period: str, min_views_label: str) -> pd.DataFrame:
    if upload_period != "제한없음" and "업로드시각" in df.columns:
        days = int(upload_period.replace("일", ""))
        cutoff = datetime.now(KST) - timedelta(days=days)
        df = df[df["업로드시각"] >= cutoff]
    min_views = parse_min_views(min_views_label)
    if "영상조회수" in df.columns:
        df = df[df["영상조회수"] >= min_views]
    return df

def sort_dataframe(df: pd.DataFrame, mode: str, sort_key: str, ascending: bool) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    if sort_key not in df.columns:
        if mode in ("general", "trend", "random_trend", "channel_videos"):
            key_fallback = "등급" if "등급" in df.columns else None
        else:
            key_fallback = "구독자수" if "구독자수" in df.columns else None
        if not key_fallback:
            return df
        sort_key = key_fallback

    if sort_key == "등급":
        order = ["A", "B", "C", "D", "E", "F", "G", "H"]
        cat = pd.Categorical(df["등급"], categories=order, ordered=True)
        df = df.assign(_grade_cat=cat)
        df = df.sort_values("_grade_cat", ascending=ascending, kind="mergesort")
        return df.drop(columns=["_grade_cat"])

    if sort_key in ["영상조회수", "시간당클릭"]:
        return df.sort_values(sort_key, ascending=ascending, kind="mergesort")

    if sort_key == "업로드시각":
        return df.sort_values("업로드시각", ascending=ascending, kind="mergesort")

    if sort_key in ["구독자수", "채널조회수", "채널영상수"]:
        tmp = df[sort_key].astype(str).str.replace(",", "").str.replace(" ", "")
        num = pd.to_numeric(tmp, errors="coerce").fillna(0)
        df = df.assign(_num=num)
        df = df.sort_values("_num", ascending=ascending, kind="mergesort")
        return df.drop(columns=["_num"])

    return df.sort_values(sort_key, ascending=ascending, kind="mergesort")

try:
    mode_triggered = None
    if do_search:
        if search_mode_label == "랜덤 트렌드":
            mode_triggered = "random_trend"
        elif search_mode_label == "일반":
            mode_triggered = "general"
        elif search_mode_label == "트렌드":
            mode_triggered = "trend"
        elif search_mode_label == "채널영상":
            mode_triggered = "channel_videos"
        elif search_mode_label == "키워드 채널":
            mode_triggered = "channel_list"

    if mode_triggered is not None:
        search_dt = datetime.now(KST)

        if mode_triggered == "random_trend":
            rand_country_label = random.choice(COUNTRY_LIST)
            rand_region_code, _ = COUNTRY_LANG_MAP[rand_country_label]
            rand_cat_label = random.choice(list(TREND_CATEGORY_MAP.keys()))
            rand_cat_id = TREND_CATEGORY_MAP[rand_cat_label]

            append_keyword_log(f"[random]{rand_country_label}/{rand_cat_label}")
            status_placeholder.info(
                f"랜덤 트렌드 검색 실행 중... (국가: {rand_country_label}, 카테고리: {rand_cat_label})"
            )
            raw_results, cost_used = search_trending_videos(
                max_results=max_fetch,
                region_code=rand_region_code,
                video_category_id=rand_cat_id,
            )
            add_quota_usage(cost_used)

            if not raw_results:
                st.session_state.results_df = None
                st.session_state.search_mode = "random_trend"
                status_placeholder.info("랜덤 트렌드 결과 0건")
            else:
                rows = []
                for r in raw_results:
                    pub_kst = parse_published_at_to_kst(r["published_at_iso"])
                    d, h = human_elapsed_days_hours(search_dt, pub_kst)
                    total_hours = max(1, d * 24 + h)
                    cph = int(round(r["views"] / total_hours))
                    rows.append(
                        {
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
                        }
                    )
                df = pd.DataFrame(rows)
                if not df.empty:
                    df = apply_client_filters(df, upload_period, min_views_label)
                st.session_state.results_df = df
                st.session_state.last_search_time = search_dt
                st.session_state.search_mode = "random_trend"
                status_placeholder.success(
                    f"[랜덤 트렌드 검색] 결과: {len(df):,}건 (이번 쿼터 사용량: {cost_used})"
                )

        elif mode_triggered == "general":
            base_query = (st.session_state.get("search_query") or "").strip()
            if not base_query:
                st.warning("검색어를 입력해주세요.")
            else:
                append_keyword_log(base_query)
                status_placeholder.info("일반 영상 검색 실행 중...")
                raw_results, cost_used = search_videos(
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
                        total_hours = max(1, d * 24 + h)
                        cph = int(round(r["views"] / total_hours))
                        rows.append(
                            {
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
                            }
                        )
                    df = pd.DataFrame(rows)
                    if not df.empty:
                        df = apply_client_filters(df, upload_period, min_views_label)
                    st.session_state.results_df = df
                    st.session_state.last_search_time = search_dt
                    st.session_state.search_mode = "general"
                    status_placeholder.success(
                        f"[일반 검색] 서버 결과: {len(raw_results):,}건 / 필터 후: {len(df):,}건 (이번 쿼터 사용량: {cost_used})"
                    )

        elif mode_triggered == "trend":
            options = list(TREND_CATEGORY_MAP.keys())
            trend_label = st.session_state.get("trend_category_label", options[0])
            trend_cat_id = TREND_CATEGORY_MAP.get(trend_label)
            append_keyword_log(f"[trend]{trend_label}")
            status_placeholder.info("트렌드 검색 실행 중...")
            raw_results, cost_used = search_trending_videos(
                max_results=max_fetch,
                region_code=region_code,
                video_category_id=trend_cat_id,
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
                    total_hours = max(1, d * 24 + h)
                    cph = int(round(r["views"] / total_hours))
                    rows.append(
                        {
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
                        }
                    )
                df = pd.DataFrame(rows)
                if not df.empty:
                    df = apply_client_filters(df, upload_period, min_views_label)
                st.session_state.results_df = df
                st.session_state.last_search_time = search_dt
                st.session_state.search_mode = "trend"
                status_placeholder.success(
                    f"[트렌드 검색] 결과: {len(df):,}건 (이번 쿼터 사용량: {cost_used})"
                )

        elif mode_triggered == "channel_videos":
            ch_name = (st.session_state.get("search_query") or "").strip()
            if not ch_name:
                st.warning("채널 이름을 입력해주세요.")
            else:
                append_keyword_log(f"[channel_videos]{ch_name}")
                status_placeholder.info("채널 영상 검색 실행 중...")
                raw_results, cost_used = search_videos_in_channel_by_name(
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
                        total_hours = max(1, d * 24 + h)
                        cph = int(round(r["views"] / total_hours))
                        rows.append(
                            {
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
                            }
                        )
                    df = pd.DataFrame(rows)
                    if not df.empty:
                        df = apply_client_filters(df, upload_period, min_views_label)
                    st.session_state.results_df = df
                    st.session_state.last_search_time = search_dt
                    st.session_state.search_mode = "channel_videos"
                    status_placeholder.success(
                        f"[채널 영상 검색] 서버 결과: {len(raw_results):,}건 / 필터 후: {len(df):,}건 (이번 쿼터 사용량: {cost_used})"
                    )

        elif mode_triggered == "channel_list":
            ch_kw = (st.session_state.get("search_query") or "").strip()
            if not ch_kw:
                st.warning("채널 키워드를 입력해주세요.")
            else:
                append_keyword_log(f"[channel]{ch_kw}")
                status_placeholder.info("채널 목록 검색 실행 중...")
                ch_results, cost_used = search_channels_by_keyword(
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
                        df_rows.append(
                            {
                                "썸네일": r.get("thumbnail_url", ""),
                                "채널명": r["channel_title"],
                                "구독자수": subs_text,
                                "채널조회수": f"{r['total_views']:,}",
                                "채널영상수": f"{r['videos']:,}",
                                "링크URL": r["url"],
                            }
                        )
                    df = pd.DataFrame(df_rows)
                    st.session_state.results_df = df
                    st.session_state.last_search_time = search_dt
                    st.session_state.search_mode = "channel_list"
                    status_placeholder.success(
                        f"[채널 목록 검색] 결과: {len(df):,}건 (이번 쿼터 사용량: {cost_used})"
                    )

except Exception as e:
    st.error(f"검색 중 오류: {e}")
    st.session_state.results_df = None

df = st.session_state.results_df
mode = st.session_state.search_mode

if df is None or df.empty:
    st.info("아직 검색 결과가 없습니다. 상단의 '검색'을 열고 검색모드와 검색어를 설정한 뒤 실행해보세요.")
else:
    if "영상조회수" in df.columns and "등급" in df.columns:
        tmp = df.copy()
        tmp = tmp[tmp["등급"].notna()]
        if not tmp.empty:
            tmp_sorted = tmp.sort_values("영상조회수")
            grade_order = ["H", "G", "F", "E", "D", "C", "B", "A"]
            grade_map = {g: i + 1 for i, g in enumerate(grade_order)}
            tmp_sorted["grade_score"] = tmp_sorted["등급"].map(grade_map).fillna(0)
            tmp_sorted["순위(조회수기준)"] = range(1, len(tmp_sorted) + 1)
            chart_data = tmp_sorted[
                ["순위(조회수기준)", "grade_score", "제목", "채널명", "영상조회수", "등급"]
            ]

            grade_axis_expr = (
                "datum.value == 1 ? 'H' : "
                "datum.value == 2 ? 'G' : "
                "datum.value == 3 ? 'F' : "
                "datum.value == 4 ? 'E' : "
                "datum.value == 5 ? 'D' : "
                "datum.value == 6 ? 'C' : "
                "datum.value == 7 ? 'B' : "
                "datum.value == 8 ? 'A' : ''"
            )

            chart = (
                alt.Chart(chart_data)
                .mark_bar()
                .encode(
                    x=alt.X("순위(조회수기준):O", title="조회수 기준 순위"),
                    y=alt.Y(
                        "grade_score:Q",
                        title="등급",
                        axis=alt.Axis(
                            values=[1, 2, 3, 4, 5, 6, 7, 8],
                            labelExpr=grade_axis_expr,
                        ),
                    ),
                    tooltip=[
                        alt.Tooltip("제목:N", title="제목"),
                        alt.Tooltip("채널명:N", title="채널"),
                        alt.Tooltip("영상조회수:Q", title="조회수", format=",.0f"),
                        alt.Tooltip("등급:N", title="등급"),
                    ],
                )
                .properties(height=180, width="container")
            )
            st.altair_chart(chart, use_container_width=True)
            st.markdown("<div style='margin-top:0.5rem;'></div>", unsafe_allow_html=True)

    df_display = df.copy()
    if "링크URL" in df_display.columns:
        df_display["링크"] = df_display["링크URL"]
        df_display = df_display.drop(columns=["링크URL"])

    df_display = sort_dataframe(
        df_display,
        mode=mode or "",
        sort_key=st.session_state["sort_key"],
        ascending=st.session_state["sort_asc"],
    )

    if mode == "general":
        st.subheader("📊 일반 검색 결과 리스트")
    elif mode in ("trend", "random_trend"):
        st.subheader("🔥 트렌드 검색 결과 리스트")
    elif mode == "channel_videos":
        st.subheader("🎬 채널 영상 리스트")
    elif mode == "channel_list":
        st.subheader("📺 채널검색 리스트")
    else:
        st.subheader("📊 검색 결과 리스트")

    if view_mode == "shorts":
        if mode in ("general", "trend", "random_trend", "channel_videos"):
            html_items = []
            for _, row in df_display.iterrows():
                thumb = str(row.get("썸네일", "") or "")
                if not thumb:
                    continue
                ch = str(row.get("채널명", "") or "")
                link = str(row.get("링크", "") or "")
                channel_html = (
                    f'<div class="shorts-meta-channel">{ch}</div>' if ch else ""
                )
                link_html = (
                    f'<a href="{link}" target="_blank" class="shorts-meta-link">영상 열기</a>'
                    if link
                    else ""
                )
                meta_html = ""
                if channel_html or link_html:
                    meta_html = f'<div class="shorts-meta">{channel_html}{link_html}</div>'
                html_items.append(
                    '<div class="shorts-item">'
                    f'  <div class="shorts-frame" style="background-image:url(\'{thumb}\');"></div>'
                    f'  {meta_html}'
                    "</div>"
                )
            html = (
                "<style>"
                ".shorts-container{display:flex;flex-wrap:wrap;justify-content:center;gap:4px 4px;}"
                ".shorts-item{flex:0 0 23%;max-width:170px;}"
                ".shorts-frame{position:relative;width:100%;height:0;padding-bottom:177%;"
                "overflow:hidden;border-radius:10px;background:#000;"
                "background-size:cover;background-position:center center;background-repeat:no-repeat;}"
                ".shorts-meta{display:flex;justify-content:space-between;align-items:center;"
                "margin-top:2px;font-size:11px;line-height:1.2;}"
                ".shorts-meta-channel{flex:1;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;"
                "padding-right:4px;}"
                ".shorts-meta-link{text-decoration:none;border:1px solid #ccc;border-radius:999px;"
                "padding:1px 6px;font-size:11px;}"
                "@media (max-width:480px){"
                ".shorts-item{flex:0 0 48%;max-width:none;}"
                ".shorts-container{gap:4px 4px;}"
                "}"
                "</style>"
                f'<div class="shorts-container">{"".join(html_items)}</div>'
            )
            st.markdown(html, unsafe_allow_html=True)

        elif mode == "channel_list":
            thumbs = df_display["썸네일"].astype(str).tolist()
            html_items = []
            for url in thumbs:
                if not url:
                    continue
                html_items.append(
                    '<div class="shorts-item">'
                    f'  <img src="{url}" class="channel-icon"/>'
                    "</div>"
                )
            html = (
                "<style>"
                ".shorts-container{display:flex;flex-wrap:wrap;justify-content:center;gap:6px 6px;}"
                ".shorts-item{flex:0 0 22%;max-width:100px;}"
                ".channel-icon{width:100px;height:100px;object-fit:cover;border-radius:50%;display:block;}"
                "@media (max-width:480px){"
                ".shorts-item{flex:0 0 25%;}"
                ".channel-icon{width:80px;height:80px;}"
                "}"
                "</style>"
                f'<div class="shorts-container">{"".join(html_items)}</div>'
            )
            st.markdown(html, unsafe_allow_html=True)

        st.caption("쇼츠 뷰: 이미지를 눌러도 별도 동작은 하지 않습니다.")

    elif view_mode == "grid":
        n_cols = 3
        cols = st.columns(n_cols)

        for idx, (_, row) in enumerate(df_display.iterrows()):
            col = cols[idx % n_cols]
            with col:
                if (
                    "썸네일" in df_display.columns
                    and isinstance(row["썸네일"], str)
                    and row["썸네일"]
                ):
                    st.image(row["썸네일"], use_column_width=True)

                if mode == "channel_list":
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
                    title = row.get("제목", "")
                    ch = row.get("채널명", "")
                    views = row.get("영상조회수", "")
                    grade = row.get("등급", "")
                    link = row.get("링크", "")
                    st.markdown(f"**{title}**")
                    if isinstance(views, int):
                        st.caption(f"등급 {grade} · {ch} · 조회수 {views:,}")
                    else:
                        st.caption(f"등급 {grade} · {ch} · 조회수 {views}")
                    if link:
                        st.markdown(f"[영상 열기]({link})")

            if (idx + 1) % n_cols == 0 and (idx + 1) < len(df_display):
                cols = st.columns(n_cols)

        st.caption("👉 텍스트 링크를 눌러 새 탭에서 영상 또는 채널을 열 수 있습니다.")

    else:
        if mode in ("general", "trend", "random_trend", "channel_videos"):
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
        if "업로드시각" in df_display.columns:
            column_config["업로드시각"] = st.column_config.DatetimeColumn(
                "업로드시각",
                format="YYYY-MM-DD HH:mm",
            )

        if mode == "general":
            editor_key = "video_results_editor_general"
        elif mode in ("trend", "random_trend"):
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
            height=620,
            hide_index=True,
            column_order=column_order if column_order else None,
            column_config=column_config,
            key=editor_key,
            disabled=True,
            num_rows="fixed",
        )

        st.caption("👉 '열기' 링크를 누르면 새 탭에서 영상 또는 채널이 열립니다.")

st.markdown(
    """
<div style="text-align:center;margin:1.5rem 0 2.5rem;">
  <a href="#page_top"
     style="display:inline-block;padding:0.5rem 1.2rem;border-radius:999px;
            border:1px solid #ccc;text-decoration:none;font-size:13px;">
    ⬆ 페이지 상단으로
  </a>
</div>
""",
    unsafe_allow_html=True,
)

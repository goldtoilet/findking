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

st.set_page_config(
    page_title="YouTube 검색기 (Streamlit)",
    page_icon="🔍",
    layout="wide",
)

st.markdown(
    """
    <style>
    .block-container { padding-top: 3rem !important; }
    </style>
    """,
    unsafe_allow_html=True,
)

st.title("🔍 YouTube 검색기 (Streamlit)")

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

# ----------------------------
# Supabase 연동 (설정/로그 저장용)
# ----------------------------
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

# 설정/로그 파일 이름
KEYWORD_LOG_PATH  = "yts_keyword_log.json"
QUOTA_PATH        = "yts_quota_usage.json"

# ----------------------------
# API 키: secrets에서만 사용
# ----------------------------
YOUTUBE_API_KEYS = st.secrets.get("YOUTUBE_API_KEYS", [])
if isinstance(YOUTUBE_API_KEYS, str):
    YOUTUBE_API_KEYS = [YOUTUBE_API_KEYS]
SINGLE_API_KEY = st.secrets.get("YOUTUBE_API_KEY", "")

def get_current_api_key() -> str:
    """
    - st.secrets["YOUTUBE_API_KEYS"] → 리스트면 첫 번째 키 사용
    - 없으면 st.secrets["YOUTUBE_API_KEY"] 사용
    """
    if YOUTUBE_API_KEYS:
        return YOUTUBE_API_KEYS[0]
    if SINGLE_API_KEY:
        return SINGLE_API_KEY
    return ""

# ----------------------------
# 쿼터 관리
# ----------------------------
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

# ----------------------------
# 키워드 로그
# ----------------------------
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

# ----------------------------
# 시간/형식 유틸
# ----------------------------
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

# ----------------------------
# YouTube 클라이언트
# ----------------------------
def get_youtube_client():
    key = get_current_api_key()
    if not key:
        raise RuntimeError("YouTube API 키가 없습니다. (st.secrets에 YOUTUBE_API_KEYS 또는 YOUTUBE_API_KEY 설정 필요)")
    try:
        return build("youtube", "v3", developerKey=key, cache_discovery=False)
    except TypeError:
        return build("youtube", "v3", developerKey=key)

# ----------------------------
# 영상 검색 (키워드 기반 - 일반검색)
# ----------------------------
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

# ----------------------------
# 트렌드 검색 (최근 인기 영상 - 검색어 X)
# ----------------------------
def search_trending_videos(
    min_views: int,
    duration_label: str,
    max_fetch: int,
    region_code: str | None,
):
    youtube = get_youtube_client()

    cost_used = 0
    breakdown = {"videos.list": 0}
    max_fetch = max(1, min(int(max_fetch or 100), 200))  # 트렌드는 200개 정도면 충분

    results_tmp = []
    next_token = None
    fetched = 0

    while fetched < max_fetch:
        take = min(50, max_fetch - fetched)
        kwargs = dict(
            part="snippet,statistics,contentDetails",
            chart="mostPopular",
            maxResults=take,
        )
        if region_code:
            kwargs["regionCode"] = region_code
        if next_token:
            kwargs["pageToken"] = next_token

        try:
            resp = youtube.videos().list(**kwargs).execute()
            cost_used += 1
            breakdown["videos.list"] += 1
        except HttpError as e:
            raise RuntimeError(f"트렌드 API 오류: {e}")

        items = resp.get("items", [])
        if not items:
            break

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

        fetched += len(items)
        next_token = resp.get("nextPageToken")
        if not next_token:
            break

    return results_tmp, cost_used, breakdown

# ----------------------------
# 채널 키워드로 채널 찾기
# ----------------------------
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
        subs = int(stt.get("subscriberCount", 0)) if stt.get("subscriberCount") is not None else None
        total_views = int(stt.get("viewCount", 0))
        videos = int(stt.get("videoCount", 0))
        url = f"https://www.youtube.com/channel/{cid}" if cid else ""
        results.append({
            "channel_title": sn.get("title", ""),
            "subs": subs,
            "total_views": total_views,
            "videos": videos,
            "url": url,
        })

    results.sort(key=lambda r: (r["subs"] or 0), reverse=True)
    return results, cost_used, {"search.list": 100, "channels.list": 1}

# ----------------------------
# 채널 검색어(채널 이름)로 채널 영상 검색
# ----------------------------
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
    cost_used = 0
    breakdown = {"search.list": 0, "videos.list": 0}

    # 1) 채널 검색 (이름으로)
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
        ch_search = youtube.search().list(**kwargs_ch).execute()
        cost_used += 100; breakdown["search.list"] += 100
    except HttpError as e:
        raise RuntimeError(f"채널 검색 오류: {e}")

    items = ch_search.get("items", [])
    if not items:
        return [], cost_used, breakdown

    channel_id = items[0]["id"]["channelId"]
    channel_title = (items[0].get("snippet") or {}).get("title", channel_name)

    # 2) 해당 채널의 영상 검색
    published_after = published_after_from_label(api_period_label)
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
            video_resp = youtube.videos().list(
                part="snippet,statistics,contentDetails",
                id=",".join(page_ids),
            ).execute()
            cost_used += 1; breakdown["videos.list"] += 1
        except HttpError as e:
            raise RuntimeError(f"Videos API 오류: {e}")

        for item in video_resp.get("items", []):
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
                "channel_title": channel_title,
            })

        fetched += len(page_ids)
        next_token = v_search.get("nextPageToken")
        if not next_token:
            break

    return results_tmp, cost_used, breakdown

# ----------------------------
# 등급 계산
# ----------------------------
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

# ==================================================================
# 사이드바 UI
# ==================================================================

st.sidebar.header("검색")

# 1) 일반 검색 (항상 펼쳐져 있는 영역)
query = st.sidebar.text_input("🔍 일반 검색어", "", placeholder="예: 월드컵 경제학")
btn_general = st.sidebar.button("일반 검색 실행", use_container_width=True)

# Separator
try:
    st.sidebar.divider()
except Exception:
    st.sidebar.markdown("---")

# 2) 나머지 검색방식: 트렌드 / 채널 키워드 / 채널 영상 → 모두 expander로
with st.sidebar.expander("🔥 트렌드 검색", expanded=False):
    st.caption("현재 국가 기준으로 YouTube 인기 동영상을 가져옵니다.")
    btn_trend = st.button("트렌드 가져오기", use_container_width=True, key="btn_trend")

with st.sidebar.expander("📈 채널 키워드로 채널 찾기", expanded=False):
    channel_keyword = st.text_input("채널 키워드", "", placeholder="예: 축구 하이라이트", key="channel_keyword")
    btn_channel_find = st.button("채널 검색 실행", use_container_width=True, key="btn_channel_find")

with st.sidebar.expander("🎞 채널 이름으로 채널 영상 검색", expanded=False):
    channel_name = st.text_input("채널 검색어(채널 이름)", "", placeholder="예: SPOTV", key="channel_name")
    btn_channel_videos = st.button("채널 영상 불러오기", use_container_width=True, key="btn_channel_videos")

st.sidebar.markdown("---")

with st.sidebar.expander("⚙ 세부 필터", expanded=False):
    api_period = st.selectbox(
        "서버 검색기간(일반검색/채널영상)",
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

st.sidebar.markdown("---")

with st.sidebar.expander("⏱ 최근 검색 키워드", expanded=False):
    recents = get_recent_keywords(30)
    if not recents:
        st.write("최근 검색 없음")
    else:
        for dt, q in recents:
            st.write(f"- {dt.strftime('%m-%d %H:%M')} — `{q}`")

st.sidebar.markdown("---")
st.sidebar.metric("오늘 사용한 쿼터", f"{get_today_quota_total():,} units")

# ==================================================================
# 메인 영역
# ==================================================================

status_placeholder = st.empty()

if "results_df" not in st.session_state:
    st.session_state.results_df = None
    st.session_state.last_search_time = None
    st.session_state.search_type = None  # "video_general","video_trend","channel_find","channel_videos"

def apply_client_filters(df: pd.DataFrame, upload_period: str, min_views_label: str) -> pd.DataFrame:
    if upload_period != "제한없음" and "업로드시각" in df.columns:
        days = int(upload_period.replace("일",""))
        cutoff = datetime.now(KST) - timedelta(days=days)
        df = df[df["업로드시각"] >= cutoff]
    min_views = parse_min_views(min_views_label)
    if "영상조회수" in df.columns:
        df = df[df["영상조회수"] >= min_views]
    return df

# 어떤 버튼이 눌렸는지 확인
mode = None
if btn_general:
    mode = "video_general"
elif btn_trend:
    mode = "video_trend"
elif btn_channel_find:
    mode = "channel_find"
elif btn_channel_videos:
    mode = "channel_videos"

if mode is not None:
    try:
        # ---------------- 일반 검색 ----------------
        if mode == "video_general":
            base_query = (query or "").strip()
            if not base_query:
                st.warning("일반 검색어를 입력해주세요.")
            else:
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

                if not raw_results:
                    st.session_state.results_df = None
                    st.session_state.search_type = "video_general"
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
                        df = apply_client_filters(df, upload_period, min_views_label)
                    st.session_state.results_df = df
                    st.session_state.last_search_time = search_dt
                    st.session_state.search_type = "video_general"
                    status_placeholder.success(
                        f"서버 결과: {len(raw_results):,}건 / 필터 후: {len(df):,}건 (이번 쿼터 사용량: {cost_used})"
                    )
                add_quota_usage(cost_used)

        # ---------------- 트렌드 검색 ----------------
        elif mode == "video_trend":
            append_keyword_log("[trend]")
            status_placeholder.info("트렌드 인기 영상 불러오는 중...")

            raw_results, cost_used, breakdown = search_trending_videos(
                min_views=parse_min_views(min_views_label),
                duration_label=duration_label,
                max_fetch=max_fetch,
                region_code=region_code,
            )

            if not raw_results:
                st.session_state.results_df = None
                st.session_state.search_type = "video_trend"
                status_placeholder.info("트렌드 결과 0건")
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
                    df = apply_client_filters(df, upload_period, min_views_label)
                st.session_state.results_df = df
                st.session_state.last_search_time = search_dt
                st.session_state.search_type = "video_trend"
                status_placeholder.success(
                    f"트렌드 결과: {len(raw_results):,}건 / 필터 후: {len(df):,}건 (이번 쿼터 사용량: {cost_used})"
                )
            add_quota_usage(cost_used)

        # ---------------- 채널 키워드로 채널 찾기 ----------------
        elif mode == "channel_find":
            ch_kw = (channel_keyword or "").strip()
            if not ch_kw:
                st.warning("채널 키워드를 입력해주세요.")
            else:
                append_keyword_log(f"[channel-find]{ch_kw}")
                status_placeholder.info("채널 검색 실행 중...")
                ch_results, cost_used, breakdown = search_channels_by_keyword(
                    keyword=ch_kw,
                    max_results=max_fetch,
                    region_code=region_code,
                    lang_code=lang_code,
                )
                rows = []
                for r in ch_results:
                    subs = r["subs"]
                    subs_text = f"{subs:,}" if isinstance(subs, int) else "-"
                    rows.append({
                        "채널명": r["channel_title"],
                        "구독자수": subs_text,
                        "채널조회수": f"{r['total_views']:,}",
                        "채널영상수": f"{r['videos']:,}",
                        "URL": r["url"],
                    })
                df = pd.DataFrame(rows)
                st.session_state.results_df = df
                st.session_state.last_search_time = datetime.now(KST)
                st.session_state.search_type = "channel_find"
                status_placeholder.success(
                    f"채널 검색 결과: {len(df):,}건 (이번 쿼터 사용량: {cost_used})"
                )
                add_quota_usage(cost_used)

        # ---------------- 채널 이름으로 채널 영상 검색 ----------------
        elif mode == "channel_videos":
            ch_name = (channel_name or "").strip()
            if not ch_name:
                st.warning("채널 검색어(채널 이름)를 입력해주세요.")
            else:
                append_keyword_log(f"[channel-video]{ch_name}")
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
                if not raw_results:
                    st.session_state.results_df = None
                    st.session_state.search_type = "channel_videos"
                    status_placeholder.info("채널 영상 결과 0건")
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
                        df = apply_client_filters(df, upload_period, min_views_label)
                    st.session_state.results_df = df
                    st.session_state.last_search_time = search_dt
                    st.session_state.search_type = "channel_videos"
                    status_placeholder.success(
                        f"채널 영상 결과: {len(raw_results):,}건 / 필터 후: {len(df):,}건 (이번 쿼터 사용량: {cost_used})"
                    )
                add_quota_usage(cost_used)

    except Exception as e:
        st.error(f"검색 중 오류: {e}")
        st.session_state.results_df = None

# ==================================================================
# 결과 표시
# ==================================================================
df = st.session_state.results_df
search_type = st.session_state.search_type

if df is None or df.empty:
    st.info("아직 검색 결과가 없습니다. 좌측에서 조건을 설정하고 **검색 버튼**을 눌러주세요.")
else:
    # 검색 타입별 제목
    video_title_map = {
        "video_general": "📊 일반 검색 결과 리스트",
        "video_trend": "📈 트렌드 검색 결과 리스트",
        "channel_videos": "🎞 채널 영상 리스트",
    }

    if search_type in ("video_general", "video_trend", "channel_videos"):
        df_display = df.copy()
        df_display["링크"] = df_display["URL"]
        df_display = df_display.drop(columns=["URL"])
        st.subheader(video_title_map.get(search_type, "📊 영상 결과 리스트"))
        st.data_editor(
            df_display,
            use_container_width=True,
            height=500,
            hide_index=True,
            column_config={
                "링크": st.column_config.LinkColumn("열기", display_text="열기"),
            },
        )
    elif search_type == "channel_find":
        df_display = df.copy()
        df_display["링크"] = df_display["URL"]
        df_display = df_display.drop(columns=["URL"])
        st.subheader("📂 채널검색 리스트")
        st.data_editor(
            df_display,
            use_container_width=True,
            height=500,
            hide_index=True,
            column_config={
                "링크": st.column_config.LinkColumn("채널 열기", display_text="열기"),
            },
        )

    st.caption("열기 링크를 누르면 새 탭에서 영상 또는 채널이 열립니다.")

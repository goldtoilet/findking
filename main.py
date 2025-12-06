#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import os
import json
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path
import random

import streamlit as st

# --- 선택 의존성(썸네일 미리보기용, 없어도 동작) ---
try:
    import requests
    PIL_AVAILABLE = True
except Exception:
    PIL_AVAILABLE = False

# --- YouTube Data API ---
try:
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
except Exception:
    build = None
    HttpError = Exception

# ============================
# 저장 위치(iCloud) – Tk 버전과 동일
# ============================
ICLOUD_ROOT = Path.home() / "Library" / "Mobile Documents" / "com~apple~CloudDocs"
BASE_DIR = ICLOUD_ROOT / "youtubesearch"
BASE_DIR.mkdir(parents=True, exist_ok=True)

def _p(name: str) -> str:
    return str(BASE_DIR / name)

def _migrate(old: str, new: str):
    old = os.path.expanduser(old)
    if os.path.exists(old) and not os.path.exists(new):
        try:
            os.rename(old, new)
        except Exception:
            try:
                shutil.copy2(old, new)
            except Exception:
                pass

CONFIG_PATH       = _p("yts_config.json")
HISTORY_PATH      = _p("yts_search_history.json")
KEYWORD_LOG_PATH  = _p("yts_keyword_log.json")
QUOTA_PATH        = _p("yts_quota_usage.json")

# 과거 dot파일 마이그레이션
_migrate("~/.yts_config.json",         CONFIG_PATH)
_migrate("~/.yts_search_history.json", HISTORY_PATH)
_migrate("~/.yts_keyword_log.json",    KEYWORD_LOG_PATH)

# ----------------------------
# 환경/상수
# ----------------------------
ENV_KEY_NAME = "YOUTUBE_API_KEY"
KST = timezone(timedelta(hours=9))
WEEKDAY_KO = ["월요일","화요일","수요일","목요일","금요일","토요일","일요일"]

# 국가/언어 선택 - UI 라벨 → (regionCode, relevanceLanguage)
COUNTRY_LANG_MAP = {
    "한국": ("KR", "ko"),
    "일본": ("JP", "ja"),
    "미국": ("US", "en"),
    "영국": ("GB", "en"),
    "독일": ("DE", "de"),
    "프랑스": ("FR", "fr"),
    "브라질": ("BR", "pt"),
    "인도": ("IN", "en"),
    "인도네시아": ("ID", "id"),
    "베트남": ("VN", "vi"),
    "태국": ("TH", "th"),
    "필리핀": ("PH", "en"),
}
COUNTRY_LIST = list(COUNTRY_LANG_MAP.keys())

# ----------------------------
# 공용 JSON I/O
# ----------------------------
def _load_json(path, default):
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return default
    return default

def _save_json(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

# ----------------------------
# 쿼터 저장/로드
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
# API 키 관리 (간단 버전; Tk와 동일한 config 사용)
# ----------------------------
_DEFAULT_API_KEYS = []

def _load_api_keys_config():
    data = _load_json(CONFIG_PATH, {})
    keys = [k.strip() for k in (data.get("api_keys") or []) if k.strip()]
    if not keys and _DEFAULT_API_KEYS:
        keys = _DEFAULT_API_KEYS[:]
    sel = data.get("selected_index", 0)
    sel = max(0, min(sel, len(keys)-1)) if keys else 0
    return {"api_keys": keys, "selected_index": sel}

def _save_api_keys_config(keys, selected_index: int):
    keys = [k.strip() for k in keys if k.strip()]
    selected_index = max(0, min(selected_index, len(keys)-1)) if keys else 0
    _save_json(CONFIG_PATH, {"api_keys": keys, "selected_index": selected_index})

API_KEYS_STATE = {
    "keys": [],
    "index": 0,
}

def _apply_env_key(key: str):
    if key:
        os.environ[ENV_KEY_NAME] = key
    else:
        os.environ.pop(ENV_KEY_NAME, None)

def init_api_keys_state():
    cfg = _load_api_keys_config()
    API_KEYS_STATE["keys"] = cfg["api_keys"]
    API_KEYS_STATE["index"] = cfg["selected_index"]
    if API_KEYS_STATE["keys"]:
        _apply_env_key(API_KEYS_STATE["keys"][API_KEYS_STATE["index"]])
    else:
        _apply_env_key("")

def get_current_api_key() -> str:
    if not API_KEYS_STATE["keys"]:
        return ""
    return API_KEYS_STATE["keys"][API_KEYS_STATE["index"]]

def save_api_keys_from_user(keys: list[str], selected_index: int = 0):
    if not keys:
        _save_api_keys_config([], 0)
        API_KEYS_STATE["keys"] = []
        API_KEYS_STATE["index"] = 0
        _apply_env_key("")
        return
    _save_api_keys_config(keys, selected_index)
    cfg = _load_api_keys_config()
    API_KEYS_STATE["keys"] = cfg["api_keys"]
    API_KEYS_STATE["index"] = cfg["selected_index"]
    _apply_env_key(API_KEYS_STATE["keys"][API_KEYS_STATE["index"]])

# ----------------------------
# YouTube 클라이언트
# ----------------------------
def get_youtube_client():
    if build is None:
        raise RuntimeError(
            "google-api-python-client가 설치되어 있지 않습니다.\n"
            "터미널에서 아래 명령을 실행하세요:\n\n"
            "pip install google-api-python-client"
        )
    key = get_current_api_key()
    if not key:
        raise RuntimeError("API 키가 비어 있습니다. 사이드바에서 API 키를 입력/저장하세요.")
    try:
        return build("youtube", "v3", developerKey=key, cache_discovery=False)
    except TypeError:
        return build("youtube", "v3", developerKey=key)

# ----------------------------
# 기록/로그
# ----------------------------
def _load_history_raw():
    return _load_json(HISTORY_PATH, {})

def _save_history_raw(data: dict):
    try:
        _save_json(HISTORY_PATH, data)
    except Exception:
        pass

def add_to_history(query: str, limit_per_day: int = 100):
    q = (query or "").strip()
    if not q:
        return
    data = _load_history_raw()
    today = datetime.now(KST).strftime("%Y-%m-%d")
    lst = data.get(today, [])
    lst = [x for x in lst if x != q]
    lst.insert(0, q)
    data[today] = lst[:limit_per_day]
    _save_history_raw(data)

def _load_keyword_log():
    return _load_json(KEYWORD_LOG_PATH, [])

def _save_keyword_log(entries: list):
    try:
        _save_json(KEYWORD_LOG_PATH, entries)
    except Exception:
        pass

def append_keyword_log(query: str):
    q = (query or "").strip()
    if not q:
        return
    entries = _load_keyword_log()
    now = datetime.now(KST).isoformat(timespec="seconds")
    entries.append({"ts": now, "q": q})
    _save_keyword_log(entries)

def get_recent_keywords(days: int = 14, limit: int = 50):
    cutoff = datetime.now(KST) - timedelta(days=days)
    out = []
    for item in _load_keyword_log():
        ts = item.get("ts"); q = item.get("q")
        if not ts or not q:
            continue
        try:
            dt = datetime.fromisoformat(ts)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=KST)
            dt_kst = dt.astimezone(KST)
        except Exception:
            continue
        if dt_kst >= cutoff:
            out.append((dt_kst, q))
    out.sort(key=lambda x: x[0], reverse=True)
    return out[:limit]

# ----------------------------
# 시간/길이 유틸
# ----------------------------
def format_k_datetime_simple(dt_aw: datetime) -> str:
    if dt_aw.tzinfo is None:
        dt_aw = dt_aw.replace(tzinfo=KST)
    dt = dt_aw.astimezone(KST)
    return f"{dt.month}월 {dt.day}일 {dt.hour}시 {dt.minute}분"

def parse_published_at_to_kst(published_iso: str) -> datetime:
    dt_utc = datetime.fromisoformat(published_iso.replace("Z", "+00:00"))
    return dt_utc.astimezone(KST)

def human_elapsed_days_hours(later: datetime, earlier: datetime) -> (int, int):
    delta = later - earlier
    if delta.total_seconds() < 0:
        return 0, 0
    days = delta.days
    hours = delta.seconds // 3600
    return days, hours

def published_after_from_label(label: str):
    label = label.strip()
    now_utc = datetime.now(timezone.utc)
    if label.endswith("일"):
        days = int(label[:-1]); dt = now_utc - timedelta(days=days)
    else:
        return None
    return dt.isoformat(timespec="seconds").replace("+00:00", "Z")

def cutoff_dt_from_label_kst(label: str) -> datetime:
    label = label.strip()
    now_kst = datetime.now(KST)
    if label.endswith("일"):
        return now_kst - timedelta(days=int(label[:-1]))
    return now_kst

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
    if label == "1분~20분": return 60 <= seconds < 20*60
    if label == "20분~40분": return 20*60 <= seconds < 40*60
    if label == "40분~60분": return 40*60 <= seconds < 60*60
    if label == "60분이상": return seconds >= 60*60
    return True

def _chunked(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i:i+n]

# ----------------------------
# YouTube API 호출: 키워드 영상 검색
# ----------------------------
def search_videos(query: str, min_views: int, period_label: str, duration_label: str,
                  max_fetch: int = 200,
                  region_code: str | None = None, lang_code: str | None = None):
    youtube = get_youtube_client()
    published_after = published_after_from_label(period_label)

    cost_used = 0
    max_fetch = max(1, min(int(max_fetch or 200), 5000))

    results_tmp = []
    next_token = None
    fetched = 0

    while fetched < max_fetch:
        take = min(50, max_fetch - fetched)
        try:
            kwargs = dict(q=query, part="id", type="video", maxResults=take)
            if published_after:
                kwargs["publishedAfter"] = published_after
            if region_code:
                kwargs["regionCode"] = region_code
            if lang_code:
                kwargs["relevanceLanguage"] = lang_code
            if next_token:
                kwargs["pageToken"] = next_token

            search_response = youtube.search().list(**kwargs).execute()
            cost_used += 100
        except HttpError as e:
            raise RuntimeError(f"Search API 오류: {e}")

        page_ids = [it["id"]["videoId"] for it in search_response.get("items", [])
                    if "id" in it and "videoId" in it["id"]]
        if not page_ids:
            break

        try:
            video_response = youtube.videos().list(
                part="snippet,statistics,contentDetails",
                id=",".join(page_ids)
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
            thumbs = snip.get("thumbnails", {})
            thumb_url = (thumbs.get("high", {}) or {}).get("url") \
                        or (thumbs.get("medium", {}) or {}).get("url") \
                        or (thumbs.get("default", {}) or {}).get("url") \
                        or ""
            seconds = parse_duration_iso8601(cdet.get("duration", ""))

            if not duration_filter_ok(seconds, duration_label): 
                continue
            if view_count < min_views: 
                continue

            results_tmp.append({
                "title": title,
                "views": view_count,
                "published_at_iso": published_at_iso,
                "url": url,
                "thumbnail_url": thumb_url,
                "duration_sec": seconds,
                "channel_id": snip.get("channelId", ""),
                "channel_title": snip.get("channelTitle", ""),
            })

        fetched += len(page_ids)
        next_token = search_response.get("nextPageToken")
        if not next_token:
            break

    if not results_tmp:
        return [], cost_used

    # 채널 통계
    channel_ids = {r["channel_id"] for r in results_tmp if r.get("channel_id")}
    channels_map = {}
    try:
        for batch in _chunked(list(channel_ids), 50):
            ch_resp = youtube.channels().list(
                part="snippet,statistics",
                id=",".join(batch)
            ).execute()
            cost_used += 1
            for c in ch_resp.get("items", []):
                cid = c.get("id")
                cstats = c.get("statistics", {}) or {}
                subs = cstats.get("subscriberCount")
                subs_int = int(subs) if subs is not None else None
                channels_map[cid] = {
                    "title": (c.get("snippet", {}) or {}).get("title", ""),
                    "subs": subs_int,
                    "views": int(cstats.get("viewCount", 0)),
                    "videos": int(cstats.get("videoCount", 0)),
                }
    except HttpError:
        channels_map = {}

    results = []
    for r in results_tmp:
        cinfo = channels_map.get(r["channel_id"], {})
        r.update({
            "channel_subs": cinfo.get("subs"),
            "channel_total_views": cinfo.get("views", 0),
            "channel_video_count": cinfo.get("videos", 0),
            "channel_title": cinfo.get("title", r.get("channel_title", "")),
        })
        results.append(r)

    results.sort(key=lambda x: x["views"], reverse=True)
    return results, cost_used

# ----------------------------
# YouTube API: 채널 내부 영상 검색
# ----------------------------
def search_videos_in_channel_by_name(channel_query: str, min_views: int, period_label: str,
                                     duration_label: str, max_fetch: int = 200,
                                     region_code: str | None = None, lang_code: str | None = None):
    youtube = get_youtube_client()
    published_after = published_after_from_label(period_label)

    cost_used = 0
    max_fetch = max(1, min(int(max_fetch or 200), 5000))

    # 1) 채널 찾기
    try:
        kwargs_ch = dict(part="id,snippet", q=channel_query, type="channel", maxResults=1)
        if region_code: kwargs_ch["regionCode"] = region_code
        if lang_code:   kwargs_ch["relevanceLanguage"] = lang_code
        ch_resp = youtube.search().list(**kwargs_ch).execute()
        cost_used += 100
    except HttpError as e:
        raise RuntimeError(f"채널 검색 오류: {e}")

    items = ch_resp.get("items", [])
    if not items:
        return [], cost_used
    channel_id = items[0]["id"]["channelId"]

    # 2) 해당 채널의 영상들
    results_tmp = []
    next_token = None
    fetched = 0

    while fetched < max_fetch:
        take = min(50, max_fetch - fetched)
        try:
            kwargs = dict(part="id", type="video", channelId=channel_id, maxResults=take, order="date")
            if published_after:
                kwargs["publishedAfter"] = published_after
            if region_code:
                kwargs["regionCode"] = region_code
            if lang_code:
                kwargs["relevanceLanguage"] = lang_code
            if next_token:
                kwargs["pageToken"] = next_token

            v_search = youtube.search().list(**kwargs).execute()
            cost_used += 100
        except HttpError as e:
            raise RuntimeError(f"채널 영상 검색 오류: {e}")

        page_ids = [it["id"]["videoId"] for it in v_search.get("items", [])
                    if "id" in it and "videoId" in it["id"]]
        if not page_ids:
            break

        try:
            video_response = youtube.videos().list(
                part="snippet,statistics,contentDetails",
                id=",".join(page_ids)
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
            thumbs = snip.get("thumbnails", {})
            thumb_url = (thumbs.get("high", {}) or {}).get("url") \
                        or (thumbs.get("medium", {}) or {}).get("url") \
                        or (thumbs.get("default", {}) or {}).get("url") \
                        or ""
            seconds = parse_duration_iso8601(cdet.get("duration", ""))

            if not duration_filter_ok(seconds, duration_label): 
                continue
            if view_count < min_views: 
                continue

            results_tmp.append({
                "title": title,
                "views": view_count,
                "published_at_iso": published_at_iso,
                "url": url,
                "thumbnail_url": thumb_url,
                "duration_sec": seconds,
                "channel_id": channel_id,
                "channel_title": snip.get("channelTitle", ""),
            })

        fetched += len(page_ids)
        next_token = v_search.get("nextPageToken")
        if not next_token:
            break

    if not results_tmp:
        return [], cost_used

    # 채널 메타
    channels_map = {}
    try:
        ch_resp2 = youtube.channels().list(
            part="snippet,statistics",
            id=channel_id
        ).execute()
        cost_used += 1
        for c in ch_resp2.get("items", []):
            cid = c.get("id")
            cstats = c.get("statistics", {}) or {}
            subs = cstats.get("subscriberCount")
            subs_int = int(subs) if subs is not None else None
            channels_map[cid] = {
                "title": (c.get("snippet", {}) or {}).get("title", ""),
                "subs": subs_int,
                "views": int(cstats.get("viewCount", 0)),
                "videos": int(cstats.get("videoCount", 0)),
            }
    except HttpError:
        channels_map = {}

    results = []
    for r in results_tmp:
        cinfo = channels_map.get(r["channel_id"], {})
        r.update({
            "channel_subs": cinfo.get("subs"),
            "channel_total_views": cinfo.get("views", 0),
            "channel_video_count": cinfo.get("videos", 0),
            "channel_title": cinfo.get("title", r.get("channel_title", "")),
        })
        results.append(r)

    results.sort(key=lambda x: x["views"], reverse=True)
    return results, cost_used

# ----------------------------
# YouTube API: 채널 키워드로 채널 찾기
# ----------------------------
def search_channels_by_keyword(keyword: str, max_fetch: int = 50,
                               region_code: str | None = None, lang_code: str | None = None):
    youtube = get_youtube_client()
    cost_used = 0
    max_fetch = max(1, min(int(max_fetch or 50), 200))

    results = []
    next_token = None
    fetched = 0

    while fetched < max_fetch:
        take = min(50, max_fetch - fetched)
        try:
            kwargs = dict(part="id,snippet", q=keyword, type="channel", maxResults=take)
            if region_code: kwargs["regionCode"] = region_code
            if lang_code:   kwargs["relevanceLanguage"] = lang_code
            if next_token:
                kwargs["pageToken"] = next_token

            resp = youtube.search().list(**kwargs).execute()
            cost_used += 100
        except HttpError as e:
            raise RuntimeError(f"채널 검색 오류: {e}")

        items = resp.get("items", [])
        if not items:
            break

        channel_ids = [it["id"]["channelId"] for it in items
                       if "id" in it and "channelId" in it["id"]]

        try:
            ch_resp = youtube.channels().list(
                part="snippet,statistics",
                id=",".join(channel_ids)
            ).execute()
            cost_used += 1
        except HttpError as e:
            raise RuntimeError(f"채널 상세 조회 오류: {e}")

        for c in ch_resp.get("items", []):
            cid = c.get("id")
            snip = c.get("snippet", {}) or {}
            stats = c.get("statistics", {}) or {}
            subs = stats.get("subscriberCount")
            subs_int = int(subs) if subs is not None else None
            results.append({
                "channel_id": cid,
                "channel_title": snip.get("title", ""),
                "description": snip.get("description", ""),
                "subs": subs_int,
                "total_views": int(stats.get("viewCount", 0)),
                "video_count": int(stats.get("videoCount", 0)),
                "url": f"https://www.youtube.com/channel/{cid}",
            })

        fetched += len(items)
        next_token = resp.get("nextPageToken")
        if not next_token:
            break

    # 구독자 수 기준 정렬
    results.sort(key=lambda x: x.get("subs") or 0, reverse=True)
    return results, cost_used

# ===================================================================
# 아래부터 Streamlit UI
# ===================================================================

st.set_page_config(page_title="YouTube 검색기 (Streamlit)", page_icon="🎬", layout="wide")

# API 키 상태 초기화
init_api_keys_state()

# 세션 상태 기본값
if "results" not in st.session_state:
    st.session_state["results"] = []
if "result_type" not in st.session_state:
    st.session_state["result_type"] = None
if "quota_last_cost" not in st.session_state:
    st.session_state["quota_last_cost"] = 0

# ----------------------------
# 사이드바 (왼쪽)
# ----------------------------
with st.sidebar:
    st.markdown("### 🔍 검색 설정")

    # 일반 검색어
    query = st.text_input("일반 검색어", value="")

    col_btn1, col_btn2 = st.columns(2)
    do_general = col_btn1.button("일반 검색", use_container_width=True)
    do_trend   = col_btn2.button("트렌드 검색", use_container_width=True)  # 입력칸 없는 트렌드 버튼

    st.markdown("---")

    # 채널 키워드로 채널 찾기
    ch_keyword = st.text_input("채널 키워드 (채널 찾기)", value="")
    do_channel_find = st.button("채널 키워드로 채널찾기", use_container_width=True)

    # 채널 검색어로 채널 영상 검색
    ch_exact = st.text_input("채널 검색어 (채널 이름)", value="")
    do_channel_videos = st.button("채널 영상 검색", use_container_width=True)

    st.markdown("---")

    with st.expander("📌 검색 옵션 (기간/길이/지역)", expanded=False):
        period_options = ["30일", "90일", "365일"]
        period_label = st.selectbox("검색기간(서버)", period_options, index=1)

        client_period_options = ["30일", "90일", "365일", "3650일"]
        client_period = st.selectbox("업로드 기간(필터)", client_period_options, index=1)

        dur_options = ["전체","쇼츠","롱폼","1분~20분","20분~40분","40분~60분","60분이상"]
        dur_label = st.selectbox("영상 길이", dur_options, index=0)

        min_views_str = st.selectbox("최소 조회수", ["5,000","10,000","50,000","100,000","500,000","1,000,000"], index=0)
        max_fetch = st.number_input("가져올 최대 개수", min_value=10, max_value=500, value=50, step=10)

        country_label = st.selectbox("국가/언어", COUNTRY_LIST, index=0)
        region_code, lang_code = COUNTRY_LANG_MAP.get(country_label, ("KR","ko"))

    st.markdown("---")

    # 아래쪽: API 키 + 최근 검색 키워드
    st.markdown("### 🔑 YouTube API 키 (아래)")

    existing_keys = API_KEYS_STATE["keys"]
    keys_text_default = "\n".join(existing_keys) if existing_keys else ""
    api_keys_text = st.text_area(
        "API 키 목록 (한 줄에 한 개)",
        value=keys_text_default,
        height=80
    )
    if st.button("API 키 저장", use_container_width=True):
        keys = [line.strip() for line in api_keys_text.splitlines() if line.strip()]
        save_api_keys_from_user(keys, 0)
        st.success("API 키 목록을 저장하고 1번 키를 활성화했습니다. (config.json)")

    st.markdown("---")
    st.markdown("### 🕒 최근 검색 키워드")

    recent = get_recent_keywords(days=14, limit=20)
    if recent:
        for dt_kst, q in recent:
            st.write(f"- {format_k_datetime_simple(dt_kst)} · {q}")
    else:
        st.write("최근 기록 없음")

    st.markdown("---")
    today_total = get_today_quota_total()
    st.caption(f"오늘 사용한 YouTube API 쿼터 추정: {today_total} units\n"
               f"(마지막 검색: {st.session_state['quota_last_cost']} units)")

# ----------------------------
# 메인 영역 (오른쪽)
# ----------------------------
st.title("🎬 YouTube 검색기 (Streamlit 버전)")

# 어떤 버튼이 눌렸는지에 따라 검색 실행
error_msg = None

def _parse_min_views(txt: str) -> int:
    return int(txt.replace(",", "").replace(" ", ""))

def _filter_by_client_period_and_duration(items, client_period_label, dur_label):
    cutoff = cutoff_dt_from_label_kst(client_period_label)
    out = []
    now_kst = datetime.now(KST)
    for r in items:
        pub_kst = parse_published_at_to_kst(r["published_at_iso"])
        if pub_kst < cutoff:
            continue
        if not duration_filter_ok(r["duration_sec"], dur_label):
            continue
        # 시간당 클릭수 계산
        d, h = human_elapsed_days_hours(now_kst, pub_kst)
        total_hours = max(1, d*24 + h)
        r["clicks_per_hour"] = int(round(r["views"] / total_hours))
        r["published_kst"] = pub_kst
        out.append(r)
    return out

try:
    if do_general:
        if not query.strip():
            error_msg = "일반 검색어를 입력하세요."
        else:
            min_views = _parse_min_views(min_views_str)
            results, cost = search_videos(
                query=query.strip(),
                min_views=min_views,
                period_label=period_label,
                duration_label="전체",  # 서버 쿼리는 전체, 클라이언트에서 다시 필터
                max_fetch=max_fetch,
                region_code=region_code,
                lang_code=lang_code
            )
            add_quota_usage(cost)
            st.session_state["quota_last_cost"] = cost

            add_to_history(query.strip())
            append_keyword_log(query.strip())

            filtered = _filter_by_client_period_and_duration(results, client_period, dur_label)

            st.session_state["results"] = filtered
            st.session_state["result_type"] = "general"

    elif do_trend:
        if not query.strip():
            error_msg = "트렌드 검색도 기본 검색어는 필요합니다."
        else:
            min_views = _parse_min_views(min_views_str)
            results, cost = search_videos(
                query=query.strip(),
                min_views=min_views,
                period_label=period_label,
                duration_label="전체",
                max_fetch=max_fetch,
                region_code=region_code,
                lang_code=lang_code
            )
            add_quota_usage(cost)
            st.session_state["quota_last_cost"] = cost

            # 트렌드 표시용 태그
            add_to_history(f"[trend]{query.strip()}")
            append_keyword_log(f"[trend]{query.strip()}")

            filtered = _filter_by_client_period_and_duration(results, client_period, dur_label)

            st.session_state["results"] = filtered
            st.session_state["result_type"] = "trend"

    elif do_channel_find:
        if not ch_keyword.strip():
            error_msg = "채널 키워드를 입력하세요."
        else:
            results, cost = search_channels_by_keyword(
                keyword=ch_keyword.strip(),
                max_fetch=max_fetch,
                region_code=region_code,
                lang_code=lang_code
            )
            add_quota_usage(cost)
            st.session_state["quota_last_cost"] = cost

            add_to_history(f"[channel-find]{ch_keyword.strip()}")
            append_keyword_log(f"[channel-find]{ch_keyword.strip()}")

            st.session_state["results"] = results
            st.session_state["result_type"] = "channel_find"

    elif do_channel_videos:
        if not ch_exact.strip():
            error_msg = "채널 검색어(채널 이름)를 입력하세요."
        else:
            min_views = _parse_min_views(min_views_str)
            results, cost = search_videos_in_channel_by_name(
                channel_query=ch_exact.strip(),
                min_views=min_views,
                period_label=period_label,
                duration_label="전체",
                max_fetch=max_fetch,
                region_code=region_code,
                lang_code=lang_code
            )
            add_quota_usage(cost)
            st.session_state["quota_last_cost"] = cost

            add_to_history(f"[channel]{ch_exact.strip()}")
            append_keyword_log(f"[channel]{ch_exact.strip()}")

            filtered = _filter_by_client_period_and_duration(results, client_period, dur_label)

            st.session_state["results"] = filtered
            st.session_state["result_type"] = "channel_videos"

except Exception as e:
    error_msg = str(e)

if error_msg:
    st.error(error_msg)

# ----------------------------
# 결과 제목 / 리스트 표시
# ----------------------------
result_type = st.session_state.get("result_type")
results = st.session_state.get("results") or []

title_map = {
    "general": "📄 일반 검색 결과 리스트",
    "trend": "📈 트렌드 검색 결과 리스트",
    "channel_find": "📂 채널검색 리스트",
    "channel_videos": "🎞 채널 영상 리스트",
}

if result_type is None or not results:
    st.info("왼쪽에서 검색어를 입력하고 버튼을 눌러 검색을 실행하세요.")
else:
    st.subheader(title_map.get(result_type, "검색 결과 리스트"))

    if result_type in ("general", "trend", "channel_videos"):
        # 영상 결과 테이블
        import pandas as pd
        rows = []
        for r in results:
            pub_kst = r.get("published_kst") or parse_published_at_to_kst(r["published_at_iso"])
            rows.append({
                "제목": r["title"],
                "채널명": r.get("channel_title", ""),
                "조회수": r["views"],
                "시간당 클릭수": r.get("clicks_per_hour", None),
                "영상길이": format_duration_hms(r["duration_sec"]),
                "업로드일(KST)": pub_kst.strftime("%Y-%m-%d"),
                "URL": r["url"],
            })
        df = pd.DataFrame(rows)
        st.dataframe(df, use_container_width=True, height=600)

        # 상단 몇 개 썸네일
        if PIL_AVAILABLE and results:
            st.markdown("#### 대표 썸네일 (상위 3개)")
            thumb_cols = st.columns(min(3, len(results)))
            for i, col in enumerate(thumb_cols):
                r = results[i]
                url = r.get("thumbnail_url")
                if url:
                    with col:
                        st.image(url, use_column_width=True)
                        st.caption(r["title"][:40] + ("..." if len(r["title"]) > 40 else ""))

    elif result_type == "channel_find":
        # 채널 리스트
        import pandas as pd
        rows = []
        for c in results:
            rows.append({
                "채널명": c["channel_title"],
                "구독자수": c["subs"],
                "총조회수": c["total_views"],
                "영상개수": c["video_count"],
                "URL": c["url"],
                "설명": (c["description"] or "")[:120] + ("..." if len(c["description"] or "") > 120 else "")
            })
        df = pd.DataFrame(rows)
        st.dataframe(df, use_container_width=True, height=600)


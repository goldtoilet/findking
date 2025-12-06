
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import json
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
import pandas as pd
import streamlit as st

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# ============================
# 공통 상수/환경
# ============================
KST = timezone(timedelta(hours=9))
WEEKDAY_KO = ["월요일","화요일","수요일","목요일","금요일","토요일","일요일"]

# iCloud 경로 (로컬 맥에서 실행 시)
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

_migrate("~/.yts_config.json",         CONFIG_PATH)
_migrate("~/.yts_search_history.json", HISTORY_PATH)
_migrate("~/.yts_keyword_log.json",    KEYWORD_LOG_PATH)

# ----------------------------
# 국가/언어 선택
# ----------------------------
COUNTRY_LANG_MAP = {
    "한국": ("KR", "ko"),
    "일본": ("JP", "ja"),
    "중국": ("CN", "zh"),
    "대만": ("TW", "zh"),
    "홍콩": ("HK", "zh"),
    "싱가포르": ("SG", "en"),
    "말레이시아": ("MY", "ms"),
    "태국": ("TH", "th"),
    "베트남": ("VN", "vi"),
    "인도": ("IN", "en"),
    "인도네시아": ("ID", "id"),
    "필리핀": ("PH", "en"),
    "미국": ("US", "en"),
    "캐나다": ("CA", "en"),
    "멕시코": ("MX", "es"),
    "브라질": ("BR", "pt"),
    "아르헨티나": ("AR", "es"),
    "칠레": ("CL", "es"),
    "콜롬비아": ("CO", "es"),
    "페루": ("PE", "es"),
    "영국": ("GB", "en"),
    "독일": ("DE", "de"),
    "프랑스": ("FR", "fr"),
    "이탈리아": ("IT", "it"),
    "스페인": ("ES", "es"),
    "포르투갈": ("PT", "pt"),
    "네덜란드": ("NL", "nl"),
    "벨기에": ("BE", "nl"),
    "스웨덴": ("SE", "sv"),
    "노르웨이": ("NO", "no"),
    "덴마크": ("DK", "da"),
    "핀란드": ("FI", "fi"),
    "스위스": ("CH", "de"),
    "오스트리아": ("AT", "de"),
    "아일랜드": ("IE", "en"),
    "폴란드": ("PL", "pl"),
    "체코": ("CZ", "cs"),
    "루마니아": ("RO", "ro"),
    "헝가리": ("HU", "hu"),
    "그리스": ("GR", "el"),
    "터키": ("TR", "tr"),
    "호주": ("AU", "en"),
    "뉴질랜드": ("NZ", "en"),
    "사우디아라비아": ("SA", "ar"),
    "아랍에미리트": ("AE", "ar"),
    "이스라엘": ("IL", "he"),
    "남아프리카공화국": ("ZA", "en"),
    "나이지리아": ("NG", "en"),
    "이집트": ("EG", "ar"),
    "케냐": ("KE", "en"),
    "러시아": ("RU", "ru"),
    "우크라이나": ("UA", "uk"),
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
# API 키 관리
# ----------------------------
ENV_KEY_NAME = "YOUTUBE_API_KEY"

# 기본값: 비워두고, 사용자가 UI에서 입력
_DEFAULT_API_KEYS = []

def _load_api_keys_config():
    data = _load_json(CONFIG_PATH, {})
    keys = [k.strip() for k in (data.get("api_keys") or []) if k.strip()]
    if not keys:
        keys = _DEFAULT_API_KEYS[:]  # 비어있으면 그냥 빈 리스트
    sel = data.get("selected_index", 0)
    sel = max(0, min(sel, len(keys)-1)) if keys else 0
    return {"api_keys": keys, "selected_index": sel}

def _save_api_keys_config(keys, selected_index: int):
    keys = [k.strip() for k in keys if k.strip()]
    if keys:
        selected_index = max(0, min(selected_index, len(keys)-1))
    else:
        selected_index = 0
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

def select_api_key(index: int):
    if not API_KEYS_STATE["keys"]:
        return
    API_KEYS_STATE["index"] = max(0, min(index, len(API_KEYS_STATE["keys"])-1))
    _apply_env_key(API_KEYS_STATE["keys"][API_KEYS_STATE["index"]])
    _save_api_keys_config(API_KEYS_STATE["keys"], API_KEYS_STATE["index"])

def save_api_keys_from_user(keys):
    if not keys:
        return
    _save_api_keys_config(keys, 0)
    cfg = _load_api_keys_config()
    API_KEYS_STATE["keys"] = cfg["api_keys"]
    API_KEYS_STATE["index"] = cfg["selected_index"]
    if API_KEYS_STATE["keys"]:
        _apply_env_key(API_KEYS_STATE["keys"][API_KEYS_STATE["index"]])
    else:
        _apply_env_key("")

def clear_all_api_keys():
    _save_json(CONFIG_PATH, {"api_keys": [], "selected_index": 0})
    API_KEYS_STATE["keys"] = []
    API_KEYS_STATE["index"] = 0
    os.environ.pop(ENV_KEY_NAME, None)

def get_youtube_client():
    if not API_KEYS_STATE["keys"]:
        raise RuntimeError("API 키가 없습니다. 사이드바에서 키를 추가하세요.")
    key = get_current_api_key()
    if not key:
        raise RuntimeError("선택된 API 키가 비어 있습니다.")
    try:
        return build("youtube", "v3", developerKey=key, cache_discovery=False)
    except TypeError:
        return build("youtube", "v3", developerKey=key)

# ----------------------------
# 시간/길이 유틸
# ----------------------------
def format_k_datetime_aw(dt_aw: datetime) -> str:
    if dt_aw.tzinfo is None:
        dt_aw = dt_aw.replace(tzinfo=KST)
    dt = dt_aw.astimezone(KST)
    wd = WEEKDAY_KO[dt.weekday()]
    hour_24 = dt.hour
    ampm = "오전" if hour_24 < 12 else "오후"
    hour_12 = hour_24 % 12 or 12
    return f"{dt.month}월{dt.day}일 {wd} {ampm}{hour_12}시 {dt.minute}분"

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
    if label == "제한없음":
        return None
    now_utc = datetime.now(timezone.utc)
    if label.endswith("시간"):
        hours = int(label[:-2])
        dt = now_utc - timedelta(hours=hours)
    elif label.endswith("일"):
        days = int(label[:-1])
        dt = now_utc - timedelta(days=days)
    else:
        return None
    return dt.isoformat(timespec="seconds").replace("+00:00", "Z")

def cutoff_dt_from_label_kst(label: str) -> datetime:
    label = label.strip()
    now_kst = datetime.now(KST)
    if label.endswith("시간"):
        return now_kst - timedelta(hours=int(label[:-2]))
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

def parse_min_views(text: str) -> int:
    digits = text.replace(",", "").replace(" ", "").replace("만", "0000")
    try:
        return int(digits)
    except Exception:
        return 0

# ----------------------------
# 검색 히스토리 / 키워드 로그
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

def get_recent_keywords(days: int = 14):
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
    return out

# ----------------------------
# YouTube API 검색 함수
# ----------------------------
def _chunked(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i:i+n]

def search_videos(query: str, min_views: int, period_label: str, duration_label: str,
                  max_fetch: int = 200,
                  region_code: str | None = None, lang_code: str | None = None):
    youtube = get_youtube_client()
    published_after = published_after_from_label(period_label)
    cost_used = 0
    breakdown = {"search.list": 0, "videos.list": 0, "channels.list": 0}
    max_fetch = max(1, min(int(max_fetch or 200), 5000))

    results_tmp = []
    channel_ids = set()
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
            cost_used += 100; breakdown["search.list"] += 100
        except HttpError as e:
            raise RuntimeError(f"Search API 오류: {e}")

        page_ids = [
            it["id"]["videoId"] for it in search_response.get("items", [])
            if "id" in it and "videoId" in it["id"]
        ]
        if not page_ids:
            break

        try:
            video_response = youtube.videos().list(
                part="snippet,statistics,contentDetails",
                id=",".join(page_ids)
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
            thumbs = snip.get("thumbnails", {})
            thumb_url = (
                (thumbs.get("maxres", {}) or {}).get("url") or
                (thumbs.get("standard", {}) or {}).get("url") or
                (thumbs.get("high", {}) or {}).get("url") or
                (thumbs.get("medium", {}) or {}).get("url") or
                (thumbs.get("default", {}) or {}).get("url") or
                ""
            )
            seconds = parse_duration_iso8601(cdet.get("duration", ""))

            if not duration_filter_ok(seconds, duration_label):
                continue
            if view_count < min_views:
                continue

            channel_id = snip.get("channelId", "")
            if channel_id:
                channel_ids.add(channel_id)

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
        next_token = search_response.get("nextPageToken")
        if not next_token:
            break

    if not results_tmp:
        return [], cost_used, breakdown

    # 채널 메타
    channels_map = {}
    try:
        for batch in _chunked(list(channel_ids), 50):
            ch_resp = youtube.channels().list(
                part="snippet,statistics",
                id=",".join(batch)
            ).execute()
            cost_used += 1; breakdown["channels.list"] += 1
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
    return results, cost_used, breakdown

def search_videos_in_channel_by_name(channel_query: str, min_views: int, period_label: str, duration_label: str,
                                     max_fetch: int = 200,
                                     region_code: str | None = None, lang_code: str | None = None):
    youtube = get_youtube_client()
    published_after = published_after_from_label(period_label)

    cost_used = 0
    breakdown = {"search.list": 0, "videos.list": 0, "channels.list": 0}
    max_fetch = max(1, min(int(max_fetch or 200), 5000))

    # 1) 채널 찾기
    try:
        kwargs_ch = dict(part="id,snippet", q=channel_query, type="channel", maxResults=1)
        if region_code: kwargs_ch["regionCode"] = region_code
        if lang_code:   kwargs_ch["relevanceLanguage"] = lang_code
        ch_resp = youtube.search().list(**kwargs_ch).execute()
        cost_used += 100; breakdown["search.list"] += 100
    except HttpError as e:
        raise RuntimeError(f"채널 검색 오류: {e}")

    items = ch_resp.get("items", [])
    if not items:
        return [], cost_used, breakdown
    channel_id = items[0]["id"]["channelId"]

    # 2) 채널 내 영상 검색
    results_tmp = []
    next_token = None
    fetched = 0

    while fetched < max_fetch:
        take = min(50, max_fetch - fetched)
        try:
            kwargs = dict(part="id", type="video", channelId=channel_id,
                          maxResults=take, order="date")
            if published_after:
                kwargs["publishedAfter"] = published_after
            if region_code:
                kwargs["regionCode"] = region_code
            if lang_code:
                kwargs["relevanceLanguage"] = lang_code
            if next_token:
                kwargs["pageToken"] = next_token

            v_search = youtube.search().list(**kwargs).execute()
            cost_used += 100; breakdown["search.list"] += 100
        except HttpError as e:
            raise RuntimeError(f"채널 영상 검색 오류: {e}")

        page_ids = [
            it["id"]["videoId"] for it in v_search.get("items", [])
            if "id" in it and "videoId" in it["id"]
        ]
        if not page_ids:
            break

        try:
            video_response = youtube.videos().list(
                part="snippet,statistics,contentDetails",
                id=",".join(page_ids)
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
            thumbs = snip.get("thumbnails", {})
            thumb_url = (
                (thumbs.get("maxres", {}) or {}).get("url") or
                (thumbs.get("standard", {}) or {}).get("url") or
                (thumbs.get("high", {}) or {}).get("url") or
                (thumbs.get("medium", {}) or {}).get("url") or
                (thumbs.get("default", {}) or {}).get("url") or
                ""
            )
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
        return [], cost_used, breakdown

    # 채널 메타
    channels_map = {}
    try:
        ch_resp2 = youtube.channels().list(
            part="snippet,statistics",
            id=channel_id
        ).execute()
        cost_used += 1; breakdown["channels.list"] += 1
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
    return results, cost_used, breakdown

# ----------------------------
# 결과 필터 + 표시용 가공
# ----------------------------
def calc_grade_from_ratio(ratio: float | None) -> str:
    if ratio is None: return "F"
    v = ratio
    if v >= 5000: return "S"
    if 2000 <= v <= 4999: return "A+"
    if 1000 <= v <= 1999: return "A"
    if 500  <= v <=  999: return "B"
    if 300  <= v <=  499: return "C"
    if 100  <= v <=  299: return "D"
    if  50  <= v <=   99: return "E"
    return "F"

def filter_results(all_results,
                   min_views_label: str,
                   client_period_label: str,
                   dur_label: str,
                   grade_label: str):
    min_views = parse_min_views(min_views_label)
    cutoff = cutoff_dt_from_label_kst(client_period_label)
    filtered = []
    now_kst = datetime.now(timezone.utc).astimezone(KST)

    for r in all_results:
        if r["views"] < min_views:
            continue
        pub_kst = parse_published_at_to_kst(r["published_at_iso"])
        if pub_kst < cutoff:
            continue
        if not duration_filter_ok(r["duration_sec"], dur_label):
            continue
        subs = r.get("channel_subs")
        cviews = r.get("channel_total_views", 0)
        ratio_val = (cviews / subs) if isinstance(subs, int) and subs and subs > 0 else None
        grade_val = calc_grade_from_ratio(ratio_val)
        if grade_label != "전체등급" and grade_val != grade_label:
            continue
        filtered.append(r)

    return filtered, now_kst

def build_display_rows(filtered_results, search_dt_kst: datetime):
    rows = []
    for r in filtered_results:
        published_kst = parse_published_at_to_kst(r["published_at_iso"])
        d, h = human_elapsed_days_hours(search_dt_kst, published_kst)
        total_hours = max(1, d*24 + h)
        clicks_per_hour = int(round(r["views"] / total_hours))
        dur_text = format_duration_hms(r["duration_sec"])
        subs = r.get("channel_subs")
        cviews = r.get("channel_total_views", 0)
        ratio_val = (cviews / subs) if isinstance(subs, int) and subs and subs > 0 else None
        grade = calc_grade_from_ratio(ratio_val)

        rows.append({
            "제목": r["title"],
            "채널명": r.get("channel_title", ""),
            "조회수": r["views"],
            "시간당 클릭수": clicks_per_hour,
            "영상길이": dur_text,
            "업로드일": published_kst.strftime("%Y-%m-%d"),
            "경과시간(일)": d,
            "등급": grade,
            "채널 구독자수": subs if subs is not None else None,
            "채널 전체조회수": cviews,
            "채널 영상개수": r.get("channel_video_count", 0),
            "URL": r["url"],
            "썸네일": r["thumbnail_url"],
        })
    return rows

# ----------------------------
# Streamlit UI
# ----------------------------
init_api_keys_state()  # 모듈 로드 시 한 번

def main():
    st.set_page_config(page_title="YouTube 검색기", layout="centered")
    st.title("📺 YouTube 검색기 (Streamlit)")

    # --- 사이드바: API 키 / 쿼터 ---
    with st.sidebar:
        st.header("🔑 API 키 관리")

        keys = API_KEYS_STATE["keys"]
        if keys:
            def _mask_key(k: str) -> str:
                if len(k) <= 12:
                    return k
                return k[:6] + "..." + k[-6:]

            idx_now = API_KEYS_STATE["index"]
            idx = st.selectbox(
                "사용할 키 선택",
                options=list(range(len(keys))),
                index=idx_now,
                format_func=lambda i: f"{i+1}. {_mask_key(keys[i])}",
            )
            if idx != idx_now:
                select_api_key(idx)
                st.experimental_rerun()
        else:
            st.warning("API 키가 없습니다. 아래에서 추가해 주세요.")

        with st.expander("API 키 편집 (한 줄에 한 개)", expanded=not bool(keys)):
            text_default = "\n".join(keys) if keys else ""
            text = st.text_area("API 키 목록", value=text_default, height=120)
            col_a, col_b = st.columns(2)
            if col_a.button("저장", use_container_width=True):
                lines = [l.strip() for l in text.splitlines() if l.strip()]
                if not lines:
                    st.error("최소 1개 이상의 키를 입력하세요.")
                else:
                    save_api_keys_from_user(lines)
                    st.success("API 키 저장 완료 (1번 키 활성화)")
                    st.experimental_rerun()
            if col_b.button("모두 삭제", use_container_width=True):
                clear_all_api_keys()
                st.success("API 키를 모두 삭제했습니다.")
                st.experimental_rerun()

        st.markdown("---")
        today_quota = get_today_quota_total()
        st.caption(f"오늘까지 사용한 YouTube API 쿼터: **{today_quota} 단위**")

    # --- 메인 검색 폼 ---
    st.markdown("### 🔍 검색 조건")

    with st.form("search_form"):
        mode = st.radio(
            "검색 타입",
            ["키워드 검색", "채널 내 검색"],
            horizontal=True,
        )

        if mode == "키워드 검색":
            query = st.text_input("검색어", key="keyword_input")
            channel_query = None
        else:
            channel_query = st.text_input("채널명", key="channel_input")
            query = None

        col1, col2 = st.columns(2)
        with col1:
            min_values = ["5,000","10,000","25,000","50,000","100,000",
                          "200,000","500,000","1,000,000"]
            min_label = st.selectbox("최소 조회수", min_values, index=0)
        with col2:
            country_name = st.selectbox("국가/언어", COUNTRY_LIST, index=COUNTRY_LIST.index("한국"))

        with st.expander("추가 필터", expanded=False):
            col3, col4 = st.columns(2)
            with col3:
                search_period_values = [
                    "제한없음",
                    "90일","150일","200일","365일",
                    "730일","1095일","1825일","3650일"
                ]
                api_period = st.selectbox("서버 검색기간", search_period_values, index=1)
                period_values = [
                    "1일","3일","7일","14일","30일","60일","90일",
                    "150일","200일","365일",
                    "730일","1095일","1825일","3650일"
                ]
                period_label = st.selectbox("업로드 기간 필터", period_values, index=6)
            with col4:
                dur_values = ["전체","쇼츠","롱폼","1분~20분","20분~40분","40분~60분","60분이상"]
                dur_label = st.selectbox("영상 길이", dur_values, index=0)
                grade_values = ["전체등급","S","A+","A","B","C","D","E","F"]
                grade_label = st.selectbox("채널 등급", grade_values, index=0)

            max_fetch = st.number_input("가져올 최대 아이템 수 (1~5000)", min_value=1, max_value=5000, value=50, step=10)

        submitted = st.form_submit_button("검색 실행")

    # --- 검색 실행 ---
    if submitted:
        if not API_KEYS_STATE["keys"]:
            st.error("API 키가 없습니다. 사이드바에서 키를 먼저 설정해 주세요.")
            return

        region_code, lang_code = COUNTRY_LANG_MAP.get(country_name, ("KR", "ko"))

        try:
            if mode == "키워드 검색":
                if not (query or "").strip():
                    st.warning("검색어를 입력하세요.")
                    return
                add_to_history(query)
                append_keyword_log(query)
                results, cost_used, breakdown = search_videos(
                    query=query.strip(),
                    min_views=5000,           # 서버 단계 최소값
                    period_label=api_period,
                    duration_label="전체",    # 클라이언트에서 다시 필터
                    max_fetch=max_fetch,
                    region_code=region_code,
                    lang_code=lang_code,
                )
            else:
                if not (channel_query or "").strip():
                    st.warning("채널명을 입력하세요.")
                    return
                key_for_log = f"[channel]{channel_query.strip()}"
                add_to_history(key_for_log)
                append_keyword_log(key_for_log)
                results, cost_used, breakdown = search_videos_in_channel_by_name(
                    channel_query=channel_query.strip(),
                    min_views=5000,
                    period_label=api_period,
                    duration_label="전체",
                    max_fetch=max_fetch,
                    region_code=region_code,
                    lang_code=lang_code,
                )
        except RuntimeError as e:
            st.error(str(e))
            return

        add_quota_usage(cost_used)
        st.info(f"이번 검색에서 사용된 쿼터: **{cost_used} 단위** &nbsp;&nbsp;|&nbsp;&nbsp; 오늘 누적: **{get_today_quota_total()} 단위**")

        if not results:
            st.warning("조건에 맞는 서버 검색 결과가 없습니다.")
            st.session_state["last_results"] = []
            return

        # 클라이언트 필터 적용
        filtered, search_dt_kst = filter_results(
            results,
            min_views_label=min_label,
            client_period_label=period_label,
            dur_label=dur_label,
            grade_label=grade_label,
        )

        if not filtered:
            st.warning("서버 결과는 있었지만, 현재 필터 조건에 맞는 영상은 없습니다.")
            st.session_state["last_results"] = []
            return

        display_rows = build_display_rows(filtered, search_dt_kst)
        df = pd.DataFrame(display_rows)

        # URL / 썸네일 컬럼은 표에서는 빼고, 개별 미리보기에서 사용
        df_show = df.drop(columns=["URL", "썸네일"])

        st.markdown(f"### 결과: {len(filtered):,}건")
        st.caption("※ 표는 좌우로 스크롤할 수 있습니다 (모바일).")
        st.dataframe(df_show, use_container_width=True)

        st.session_state["last_results"] = display_rows

    # --- 결과 미리보기 ---
    if "last_results" in st.session_state and st.session_state["last_results"]:
        results = st.session_state["last_results"]
        st.markdown("### 🎬 선택한 영상 미리보기")

        options = [f"{i+1}. {row['제목'][:60]}" for i, row in enumerate(results)]
        idx = st.selectbox("미리볼 영상 선택", range(len(results)), format_func=lambda i: options[i])

        row = results[idx]
        st.markdown(f"**제목**: {row['제목']}")
        st.write(f"채널: {row['채널명']}")
        st.write(f"조회수: {row['조회수']:,} | 시간당 클릭수: {row['시간당 클릭수']:,}")
        st.write(f"영상길이: {row['영상길이']} | 업로드일: {row['업로드일']} | 등급: {row['등급']}")

        if row["썸네일"]:
            st.image(row["썸네일"], use_container_width=True)

        st.markdown(f"[YouTube에서 열기 🔗]({row['URL']})")

    # --- 최근 키워드 리스트 ---
    st.markdown("### ⏱ 최근 검색 키워드 (14일)")
    history = get_recent_keywords(days=14)
    if not history:
        st.write("최근 검색 기록이 없습니다.")
    else:
        hist_rows = [{
            "검색시각": format_k_datetime_simple(dt),
            "키워드": q
        } for dt, q in history]
        hist_df = pd.DataFrame(hist_rows)
        st.table(hist_df)

if __name__ == "__main__":
    main()

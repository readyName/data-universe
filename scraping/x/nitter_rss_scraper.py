"""
X/Twitter scraping without Apify: Nitter RSS for discovery + FxTwitter JSON for validation.

- Set ``NITTER_BASE_URL`` (e.g. ``https://nitter.poast.org``). Public Nitter instances
  change often; rotate if RSS fails.
- Validation uses the public FxTwitter-compatible JSON API (no Apify key). Third‑party
  services may rate-limit or change; this is best-effort.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import os
import re
import traceback
import xml.etree.ElementTree as ET
from email.utils import parsedate_to_datetime
from typing import List, Optional, Tuple
from urllib.parse import quote, urlparse

import bittensor as bt
import requests

from common.data import DataEntity, DataSource
from common.protocol import KeywordMode
from scraping.scraper import ScrapeConfig, Scraper, ValidationResult
from scraping.x import utils as xutils
from scraping.x.model import XContent

USER_AGENT = (
    "Mozilla/5.0 (compatible; DataUniverseMiner/1.0; +https://github.com/macrocosm-os/data-universe)"
)


def _sync_get(url: str, timeout: float = 45.0) -> requests.Response:
    return requests.get(
        url,
        timeout=timeout,
        headers={"User-Agent": USER_AGENT, "Accept": "application/json, application/rss+xml, */*"},
    )


async def _aget(url: str, timeout: float = 45.0) -> requests.Response:
    return await asyncio.to_thread(_sync_get, url, timeout)


def _nitter_base() -> str:
    base = os.getenv("NITTER_BASE_URL", "https://nitter.poast.org").rstrip("/")
    return base


def _nitter_link_to_x(url: str) -> str:
    try:
        p = urlparse(url)
        if "nitter" not in p.netloc and "twitter" not in p.netloc:
            return url
        path = p.path
        if "/status/" in path:
            parts = [x for x in path.split("/") if x]
            # .../user/status/id
            if len(parts) >= 3 and parts[-2] == "status":
                user, _, tid = parts[-3], parts[-2], parts[-1]
                return f"https://x.com/{user}/status/{tid}"
    except Exception:
        pass
    return url


def _extract_user_id_from_x_url(url: str) -> Optional[Tuple[str, str]]:
    m = re.match(r"https?://(?:www\.)?(?:x|twitter)\.com/([^/]+)/status/(\d+)", url)
    if not m:
        return None
    return m.group(1), m.group(2)


def _parse_rfc2822_date(s: Optional[str]) -> Optional[dt.datetime]:
    if not s:
        return None
    try:
        return parsedate_to_datetime(s)
    except Exception:
        return None


def _ordered_hashtags(text: str, preferred: str) -> List[str]:
    tags = xutils.extract_hashtags(text)
    pref = preferred.lower()
    lower = [t.lower() for t in tags]
    if pref.startswith("#"):
        target = pref
    else:
        target = f"#{pref}"
    if target.lower() not in lower:
        return [target] + tags
    # Move preferred to front
    match = next((t for t in tags if t.lower() == target.lower()), target)
    rest = [t for t in tags if t.lower() != target.lower()]
    return [match] + rest


class NitterRssTwitterScraper(Scraper):
    """Scrape X via Nitter RSS; validate via FxTwitter JSON."""

    async def scrape(self, scrape_config: ScrapeConfig) -> List[DataEntity]:
        labels = scrape_config.labels or []
        if not labels:
            bt.logging.warning("NitterRssTwitterScraper: no labels; skipping.")
            return []

        label = labels[0].value
        limit = scrape_config.entity_limit or 40
        base = _nitter_base()

        if label.startswith("#"):
            q = label
        elif label.startswith("@"):
            q = label
        else:
            q = label

        rss_url = f"{base}/search/rss?f=tweets&q={quote(q)}"
        bt.logging.info(f"Nitter RSS scrape: {rss_url}")

        try:
            resp = await _aget(rss_url)
            if resp.status_code != 200:
                bt.logging.error(f"Nitter RSS HTTP {resp.status_code} for {rss_url}")
                return []
        except Exception:
            bt.logging.error(f"Nitter RSS fetch failed: {traceback.format_exc()}")
            return []

        try:
            root = ET.fromstring(resp.content)
        except Exception:
            bt.logging.error("Nitter RSS: invalid XML")
            return []

        ns = {"atom": "http://www.w3.org/2005/Atom"}
        items = root.findall(".//item")
        if not items:
            items = root.findall(".//atom:entry", ns)

        dr_start = scrape_config.date_range.start
        dr_end = scrape_config.date_range.end
        if dr_start.tzinfo is None:
            dr_start = dr_start.replace(tzinfo=dt.timezone.utc)
        if dr_end.tzinfo is None:
            dr_end = dr_end.replace(tzinfo=dt.timezone.utc)

        out: List[DataEntity] = []
        for item in items:
            if len(out) >= limit:
                break
            title_el = item.find("title")
            link_el = item.find("link")
            pub_el = item.find("pubDate") or item.find("{http://www.w3.org/2005/Atom}updated")
            title = (title_el.text or "").strip() if title_el is not None else ""
            link = (link_el.text or "").strip() if link_el is not None else ""
            pub_raw = pub_el.text if pub_el is not None else None
            ts = _parse_rfc2822_date(pub_raw) or dt.datetime.now(dt.timezone.utc)

            if ts < dr_start or ts > dr_end:
                continue

            text = title
            if "RT @" in text[:10]:
                continue

            x_url = _nitter_link_to_x(link) if link else ""
            if not xutils.is_valid_twitter_url(x_url):
                continue

            tags = _ordered_hashtags(text, label)
            if label.startswith("#") and label.lower() not in text.lower():
                continue

            user_m = re.search(r"@(\w+)", text)
            username = f"@{user_m.group(1)}" if user_m else "@unknown"
            uid = _extract_user_id_from_x_url(x_url)
            if uid:
                username = f"@{uid[0]}"

            tid = uid[1] if uid else None
            xc = XContent(
                username=username,
                text=xutils.sanitize_scraped_tweet(text),
                url=x_url,
                timestamp=ts,
                tweet_hashtags=tags,
                tweet_id=tid,
                scraped_at=dt.datetime.now(dt.timezone.utc),
            )
            out.append(XContent.to_data_entity(content=xc))

        bt.logging.success(f"Nitter RSS: collected {len(out)} entities for label {label}.")
        return out

    async def validate(self, entities: List[DataEntity]) -> List[ValidationResult]:
        if not entities:
            return []

        async def one(ent: DataEntity) -> ValidationResult:
            if not xutils.is_valid_twitter_url(ent.uri):
                return ValidationResult(
                    is_valid=False,
                    reason="Invalid URI.",
                    content_size_bytes_validated=ent.content_size_bytes,
                )
            parsed = _extract_user_id_from_x_url(xutils.normalize_url(ent.uri))
            if not parsed:
                return ValidationResult(
                    is_valid=False,
                    reason="Could not parse tweet id from URI.",
                    content_size_bytes_validated=ent.content_size_bytes,
                )
            user, tweet_id = parsed
            actual, is_rt = await self._fetch_tweet_via_fxtwitter(user, tweet_id)
            if actual is None:
                return ValidationResult(
                    is_valid=False,
                    reason="Could not fetch tweet for validation (FxTwitter).",
                    content_size_bytes_validated=ent.content_size_bytes,
                )
            return self._validate_tweet_content(
                actual_tweet=actual,
                entity=ent,
                is_retweet=is_rt,
                author_data=None,
                view_count=actual.view_count,
            )

        return await asyncio.gather(*[one(e) for e in entities])

    async def _fetch_tweet_via_fxtwitter(
        self, screen_name: str, tweet_id: str
    ) -> Tuple[Optional[XContent], bool]:
        api = f"https://api.fxtwitter.com/{screen_name}/status/{tweet_id}"
        try:
            resp = await _aget(api)
            if resp.status_code != 200:
                bt.logging.debug(f"FxTwitter HTTP {resp.status_code} for {api}")
                return None, False
            data = resp.json()
        except Exception:
            bt.logging.warning(f"FxTwitter parse error: {traceback.format_exc()}")
            return None, False

        tweet = data.get("tweet") or data
        if not isinstance(tweet, dict):
            return None, False

        text = tweet.get("text") or ""
        is_rt = bool(
            tweet.get("retweeted_tweet")
            or tweet.get("retweeted_status")
            or text.strip().startswith("RT @")
        )
        author = tweet.get("author") or {}
        handle = author.get("screen_name") or author.get("screenName") or screen_name
        url = tweet.get("url") or f"https://x.com/{handle}/status/{tweet_id}"
        created = tweet.get("created_at") or tweet.get("createdAt") or tweet.get("date")
        ts = dt.datetime.now(dt.timezone.utc)
        if isinstance(created, str):
            try:
                ts = parsedate_to_datetime(created)
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=dt.timezone.utc)
            except Exception:
                pass

        tags = xutils.extract_hashtags(text)
        xc = XContent(
            username=f"@{handle}",
            text=xutils.sanitize_scraped_tweet(text),
            url=url,
            timestamp=ts,
            tweet_hashtags=tags or ["#unknown"],
            tweet_id=str(tweet_id),
            user_display_name=author.get("name"),
            user_verified=author.get("verified"),
            like_count=tweet.get("likes") or tweet.get("favorite_count"),
            retweet_count=tweet.get("retweets") or tweet.get("retweet_count"),
            reply_count=tweet.get("replies") or tweet.get("reply_count"),
            view_count=tweet.get("views") or tweet.get("view_count"),
            scraped_at=dt.datetime.now(dt.timezone.utc),
        )
        return xc, is_rt

    def _validate_tweet_content(
        self,
        actual_tweet: XContent,
        entity: DataEntity,
        is_retweet: bool,
        author_data: dict = None,
        view_count: int = None,
    ) -> ValidationResult:
        return xutils.validate_tweet_content(
            actual_tweet=actual_tweet,
            entity=entity,
            is_retweet=is_retweet,
            author_data=author_data,
            view_count=view_count,
        )

    async def on_demand_scrape(
        self,
        usernames: List[str] = None,
        keywords: List[str] = None,
        url: str = None,
        keyword_mode: KeywordMode = "all",
        start_datetime: dt.datetime = None,
        end_datetime: dt.datetime = None,
        limit: int = 100,
    ) -> List[DataEntity]:
        """Best-effort on-demand scrape via Nitter search RSS + date filter."""
        if url:
            parsed = _extract_user_id_from_x_url(url)
            if not parsed:
                return []
            u, tid = parsed
            actual, _ = await self._fetch_tweet_via_fxtwitter(u, tid)
            if actual is None:
                return []
            return [XContent.to_data_entity(content=actual)]

        if not keywords and not usernames:
            return []

        base = _nitter_base()
        parts: List[str] = []
        if usernames:
            for u in usernames:
                u = u.removeprefix("@")
                parts.append(f"from:{u}")
        if keywords:
            if keyword_mode == "all":
                parts.append(" ".join(f'"{k}"' for k in keywords))
            else:
                parts.append(" OR ".join(keywords))

        q = " ".join(parts).strip()
        if not q:
            return []

        rss_url = f"{base}/search/rss?f=tweets&q={quote(q)}"
        try:
            resp = await _aget(rss_url)
            if resp.status_code != 200:
                return []
            root = ET.fromstring(resp.content)
        except Exception:
            return []

        items = root.findall(".//item")
        dr_s = start_datetime or (dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=1))
        dr_e = end_datetime or dt.datetime.now(dt.timezone.utc)
        if dr_s.tzinfo is None:
            dr_s = dr_s.replace(tzinfo=dt.timezone.utc)
        if dr_e.tzinfo is None:
            dr_e = dr_e.replace(tzinfo=dt.timezone.utc)

        out: List[DataEntity] = []
        for item in items:
            if len(out) >= min(limit, 100):
                break
            title_el = item.find("title")
            link_el = item.find("link")
            pub_el = item.find("pubDate")
            title = (title_el.text or "").strip() if title_el is not None else ""
            link = (link_el.text or "").strip() if link_el is not None else ""
            ts = _parse_rfc2822_date(pub_el.text if pub_el is not None else None) or dt.datetime.now(
                dt.timezone.utc
            )
            if ts < dr_s or ts > dr_e:
                continue
            x_url = _nitter_link_to_x(link)
            if not xutils.is_valid_twitter_url(x_url):
                continue
            uid = _extract_user_id_from_x_url(x_url)
            username = f"@{uid[0]}" if uid else "@unknown"
            tid = uid[1] if uid else None
            tags = xutils.extract_hashtags(title)
            xc = XContent(
                username=username,
                text=xutils.sanitize_scraped_tweet(title),
                url=x_url,
                timestamp=ts,
                tweet_hashtags=tags or ["#query"],
                tweet_id=tid,
                scraped_at=dt.datetime.now(dt.timezone.utc),
            )
            out.append(XContent.to_data_entity(content=xc))
        return out

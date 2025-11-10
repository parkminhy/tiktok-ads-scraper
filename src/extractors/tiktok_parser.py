thonimport logging
import time
from typing import Any, Dict, Iterable, List, Optional

import requests

from .utils_format import parse_timestamp_ms, ensure_int, ensure_str

logger = logging.getLogger(__name__)

class TikTokAdsScraper:
    """
    High-level TikTok ad scraper.

    This class assumes there is a TikTok Ad Library-style API that returns
    JSON payloads with ad information. It is designed to be robust even if
    the HTTP request fails or the response structure varies slightly.
    """

    def __init__(
        self,
        base_url: str,
        sleep_between_requests: float = 0.5,
        timeout: float = 10.0,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.sleep_between_requests = sleep_between_requests
        self.session = requests.Session()
        self.timeout = timeout

    def _request_page(
        self,
        query: str,
        region: str,
        page: int,
    ) -> Optional[Dict[str, Any]]:
        """
        Perform an HTTP GET request to fetch a page of ads from the API.
        """
        params = {
            "search_term": query,
            "page": page,
            "region": region,
        }

        try:
            logger.debug("Requesting page %d from %s with params=%s", page, self.base_url, params)
            response = self.session.get(
                self.base_url,
                params=params,
                timeout=self.timeout,
                headers={
                    "User-Agent": (
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/120.0 Safari/537.36"
                    )
                },
            )
            response.raise_for_status()
        except requests.RequestException as exc:
            logger.error("Failed to fetch page %d: %s", page, exc)
            return None

        try:
            data = response.json()
        except ValueError as exc:
            logger.error("Failed to parse JSON from page %d: %s", page, exc)
            return None

        logger.debug("Received payload keys: %s", list(data.keys()))
        return data

    @staticmethod
    def _extract_ads_from_payload(payload: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
        """
        TikTok-like APIs can return ads under different keys. This function
        is defensive and tries several common locations.
        """
        if "data" in payload and isinstance(payload["data"], dict):
            inner = payload["data"]
        else:
            inner = payload

        # Common patterns:
        for key in ("ads", "adList", "items", "records"):
            if key in inner and isinstance(inner[key], list):
                return inner[key]

        # Fallback: if the payload itself is a list
        if isinstance(payload, list):
            return payload

        return []

    @staticmethod
    def _normalize_targeting(targeting_raw: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalize targeting fields into the documented shape.
        """
        location = []
        for loc in targeting_raw.get("locations", []):
            if not isinstance(loc, dict):
                continue
            location.append(
                {
                    "region": ensure_str(loc.get("code", loc.get("region", ""))),
                    "impressions": ensure_str(loc.get("impressions", "")),
                }
            )

        age_ranges = []
        for age in targeting_raw.get("age", []):
            if not isinstance(age, dict):
                continue
            region = ensure_str(age.get("region", ""))
            entry = {"region": region}
            for band in ("13-17", "18-24", "25-34", "35-44", "45-54", "55+"):
                entry[band] = bool(age.get(band, False))
            age_ranges.append(entry)

        gender_ranges = []
        for g in targeting_raw.get("gender", []):
            if not isinstance(g, dict):
                continue
            region = ensure_str(g.get("region", ""))
            gender_ranges.append(
                {
                    "region": region,
                    "female": bool(g.get("female", False)),
                    "male": bool(g.get("male", False)),
                    "unknown": bool(g.get("unknown", False)),
                }
            )

        return {
            "targetingByLocation": location,
            "targetingByAge": age_ranges,
            "targetingByGender": gender_ranges,
        }

    @staticmethod
    def _normalize_ad(raw: Dict[str, Any]) -> Dict[str, Any]:
        """
        Map raw ad payload into the canonical structure described in the README.
        """
        targeting_raw = raw.get("targeting", {}) if isinstance(raw.get("targeting", {}), dict) else {}

        targeting = TikTokAdsScraper._normalize_targeting(targeting_raw)

        ad = {
            "adId": ensure_str(
                raw.get("adId")
                or raw.get("ad_id")
                or raw.get("id")
                or ""
            ),
            "adTitle": ensure_str(
                raw.get("adTitle")
                or raw.get("title")
                or raw.get("ad_title")
                or ""
            ),
            "adType": ensure_str(
                raw.get("adType")
                or raw.get("type")
                or raw.get("ad_type")
                or ""
            ),
            "adVideoUrl": ensure_str(
                raw.get("adVideoUrl")
                or raw.get("video_url")
                or raw.get("creative_url")
                or ""
            ),
            "adVideoCover": ensure_str(
                raw.get("adVideoCover")
                or raw.get("thumbnail_url")
                or raw.get("cover_url")
                or ""
            ),
            "adStartDate": parse_timestamp_ms(
                raw.get("adStartDate")
                or raw.get("start_time")
                or raw.get("startDate")
            ),
            "adEndDate": parse_timestamp_ms(
                raw.get("adEndDate")
                or raw.get("end_time")
                or raw.get("endDate")
            ),
            "advertiserId": ensure_str(
                raw.get("advertiserId")
                or raw.get("advertiser_id")
                or raw.get("account_id")
                or ""
            ),
            "advertiserName": ensure_str(
                raw.get("advertiserName")
                or raw.get("advertiser_name")
                or raw.get("account_name")
                or ""
            ),
            "adImpressions": ensure_str(
                raw.get("adImpressions")
                or raw.get("impressions")
                or raw.get("impression_range")
                or ""
            ),
            "advertiserPaidForBy": ensure_str(
                raw.get("advertiserPaidForBy")
                or raw.get("paid_for_by")
                or ""
            ),
            "adTotalRegions": ensure_int(
                raw.get("adTotalRegions")
                or raw.get("total_regions")
                or len(targeting.get("targetingByLocation", []))
            ),
            "adEstimatedAudience": ensure_str(
                raw.get("adEstimatedAudience")
                or raw.get("estimated_audience")
                or ""
            ),
        }

        ad.update(targeting)
        return ad

    def scrape(
        self,
        query: str,
        region: str,
        max_pages: int = 1,
    ) -> List[Dict[str, Any]]:
        """
        High-level scraping orchestration: iterate pages and normalize all ads.
        """
        all_ads: List[Dict[str, Any]] = []
        logger.info(
            "Scraping TikTok ads: query=%r region=%r pages=%d", query, region, max_pages
        )

        for page in range(1, max_pages + 1):
            payload = self._request_page(query=query, region=region, page=page)
            if payload is None:
                logger.warning("Stopping pagination due to request failure on page %d", page)
                break

            raw_ads = list(self._extract_ads_from_payload(payload))
            logger.info("Page %d returned %d raw ads", page, len(raw_ads))

            if not raw_ads:
                logger.info("No ads on page %d; stopping pagination.", page)
                break

            for raw in raw_ads:
                if not isinstance(raw, dict):
                    logger.debug("Skipping non-dict ad entry: %r", raw)
                    continue
                try:
                    normalized = self._normalize_ad(raw)
                    all_ads.append(normalized)
                except Exception as exc:  # noqa: BLE001
                    logger.exception("Failed to normalize ad entry: %s", exc)

            if page < max_pages:
                time.sleep(self.sleep_between_requests)

        logger.info("Total normalized ads: %d", len(all_ads))
        return all_ads
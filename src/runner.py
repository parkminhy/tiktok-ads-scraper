thonimport argparse
import json
import logging
import os
import sys
import time
from pathlib import Path

from .extractors.tiktok_parser import TikTokAdsScraper
from .outputs.exporters import export_ads

def configure_logging(verbosity: int) -> None:
    level = logging.WARNING
    if verbosity == 1:
        level = logging.INFO
    elif verbosity >= 2:
        level = logging.DEBUG

    logging.basicConfig(
        level=level,
        format="[%(asctime)s] [%(levelname)s] %(name)s - %(message)s",
    )

def load_settings(settings_path: Path) -> dict:
    if not settings_path.exists():
        logging.warning(
            "Settings file %s not found. Using internal defaults.", settings_path
        )
        # Fallback default configuration
        return {
            "base_url": "https://ads.tiktok.com/your-ad-library-endpoint",
            "default_query": "",
            "default_region": "GB",
            "default_pages": 1,
            "default_output_format": "json",
            "output_dir": "data",
            "sleep_between_requests": 0.5,
        }

    try:
        with settings_path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except json.JSONDecodeError as exc:
        logging.error("Failed to parse settings file: %s", exc)
        raise SystemExit(1)

def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="TikTok Ads Scraper - scrape TikTok ad library data."
    )
    parser.add_argument(
        "--settings",
        type=str,
        default=str(Path(__file__).parent / "config" / "settings.example.json"),
        help="Path to the settings JSON file.",
    )
    parser.add_argument(
        "--query",
        type=str,
        default=None,
        help="Search query for ads (advertiser name, domain, keyword).",
    )
    parser.add_argument(
        "--region",
        type=str,
        default=None,
        help="Region / country code to filter ads (e.g. GB, US).",
    )
    parser.add_argument(
        "--pages",
        type=int,
        default=None,
        help="Number of pages to scrape (pagination).",
    )
    parser.add_argument(
        "--format",
        dest="output_format",
        type=str,
        default=None,
        choices=["json", "csv", "xml"],
        help="Output format for exported ads.",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Output file path. If omitted, generated automatically.",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=0,
        help="Increase logging verbosity (use -vv for debug).",
    )
    return parser

def main(argv=None) -> None:
    argv = argv or sys.argv[1:]
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    configure_logging(args.verbose)
    logger = logging.getLogger("runner")

    settings_path = Path(args.settings).resolve()
    logger.info("Loading settings from %s", settings_path)
    settings = load_settings(settings_path)

    base_url = settings.get("base_url")
    if not base_url:
        logger.error("Missing 'base_url' in settings.")
        raise SystemExit(1)

    query = args.query if args.query is not None else settings.get("default_query", "")
    region = args.region if args.region is not None else settings.get("default_region", "GB")
    pages = args.pages if args.pages is not None else int(settings.get("default_pages", 1))
    output_format = (
        args.output_format
        if args.output_format is not None
        else settings.get("default_output_format", "json").lower()
    )
    if output_format not in {"json", "csv", "xml"}:
        logger.error("Unsupported output format: %s", output_format)
        raise SystemExit(1)

    sleep_between = float(settings.get("sleep_between_requests", 0.5))

    output_dir = (
        Path(args.output).parent
        if args.output is not None
        else Path(settings.get("output_dir", "data"))
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    if args.output is not None:
        output_path = Path(args.output)
    else:
        timestamp = int(time.time())
        output_path = output_dir / f"tiktok_ads_{timestamp}.{output_format}"

    logger.info(
        "Starting TikTok ads scraping: query=%r region=%r pages=%d format=%s",
        query,
        region,
        pages,
        output_format,
    )

    scraper = TikTokAdsScraper(
        base_url=base_url,
        sleep_between_requests=sleep_between,
    )

    ads = scraper.scrape(query=query, region=region, max_pages=pages)

    if not ads:
        logger.warning("No ads were scraped. Nothing to export.")
    else:
        logger.info("Scraped %d ads. Exporting to %s", len(ads), output_path)
        export_ads(ads, output_format=output_format, output_file=output_path)
        logger.info("Export completed successfully.")

    logger.info("Done.")

if __name__ == "__main__":
    main()
thonimport csv
import json
import logging
from pathlib import Path
from typing import Any, Dict, Iterable, List
import xml.etree.ElementTree as ET

logger = logging.getLogger(__name__)

def _ensure_list_of_dicts(ads: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [dict(ad) for ad in ads]

def _export_json(ads: List[Dict[str, Any]], output_file: Path) -> None:
    with output_file.open("w", encoding="utf-8") as f:
        json.dump(ads, f, ensure_ascii=False, indent=2)

def _export_csv(ads: List[Dict[str, Any]], output_file: Path) -> None:
    if not ads:
        logger.warning("No ads to export to CSV.")
        with output_file.open("w", encoding="utf-8", newline="") as f:
            f.write("")
        return

    # Gather all keys to create a consistent CSV header
    fieldnames = set()
    for ad in ads:
        fieldnames.update(ad.keys())
    fieldnames = sorted(fieldnames)

    with output_file.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for ad in ads:
            writer.writerow(ad)

def _export_xml(ads: List[Dict[str, Any]], output_file: Path) -> None:
    root = ET.Element("ads")

    for ad in ads:
        ad_el = ET.SubElement(root, "ad")
        for key, value in ad.items():
            field_el = ET.SubElement(ad_el, key)

            # Serialize nested structures (lists/dicts) as JSON strings
            if isinstance(value, (list, dict)):
                field_el.text = json.dumps(value, ensure_ascii=False)
            else:
                field_el.text = "" if value is None else str(value)

    tree = ET.ElementTree(root)
    tree.write(output_file, encoding="utf-8", xml_declaration=True)

def export_ads(
    ads: Iterable[Dict[str, Any]],
    output_format: str,
    output_file: Path,
) -> None:
    """
    Export ads into the requested format.

    :param ads: Iterable of normalized ad dictionaries.
    :param output_format: 'json', 'csv', or 'xml'.
    :param output_file: Destination file path.
    """
    ads_list = _ensure_list_of_dicts(ads)
    output_file = Path(output_file)
    output_file.parent.mkdir(parents=True, exist_ok=True)

    output_format = output_format.lower()
    logger.debug(
        "Exporting %d ads to %s (format=%s)", len(ads_list), output_file, output_format
    )

    if output_format == "json":
        _export_json(ads_list, output_file)
    elif output_format == "csv":
        _export_csv(ads_list, output_file)
    elif output_format == "xml":
        _export_xml(ads_list, output_file)
    else:
        raise ValueError(f"Unsupported output format: {output_format}")
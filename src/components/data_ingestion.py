import os
import csv
import glob
import time
import re
import httpx
from selectolax.parser import HTMLParser
from urllib.parse import urljoin
from dataclasses import dataclass, fields, asdict
from typing import Optional

# Shared dataclass & helpers

@dataclass
class Property:
    title: str
    location: str
    spec: str
    price: str
    description: Optional[str]
    features: Optional[list]


FIELD_NAMES = [f.name for f in fields(Property)]


def _sanitize_row(row: dict) -> dict:
    """Convert lists to semicolon-separated strings & strip newlines"""
    out = {}
    for k, v in row.items():
        if isinstance(v, list):
            out[k] = "; ".join(str(item).strip() for item in v)
        elif v is None:
            out[k] = ""
        else:
            out[k] = (
                str(v)
                .replace("\r\n", " ")
                .replace("\n", " ")
                .replace("\r", " ")
                .strip()
            )
    return out


def export_batch(rows, batch_num: int, prefix: str):
    """Export each batch of properties to csv files"""
    filename = f"{prefix}_batch_{batch_num:03d}.csv"

    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELD_NAMES, lineterminator="\n")
        writer.writeheader()
        for row in rows:
            d = asdict(row) if not isinstance(row, dict) else row
            writer.writerow(_sanitize_row(d))
    print(f"Saved {len(rows)} properties → {filename}")


def combine_all_batches(output_file: str = "all_properties.csv"):
    """Combine every batch file from both scrapers into one dataset"""
    batch_files = sorted(
        glob.glob("pp_batch_*.csv") + glob.glob("npc_batch_*.csv")
    )

    if not batch_files:
        print("No batch files found to combine.")
        return

    total = 0
    with open(output_file, "w", newline="", encoding="utf-8") as out:
        writer = None
        for bf in batch_files:
            with open(bf, encoding="utf-8") as f:
                reader = csv.DictReader(f)
                if writer is None:
                    writer = csv.DictWriter(
                        out, fieldnames=reader.fieldnames, lineterminator="\n"
                    )
                    writer.writeheader()
                for row in reader:
                    writer.writerow(row)
                    total += 1
    print(f"\nCombined {total:,} properties → {output_file}")

    if input("Delete batch files? (y/n): ").strip().lower() == "y":
        for bf in batch_files:
            os.remove(bf)
            print(f"Deleted {bf}")

# Shared HTTP helper

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


def fetch(url: str, retries: int = 3, timeout: int = 25) -> Optional[HTMLParser]:
    """Fetch the URL with retry & return parsed HTML or None"""
    for attempt in range(retries):
        try:
            resp = httpx.get(
                url, headers=HEADERS, follow_redirects=True, timeout=timeout
            )
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            return HTMLParser(resp.text)
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:
                wait = 30 * (attempt + 1)
                print(f"[rate-limited] waiting {wait}s")
                time.sleep(wait)
            else:
                print(f"[HTTP {e.response.status_code}] {url}")
                return None
        except Exception as e:
            wait = 5 * (attempt + 1)
            if attempt < retries - 1:
                print(f"[attempt {attempt+1} failed: {e}] retrying in {wait}s")
                time.sleep(wait)
            else:
                print(f"[failed after {retries} attempts] {url}: {e}")
                return None
    return None


# PropertyPro scraper

def _pp_text(node):
    if not node:
        return "N/A"
    if isinstance(node, list):
        return node[0].text().strip() if node else "N/A"
    return node.text().strip()


def _pp_nodes_texts(nodes):
    return [n.text().strip() for n in nodes] if nodes else []


def pp_parse_listing(html):
    """Extract property detail urls from the main listing page"""
    for prop in html.css("div.property-listing"):
        a = prop.css_first("a")
        if a and a.attributes.get("href"):
            yield urljoin("https://propertypro.ng", a.attributes["href"])


def pp_parse_detail(html):
    """Extract property details from each detail page"""
    title = _pp_text(html.css_first("div.col-md-8 h1"))

    location = "N/A"
    for block in html.css("div.content-block.position-relative"):
        for p in block.css("p"):
            if p.css_first("i.fa-location-dot") or p.css_first(
                "i.fa-solid.fa-location-dot"
            ):
                icon = p.css_first("i")
                loc_text = p.text().strip()
                if icon:
                    loc_text = loc_text.replace(icon.text().strip(), "").strip()
                location = loc_text
                break
        if location != "N/A":
            break

    spec_nodes = html.css("div.property-pros ul li")
    spec_list = _pp_nodes_texts(spec_nodes)[:3]
    spec = " | ".join(spec_list) if spec_list else "N/A"

    strongs = html.css("div.pricing h2 strong")
    if len(strongs) >= 2:
        price = strongs[1].text().strip()
    elif strongs:
        price = strongs[0].text().strip()
    else:
        price = "N/A"

    desc_node = html.css_first("div.des-inner.font-16.line-paragraph")
    description = desc_node.text().strip() if desc_node else "N/A"

    feature_nodes = html.css("div.amen-grid a")
    features = _pp_nodes_texts(feature_nodes) if feature_nodes else None

    return Property(
        title=title,
        location=location,
        spec=spec,
        price=price,
        description=description,
        features=features,
    )


def scrape_propertypro():
    """Scrape the listings in batches"""
    baseurl = "https://propertypro.ng/property-for-rent/in/lagos?page="
    max_pages = 800
    batch_size = 50
    batch_num = 16
    current_batch = []
    current_page = 800
    failed_urls = []
    total = 0

    print(f"Batch size: {batch_size} pages per batch\n")

    while current_page <= max_pages:
        for page_num in range(
            current_page, min(current_page + batch_size, max_pages + 1)
        ):
            print(f"\n[PP Page {page_num}] Fetching listing…")
            html = fetch(f"{baseurl}{page_num}")
            if not html:
                print("No more listings")
                current_page = max_pages + 1
                break

            prop_urls = list(pp_parse_listing(html))
            print(f"Found {len(prop_urls)} properties")

            for idx, url in enumerate(prop_urls, 1):
                print(f"[{idx}/{len(prop_urls)}] {url}")
                detail = fetch(url)
                if not detail:
                    failed_urls.append(url)
                    continue
                try:
                    prop = pp_parse_detail(detail)
                    current_batch.append(prop)
                    total += 1
                except Exception as e:
                    print(f"parse error: {e.__class__.__name__}")
                    failed_urls.append(url)
                time.sleep(1)

        if current_batch:
            export_batch(current_batch, batch_num, prefix="pp")
            current_batch = []
        batch_num += 1
        current_page += batch_size

    print(f"\nPropertyPro done: {total} properties collected")
    if failed_urls:
        print(f"Failed: {len(failed_urls)} URLs")
        with open("pp_failed_urls.txt", "w") as f:
            f.write("\n".join(failed_urls))
    return total

# NigeriaPropertyCentre scraper

NPC_BASE = "https://nigeriapropertycentre.com"
NPC_LISTING = "https://nigeriapropertycentre.com/for-rent/lagos"
NPC_REQUEST_DELAY = 1.2


def _npc_text(node) -> str:
    if not node:
        return "N/A"
    if isinstance(node, list):
        return node[0].text(strip=True) if node else "N/A"
    return node.text(strip=True)


def _npc_texts(nodes) -> list:
    return [n.text(strip=True) for n in nodes] if nodes else []


def _npc_clean(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()


def npc_extract_listing_urls(html: HTMLParser) -> list:
    seen, urls = set(), []
    for a in html.css("h3.listings-property-title a, h4 > a"):
        href = a.attributes.get("href", "")
        if href and re.search(r"/\d{5,}-", href):
            full = href if href.startswith("http") else NPC_BASE + href
            if full not in seen:
                seen.add(full)
                urls.append(full)
    if not urls:
        for a in html.css("a[href]"):
            href = a.attributes.get("href", "")
            if re.search(r"/for-rent/.+/\d{5,}-", href):
                full = href if href.startswith("http") else NPC_BASE + href
                if full not in seen:
                    seen.add(full)
                    urls.append(full)
    return urls


def npc_has_next_page(html: HTMLParser, current_page: int) -> bool:
    for a in html.css("a[href]"):
        href = a.attributes.get("href", "")
        if f"page={current_page + 1}" in href:
            return True
    total_text = _npc_text(html.css_first("h2"))
    if total_text and "N/A" not in total_text:
        m = re.search(r"([\d,]+)\s+available", total_text, re.I)
        if m:
            total = int(m.group(1).replace(",", ""))
            return current_page * 21 < total
    return False


def npc_parse_detail(html: HTMLParser, url: str) -> Optional[Property]:
    title = "N/A"
    for sel in [
        "h4.content-title", "h3.content-title", "h1.content-title",
        "h4.detail-title", "h1", "h3", "h4",
    ]:
        node = html.css_first(sel)
        if node:
            t = _npc_clean(node.text())
            if t and t not in ("N/A", ""):
                title = t
                break

    location = "N/A"
    map_tab = html.css_first("#tab-map .tab-body p")
    if map_tab:
        raw = map_tab.text(strip=True)
        t = _npc_clean(re.sub(r"\s+", " ", raw))
        if len(t) > 5:
            location = t

    if location == "N/A":
        breadcrumbs = html.css(
            "ol.breadcrumb li, ul.breadcrumb li, [class*='breadcrumb'] a"
        )
        if len(breadcrumbs) >= 3:
            parts = [_npc_clean(b.text()) for b in breadcrumbs[2:] if _npc_clean(b.text())]
            if parts:
                location = ", ".join(parts)

    if location == "N/A":
        for sel in ["address", "[class*='location']", "[class*='address']"]:
            for n in html.css(sel):
                t = _npc_clean(n.text())
                if t and len(t) > 5 and "Lagos" in t:
                    location = t
                    break
            if location != "N/A":
                break

    if location == "N/A":
        url_parts = url.rstrip("/").split("/")
        try:
            lagos_idx = url_parts.index("lagos")
            area_parts = url_parts[lagos_idx : lagos_idx + 3]
            location = " ".join(
                p.replace("-", " ").title() for p in area_parts if p
            )
        except (ValueError, IndexError):
            pass

    price = "N/A"
    for sel in [
        "p.price", "h3.price", "div.price", "span.price",
        "[class*='price']", "strong", "b",
    ]:
        for node in html.css(sel):
            t = _npc_clean(node.text())
            if re.search(r"[₦N]\s*[\d,]+", t):
                price = t
                break
        if price != "N/A":
            break

    if price == "N/A":
        page_text = html.body.text() if html.body else ""
        m = re.search(
            r"(₦[\d,]+(?:\.\d+)?\s*(?:per\s+\w+|p\.a\.|monthly|annually)?)",
            page_text,
            re.I,
        )
        if m:
            price = _npc_clean(m.group(1))

    def _extract_num(text):
        m = re.search(r"(\d+)", text)
        return int(m.group(1)) if m else None

    bed_val = bath_val = toilet_val = parking_val = None

    for td in html.css("table.table-bordered td, table.table-striped td"):
        label_node = td.css_first("strong")
        if not label_node:
            continue
        label = label_node.text(strip=True).lower().rstrip(":")
        full = _npc_clean(td.text())
        value_text = full.replace(label_node.text(strip=True), "").strip()

        if "bedroom" in label and bed_val is None:
            bed_val = _extract_num(value_text)
        elif "bathroom" in label and bath_val is None:
            bath_val = _extract_num(value_text)
        elif "toilet" in label and toilet_val is None:
            toilet_val = _extract_num(value_text)
        elif "parking" in label and parking_val is None:
            parking_val = _extract_num(value_text)

    if bed_val is None and bath_val is None:
        for ul in html.css("ul"):
            items = [_npc_clean(li.text()) for li in ul.css("li")]
            bed_items = [i for i in items if re.search(r"\d+\s*bedroom", i, re.I)]
            bath_items = [i for i in items if re.search(r"\d+\s*bathroom", i, re.I)]
            toilet_items = [i for i in items if re.search(r"\d+\s*toilet", i, re.I)]
            if bed_items or bath_items or toilet_items:
                bed_val = _extract_num(" ".join(bed_items)) or bed_val
                bath_val = _extract_num(" ".join(bath_items)) or bath_val
                toilet_val = _extract_num(" ".join(toilet_items)) or toilet_val
                p_items = [
                    i
                    for i in items
                    if re.search(r"parking|car\s*space", i, re.I)
                ]
                parking_val = _extract_num(" ".join(p_items)) or parking_val
                break

    spec_parts = []
    if bed_val is not None:
        spec_parts.append(f"{bed_val} Bed{'s' if bed_val != 1 else ''}")
    if bath_val is not None:
        spec_parts.append(f"{bath_val} Bath{'s' if bath_val != 1 else ''}")
    if toilet_val is not None:
        spec_parts.append(f"{toilet_val} Toilet{'s' if toilet_val != 1 else ''}")
    spec = " | ".join(spec_parts) if spec_parts else "N/A"

    description = "N/A"
    desc_node = html.css_first("p[itemprop='description']")
    if desc_node:
        t = _npc_clean(desc_node.text())
        if len(t) > 20:
            description = t

    if description == "N/A":
        for sel in [
            "div[itemprop='description']", "div.description",
            "div.property-description", "div[class*='description']",
            "div.detail-description",
        ]:
            node = html.css_first(sel)
            if node:
                t = _npc_clean(node.text())
                if len(t) > 30:
                    description = t
                    break

    features = None
    amenity_keywords = {
        "pool", "gym", "parking", "boys quarter", "bq", "elevator", "lift",
        "security", "generator", "air condition", "ac", "wifi", "internet",
        "water", "borehole", "furnished", "serviced", "newly built",
        "pop ceiling", "wardrobe", "kitchen", "laundry", "balcony",
        "gate", "fence", "cctv", "intercom", "paved", "tiled",
    }
    for sel in [
        "ul.features-list", "ul.property-features", "div.features ul",
        "div.property-features ul", "div[class*='feature'] ul",
        "div[class*='amenity'] ul", "div[class*='facilities'] ul",
    ]:
        nodes = html.css(f"{sel} li")
        if nodes:
            items = _npc_texts(nodes)
            filtered = [
                i for i in items
                if i and len(i) < 60
                and any(k in i.lower() for k in amenity_keywords)
            ]
            if filtered:
                features = filtered
                break

    if features is None:
        for ul in html.css("ul"):
            items = [_npc_clean(li.text()) for li in ul.css("li")]
            matched = [
                i for i in items if i and any(k in i.lower() for k in amenity_keywords)
            ]
            if len(matched) >= 2:
                features = matched
                break

    return Property(
        title=title,
        location=location,
        spec=spec,
        price=price,
        description=description,
        features=features,
    )


def scrape_npc():
    """Scrape NigeriaPropertyCentre listings in batches."""
    max_pages = 1500
    batch_size = 50
    batch_num = 1
    batch_rows = []
    failed_urls = []
    total = 0
    consecutive_empty = 0

    print(f"Batch size: {batch_size} pages per batch\n")

    for page in range(1, max_pages + 1):
        page_url = f"{NPC_LISTING}?page={page}"
        print(f"\n[NPC Page {page:>4}] {page_url}")

        html = fetch(page_url)
        if html is None:
            print(f"Failed to fetch page {page} — skipping.")
            consecutive_empty += 1
            if consecutive_empty >= 3:
                print("3 consecutive failures — stopping.")
                break
            continue

        prop_urls = npc_extract_listing_urls(html)
        if not prop_urls:
            print(f"No property URLs found on page {page}.")
            consecutive_empty += 1
            if consecutive_empty >= 3:
                print("3 consecutive empty pages — stopping.")
                break
            continue

        consecutive_empty = 0
        print(f"Found {len(prop_urls)} properties")

        for i, url in enumerate(prop_urls, 1):
            print(f"[{i:>2}/{len(prop_urls)}] {url.split('/')[-1][:55]}")
            detail_html = fetch(url)
            if detail_html is None:
                failed_urls.append(url)
                continue
            try:
                prop = npc_parse_detail(detail_html, url)
                row = asdict(prop)
                batch_rows.append(row)
                total += 1
                print(
                    f"{prop.price}  |  {prop.spec}  |  {prop.location[:40]}"
                )
            except Exception as e:
                print(f"Parse error: {e}")
                failed_urls.append(url)
            time.sleep(NPC_REQUEST_DELAY)

        if page % batch_size == 0 and batch_rows:
            export_batch(batch_rows, batch_num, prefix="npc")
            batch_rows = []
            batch_num += 1

        if not npc_has_next_page(html, page):
            print(f"\nNo next page after page {page} — done.")
            break

        time.sleep(NPC_REQUEST_DELAY)

    if batch_rows:
        export_batch(batch_rows, batch_num, prefix="npc")

    print(f"\nNPC done: {total} properties collected")
    if failed_urls:
        print(f"Failed: {len(failed_urls)} URLs")
        with open("npc_failed_urls.txt", "w") as f:
            f.write("\n".join(failed_urls))
    return total


# Main entry point

def main():
    pp_total = scrape_propertypro()
    npc_total = scrape_npc()

    print(f"TOTAL:  PropertyPro={pp_total}  |  NPC={npc_total}")

    combine_all_batches()


if __name__ == "__main__":
    main()

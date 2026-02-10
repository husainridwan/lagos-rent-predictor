import os
import glob
import csv
import time
import json
import httpx
from selectolax.parser import HTMLParser
from urllib.parse import urljoin
from dataclasses import asdict, dataclass, fields
from pprint import pprint
from typing import Optional

@dataclass
class Property:
    title: str
    location: str
    spec: str
    price: str
    description: str | None
    features: list | None


def _text(node):
    if not node:
        return "N/A"
    if isinstance(node, list):
        return node[0].text().strip() if node else "N/A"
    return node.text().strip()


def _nodes_texts(nodes):
    return [n.text().strip() for n in nodes] if nodes else []

def get_url(url, **kwargs) -> Optional[HTMLParser]:
    """Fetch URL with retry logic."""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    
    full_url = f"{url}{kwargs.get('page', '')}"
    
    for attempt in range(3):
        try:
            response = httpx.get(
                full_url,
                headers=headers,
                follow_redirects=True,
                timeout=20
            )
            response.raise_for_status()
            return HTMLParser(response.text)
            
        except Exception as e:
            if attempt < 2:
                time.sleep(3 * (attempt + 1))
            else:
                print(f"Failed to fetch {full_url}: {e}")
                return None
    
    return None

def parse_page(html):
    properties = html.css("div.property-listing")

    for prop in properties:
        a = prop.css_first("a")
        if a and a.attributes.get("href"):
            yield urljoin("https://propertypro.ng", a.attributes["href"])

def parse_property(html):
    title = _text(html.css_first("div.col-md-8 h1"))
    location = "N/A"
    for block in html.css("div.content-block.position-relative"):
        for p in block.css("p"):
            if p.css_first("i.fa-location-dot") or p.css_first("i.fa-solid.fa-location-dot"):
                icon = p.css_first("i")
                loc_text = p.text().strip()
                if icon:
                    loc_text = loc_text.replace(icon.text().strip(), "").strip()
                location = loc_text
                break
        if location != "N/A":
            break

    spec_nodes = html.css("div.property-pros ul li")
    spec_list = _nodes_texts(spec_nodes)[:3]
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
    features = _nodes_texts(feature_nodes) if feature_nodes else None

    return Property(
        title=title,
        location=location,
        spec=spec,
        price=price,
        description=description,
        features=features,
    )  

def export_to_csv(props, batch_num=None):
    field_names = [field.name for field in fields(Property)]
    
    if batch_num is not None:
        filename = f"properties_batch_{batch_num}.csv"
    else:
        filename = "properties.csv"
    
    with open(filename, "w", newline='', encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=field_names)
        writer.writeheader()
        for prop in props:
            writer.writerow(prop)
    
    if batch_num:
        print(f"Batch {batch_num} exported to {filename}")
    else:
        print("Data exported to properties.csv")

def combine_batch_csvs():
    batch_files = sorted(glob.glob("properties_batch_*.csv"), key=lambda x: int(x.split('_')[2].split('.')[0]))
    
    if not batch_files:
        print("No batch files found to combine")
        return
    
    print(f"\nCombining {len(batch_files)} batch files...")
    
    with open("properties.csv", "w", newline='', encoding="utf-8") as outfile:
        writer = None
        row_count = 0
        
        for batch_file in batch_files:
            print(f"Processing {batch_file}...")
            with open(batch_file, "r", encoding="utf-8") as infile:
                reader = csv.DictReader(infile)
                
                if writer is None:
                    fieldnames = reader.fieldnames
                    writer = csv.DictWriter(outfile, fieldnames=fieldnames)
                    writer.writeheader()
                
                for row in reader:
                    writer.writerow(row)
                    row_count += 1
    
    print(f"\n Combined {row_count} properties into properties.csv")

    delete_batches = input("\nDelete batch files after combining? (y/n): ").lower().strip()
    if delete_batches == 'y':
        for batch_file in batch_files:
            os.remove(batch_file)
            print(f"  Deleted {batch_file}")

def main():
    baseurl = "https://propertypro.ng/property-for-rent/in/lagos?page="
    failed_urls = []
    total_properties = 0
    batch_size = 20
    batch_num = 1
    current_batch_properties = []
    current_page = 1
    max_pages = 800
    
    print("\n" + "="*70)
    print("Starting Property Scraper (Batch Mode)")
    print(f"Batch size: {batch_size} pages per batch")
    print("="*70 + "\n")
    
    while current_page <= max_pages:
        batch_start_page = current_page
        batch_end_page = min(current_page + batch_size - 1, max_pages)
        
        print(f"\n[BATCH {batch_num}] Processing pages {batch_start_page}-{batch_end_page}")
        print("-" * 70)
        
        for page_num in range(batch_start_page, batch_end_page + 1):
            print(f"\n[Page {page_num}] Fetching listing...")
            html = get_url(baseurl, page=page_num)
            
            if html is False:
                print(f"  Stopping - no more properties available")
                current_page = max_pages + 1  
                break
                
            prop_urls = list(parse_page(html))
            print(f"Found {len(prop_urls)} properties on this page")
            
            for idx, url in enumerate(prop_urls, 1):
                print(f"[{idx}/{len(prop_urls)}] Fetching property...")
                html = get_url(url)
                
                if html is False:
                    print(f"Skipped (too many timeouts)")
                    failed_urls.append(url)
                    continue
                    
                try:
                    prop = parse_property(html)
                    current_batch_properties.append(prop)
                    total_properties += 1
                    print(f" {prop.title} - {prop.price}")
                except Exception as e:
                    print(f" Error parsing property: {type(e).__name__}")
                    failed_urls.append(url)

                time.sleep(1)
        
        if current_batch_properties:
            print(f"\n[BATCH {batch_num}] Saving {len(current_batch_properties)} properties...")
            export_to_csv([asdict(prop) for prop in current_batch_properties], batch_num=batch_num)
            current_batch_properties = []
        
        batch_num += 1
        current_page = batch_end_page + 1
    
    print("\n" + "="*70)
    print(f"Scraping Complete: {total_properties} total properties collected")
    if failed_urls:
        print(f"Failed to collect: {len(failed_urls)} properties")
    print("="*70)
    
    combine_batch_csvs()

if __name__ == "__main__":
    main()

"""
Data Transformation — Lagos Rental Property Cleaning Pipeline

Reads the raw scraped CSV (from PropertyPro + NigeriaPropertyCentre),
filters to residential rentals, normalises prices/specs/areas,
engineers amenity features, and saves a model-ready dataset.
"""

import re
import numpy as np
import pandas as pd


# ══════════════════════════════════════════════════════════════════════════════
# 1. Sale / non-rental detection
# ══════════════════════════════════════════════════════════════════════════════

_SALE_TITLE_RE = re.compile(
    r"\b("
    r"for sale|outright sale|sale only|units of|units for sale|"
    r"shortlet|short let|short-let|short stay|airbnb|"
    r"new development|off-plan|land for|land sale|"
    r"warehouse|factory|filling station|plaza|hotel|church|school|"
    r"co-working|co working"
    r")\b",
    re.IGNORECASE,
)

_SALE_DESC_RE = re.compile(
    r"\b("
    r"for sale|outright sale|asking price|sale price|purchase price|"
    r"investor|investment opportunity|buy now|shortlet|short let|"
    r"short-let|holiday|vacation rental"
    r")\b",
    re.IGNORECASE,
)

_RENTAL_SUFFIX_RE = re.compile(
    r"/(year|annum|month|monthly|annually|p\.a)"
    r"|per\s+(annum|year|month|monthly|annually)"
    r"|p\.a\b"
    r"|\bmonthly\b"
    r"|\bannually\b",
    re.IGNORECASE,
)


def is_for_sale(row: pd.Series) -> bool:
    """Return True if a listing looks like a sale or shortlet, not a rental."""
    title = str(row.get("title", "") or "").strip()
    desc = str(row.get("description", "") or "").strip()
    price = str(row.get("price", "") or "").strip()

    if _SALE_TITLE_RE.search(title):
        return True
    if _SALE_DESC_RE.search(desc):
        return True
    if price and not _RENTAL_SUFFIX_RE.search(price):
        return True
    return False


# ══════════════════════════════════════════════════════════════════════════════
# 2. Property-type classification
# ══════════════════════════════════════════════════════════════════════════════

_TYPE_MAP = [
    ("duplex",        "Duplex"),
    ("storey",        "Duplex"),
    ("terrace",       "Terrace"),
    ("maisonette",    "Maisonette"),
    ("townhouse",     "Townhouse"),
    ("penthouse",     "Penthouse"),
    ("villa",         "Villa"),
    ("semi-detached", "Semi-Detached"),
    ("semi detached", "Semi-Detached"),
]

_DETACHED_RE = re.compile(r"\bdetached\b", re.IGNORECASE)
_SEMI_RE = re.compile(r"\bsemi\b", re.IGNORECASE)

_APARTMENT_RE = re.compile(
    r"\b(apartment|flat|bungalow|bedroom|house|master|room|studio|bed)\b",
    re.IGNORECASE,
)

_NON_RES_RE = re.compile(
    r"\b(office|commercial|shop|warehouse|factory|plaza|hotel|"
    r"church|school|filling station|co-working)\b",
    re.IGNORECASE,
)

_SELF_CONTAIN_RE = re.compile(
    r"self.?contain|single.?room|room.?only|bedsitter|bedsit",
    re.IGNORECASE,
)


def classify_property_type(title: str) -> str | None:
    """
    Map a listing title to a canonical property type.

    Returns one of: Apartment, Duplex, Terrace, Maisonette, Townhouse,
    Penthouse, Villa, Semi-Detached, Detached — or None if the listing
    is commercial, self-contained, or unrecognisable.
    """
    if pd.isna(title):
        return None

    text = str(title)

    if _NON_RES_RE.search(text):
        return None
    if _SELF_CONTAIN_RE.search(text):
        return None

    lower = text.lower()
    for keyword, label in _TYPE_MAP:
        if keyword in lower:
            return label

    if _DETACHED_RE.search(text) and not _SEMI_RE.search(text):
        return "Detached"

    if _APARTMENT_RE.search(text):
        return "Apartment"

    return None


# ══════════════════════════════════════════════════════════════════════════════
# 3. Price cleaning
# ══════════════════════════════════════════════════════════════════════════════

# Matches monthly indicators in both PP and NPC formats
_MONTHLY_RE = re.compile(
    r"/month|/monthly|per\s+month|\bmonthly\b",
    re.IGNORECASE,
)

# Extracts the numeric part from prices like "20,000,000/year" or "₦2,000,000"
_PRICE_NUM_RE = re.compile(r"[\d,]+(?:\.\d+)?")


def clean_price(price_str: str) -> float:
    """
    Parse a price string into an annual rent figure (NGN).

    Handles formats from both PropertyPro ("20,000,000/year") and
    NigeriaPropertyCentre ("₦2,000,000 per annum").  Monthly prices
    are multiplied by 12.  Returns NaN if the value is outside the
    plausible Lagos-rent range of 100 000–500 000 000 NGN/year.
    """
    if pd.isna(price_str):
        return np.nan

    raw = str(price_str).strip()

    # Pull the first number-like substring
    match = _PRICE_NUM_RE.search(raw)
    if not match:
        return np.nan

    try:
        value = float(match.group().replace(",", ""))
    except ValueError:
        return np.nan

    # Annualise if monthly
    if _MONTHLY_RE.search(raw):
        value *= 12

    # Sanity bounds for Lagos annual rent
    if value < 100_000 or value > 500_000_000:
        return np.nan

    return value


# ══════════════════════════════════════════════════════════════════════════════
# 4. Spec parsing
# ══════════════════════════════════════════════════════════════════════════════

def parse_spec(spec: str) -> dict:
    """
    Split a spec string like '3 Beds | 2 Baths | 3 Toilets' into a dict
    with keys bedrooms, bathrooms, toilets.  Missing parts become NaN.
    """
    result = {"bedrooms": np.nan, "bathrooms": np.nan, "toilets": np.nan}
    if pd.isna(spec):
        return result

    for part in str(spec).split("|"):
        part = part.strip().lower()
        m = re.search(r"(\d+)", part)
        if not m:
            continue
        n = int(m.group(1))
        if "bed" in part:
            result["bedrooms"] = n
        elif "bath" in part:
            result["bathrooms"] = n
        elif "toilet" in part:
            result["toilets"] = n

    return result


def extract_beds_from_title(title: str) -> float:
    """
    Fallback: pull bedroom count from the listing title.

    '3 Bedroom Apartment' → 3, 'Studio' → 1, otherwise NaN.
    """
    if pd.isna(title):
        return np.nan
    lower = str(title).lower()
    if "studio" in lower:
        return 1
    m = re.search(r"(\d+)\s*(?:bed|bedroom|br\b)", lower)
    return int(m.group(1)) if m else np.nan


# ══════════════════════════════════════════════════════════════════════════════
# 5. Location / area extraction
# ══════════════════════════════════════════════════════════════════════════════

def extract_area(location: str) -> str:
    """
    Extract area name from location string.
    Returns the last two sub-areas separated by a comma (if available).
    
    'Off Isaac John Street, Ikeja Gra, Ikeja' -> 'Ikeja Gra, Ikeja'
    """
    if pd.isna(location):
        return np.nan

    s = str(location).strip()

    # Removing trailing ', Lagos' or ' Lagos'
    s = re.sub(r',?\s*Lagos\s*$', '', s, flags=re.IGNORECASE)
    s = s.strip().rstrip(',')

    if not s:
        return np.nan

    parts = [p.strip().title() for p in s.split(',') if p.strip()]
    if len(parts) >= 2:
        return f"{parts[-2]}, {parts[-1]}"
    elif len(parts) == 1:
        return parts[0]
    
    return np.nan


# ══════════════════════════════════════════════════════════════════════════════
# 6. Amenity extraction
# ══════════════════════════════════════════════════════════════════════════════

_AMENITY_PATTERNS = {
    "has_pool":       re.compile(r"pool|swimming",             re.I),
    "has_gym":        re.compile(r"\bgym\b|fitness",           re.I),
    "has_parking":    re.compile(r"parking|garage",            re.I),
    "has_bq":         re.compile(r"\bbq\b|boys.quarter",       re.I),
    "has_elevator":   re.compile(r"elevator|lift",             re.I),
    "is_newly_built": re.compile(r"newly.built|brand.new",     re.I),
    "is_furnished":   re.compile(r"\bfurnished\b",             re.I),
    "is_serviced":    re.compile(r"serviced|service.charge",   re.I),
    "has_security":   re.compile(r"security",                  re.I),
    "has_generator":  re.compile(r"generator",                 re.I),
    "has_ac":         re.compile(r"air.condition|\bac\b",      re.I),
    "has_wifi":       re.compile(r"wi-?fi|internet",           re.I),
    "has_water":      re.compile(r"water supply|borehole|water.tank", re.I),
}


def extract_amenities(description: str, features: str) -> dict:
    """
    Scan the description and features text for amenity keywords.

    Returns a dict of int flags (0/1) for each amenity.
    """
    text = (
        (str(description) if pd.notna(description) else "")
        + " "
        + (str(features) if pd.notna(features) else "")
    ).lower()

    return {
        name: int(bool(pattern.search(text)))
        for name, pattern in _AMENITY_PATTERNS.items()
    }


# ══════════════════════════════════════════════════════════════════════════════
# 7. Spec imputation helpers
# ══════════════════════════════════════════════════════════════════════════════

_RESIDENTIAL_TYPES = [
    "Apartment", "Duplex", "Terrace", "Maisonette", "Townhouse",
    "Detached", "Semi-Detached", "Villa", "Penthouse",
]


def _impute_specs(data: pd.DataFrame) -> pd.DataFrame:
    """
    Fill missing bedroom / bathroom / toilet counts using sensible
    fallbacks, then clip to physically plausible ranges.
    """
    # Fallback: pull bedrooms from title
    title_beds = data["title"].apply(extract_beds_from_title)
    data["bedrooms"] = data["bedrooms"].fillna(title_beds)

    # For residential types with known bedrooms but missing bathrooms,
    # assume bathrooms ≈ bedrooms
    residential_mask = data["property_type"].isin(_RESIDENTIAL_TYPES)
    fill_bath = residential_mask & data["bedrooms"].notna() & data["bathrooms"].isna()
    data.loc[fill_bath, "bathrooms"] = data.loc[fill_bath, "bedrooms"]

    # Group-median imputation, then global-median fallback
    for col in ["bedrooms", "bathrooms", "toilets"]:
        type_median = data.groupby("property_type")[col].transform("median")
        data[col] = data[col].fillna(type_median)
        data[col] = data[col].fillna(data[col].median())

    # Clip to sensible ranges
    data["bedrooms"]  = data["bedrooms"].clip(1, 8)
    data["bathrooms"] = data["bathrooms"].clip(1, 8)
    data["toilets"]   = data["toilets"].clip(1, 10)

    return data


# ══════════════════════════════════════════════════════════════════════════════
# 8. Final column selection
# ══════════════════════════════════════════════════════════════════════════════

FINAL_COLUMNS = [
    "title", "area", "property_type", "price_annual",
    "bedrooms", "bathrooms", "toilets",
    "has_pool", "has_gym", "has_parking", "has_bq", "has_elevator",
    "is_newly_built", "is_furnished", "is_serviced",
    "has_security", "has_generator", "has_ac", "has_wifi", "has_water",
]


# ══════════════════════════════════════════════════════════════════════════════
# 9. Main pipeline
# ══════════════════════════════════════════════════════════════════════════════

def run(input_path: str, output_path: str) -> pd.DataFrame:
    """
    Execute the full cleaning pipeline.

    1. Load raw CSV
    2. Drop exact duplicates
    3. Classify property type → keep residential only
    4. Filter out sale / shortlet listings
    5. Clean price → price_annual, drop rows without a valid price
    6. Parse spec → bedrooms, bathrooms, toilets (with imputation)
    7. Extract area from location
    8. Extract amenity flags from description + features
    9. Drop near-duplicate rows (same title + price + area)
    10. Select final columns and save

    Returns the cleaned DataFrame for inspection.
    """
    # ── Load ──────────────────────────────────────────────────────────────
    raw = pd.read_csv(input_path)
    print(f"Loaded {len(raw):,} raw listings from {input_path}")

    data = raw.copy()

    # ── Dedup ─────────────────────────────────────────────────────────────
    before = len(data)
    data.drop_duplicates(inplace=True)
    print(f"Dropped {before - len(data):,} exact duplicates → {len(data):,} remain")

    # ── Property type ─────────────────────────────────────────────────────
    data["property_type"] = data["title"].apply(classify_property_type)
    non_residential = data["property_type"].isna().sum()
    data = data[data["property_type"].notna()].copy()
    print(f"Removed {non_residential:,} non-residential listings → {len(data):,} remain")

    # ── Sale / shortlet filter ────────────────────────────────────────────
    sale_mask = data.apply(is_for_sale, axis=1)
    data = data[~sale_mask].copy()
    print(f"Removed {sale_mask.sum():,} sale/shortlet listings → {len(data):,} remain")

    # ── Price ─────────────────────────────────────────────────────────────
    data["price_annual"] = data["price"].apply(clean_price)
    no_price = data["price_annual"].isna().sum()
    data.dropna(subset=["price_annual"], inplace=True)
    print(f"Dropped {no_price:,} rows with invalid price → {len(data):,} remain")
    print(
        f"Price range: {data['price_annual'].min():,.0f}"
        f" – {data['price_annual'].max():,.0f} NGN/year"
    )

    # ── Spec → bedrooms / bathrooms / toilets ─────────────────────────────
    spec_cols = data["spec"].apply(parse_spec).apply(pd.Series)
    data[["bedrooms", "bathrooms", "toilets"]] = spec_cols
    data = _impute_specs(data)
    print(f"Spec summary:\n{data[['bedrooms', 'bathrooms', 'toilets']].describe().round(1)}")

    # ── Area ──────────────────────────────────────────────────────────────
    data["area"] = data["location"].apply(extract_area)

    # ── Amenities ─────────────────────────────────────────────────────────
    amenity_rows = [
        extract_amenities(row["description"], row["features"])
        for _, row in data.iterrows()
    ]
    amenity_df = pd.DataFrame(amenity_rows, index=data.index)
    data = pd.concat([data, amenity_df], axis=1)

    # ── Near-duplicate removal ────────────────────────────────────────────
    before = len(data)
    data.drop_duplicates(subset=["title", "price_annual", "area"], inplace=True)
    print(f"Dropped {before - len(data):,} near-duplicates → {len(data):,} remain")

    # ── Select & save ─────────────────────────────────────────────────────
    clean = data[FINAL_COLUMNS].copy()
    clean.to_csv(output_path, index=False)
    print(f"\nSaved {len(clean):,} cleaned listings → {output_path}")

    return clean


# ── CLI entry point ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys

    src = sys.argv[1] if len(sys.argv) > 1 else "notebooks/all_properties.csv"
    dst = sys.argv[2] if len(sys.argv) > 2 else "notebooks/properties_cleaned.csv"
    run(src, dst)

import os
import re
import time
import datetime
import logging
from datetime import timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Set

import requests
from dotenv import load_dotenv


# ----------------------------
# FORCE-LOAD .env FROM THIS SCRIPT'S FOLDER (VS CODE SAFE)
# ----------------------------
ENV_PATH = Path(__file__).resolve().parent / ".env"
loaded = load_dotenv(dotenv_path=ENV_PATH, override=True)
print("Loaded .env:", loaded, "from", str(ENV_PATH))


# ----------------------------
# ENV HELPERS (identical to shopify-adjust-orders-v2.py)
# ----------------------------
def env_first(*names: str, default: Optional[str] = None) -> Optional[str]:
    for name in names:
        value = os.getenv(name)
        if value is not None and str(value).strip() != "":
            return value.strip()
    return default


def env_bool(*names: str, default: bool = False) -> bool:
    value = env_first(*names)
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def env_int(*names: str, default: int) -> int:
    value = env_first(*names)
    if value is None:
        return default
    return int(str(value).strip())


def env_decimal(*names: str, default: str) -> Decimal:
    value = env_first(*names)
    try:
        return Decimal(str(value if value is not None else default))
    except (InvalidOperation, ValueError, TypeError):
        return Decimal(default)


def parse_draft_order_names(raw: Optional[str]) -> List[str]:
    if not raw:
        return []
    text = str(raw).strip()
    if text in {"[]", '[""]', "['']"}:
        return []
    if text.startswith("[") and text.endswith("]"):
        text = text[1:-1].strip()
    if not text:
        return []
    parts = [x.strip().strip('"').strip("'") for x in text.split(",")]
    return [x for x in parts if x]


def parse_csv_set(raw: Optional[str], *, casefold: bool = False) -> Set[str]:
    if not raw:
        return set()
    vals = []
    for part in str(raw).split(","):
        v = part.strip()
        if not v:
            continue
        vals.append(v.casefold() if casefold else v)
    return set(vals)


def normalize_text(value: Any) -> str:
    text = "" if value is None else str(value)
    text = text.strip().casefold()
    text = re.sub(r"\s+", " ", text)
    return text


def contains_any_substring(haystack: str, needles: Set[str]) -> List[str]:
    if not haystack or not needles:
        return []
    return [n for n in sorted(needles) if n and n in haystack]


def to_decimal(value: Any, default: str = "0") -> Decimal:
    try:
        if value is None or value == "":
            return Decimal(default)
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return Decimal(default)


# ----------------------------
# ENV CONFIG
# ----------------------------
SHOP = env_first("SHOPIFY_SHOP", "SHOPIFY_STORE")
TOKEN = env_first("SHOPIFY_ADMIN_ACCESS_TOKEN", "SHOPIFY_TOKEN")
API_VERSION = env_first("SHOPIFY_API_VERSION", "API_VERSION", default="2025-07")
LOCATION_ID = env_first("SHOPIFY_LOCATION_ID", "LOCATION_ID")

DRAFT_ORDER_NAMES = parse_draft_order_names(env_first("DRAFT_ORDER_NAMES"))

DRY_RUN = env_bool("DRY_RUN", default=True)
MAX_DRAFTS = env_int("MAX_DRAFTS", default=250)
LOG_LEVEL = (env_first("LOG_LEVEL", default="INFO") or "INFO").upper()

# Same knobs as shopify-adjust-orders-v2.py, intentionally the same env var
# names so a single GitHub repo-variable change updates both scripts at once.
MIN_SPLIT_VALUE = env_decimal("MIN_SPLIT_VALUE", default="150")
MIN_BACKORDER_HOLD_VALUE = env_decimal("MIN_BACKORDER_HOLD_VALUE", default="75")

# Value-band tags. SPLIT_150_TAG doubles as this script's ENTIRE query
# filter (see build_open_ended_query) -- any backorder-descended draft,
# regardless of generation, gets exactly one of these two tags once at
# creation and it is never touched again. split-remainder drafts are
# excluded from this pipeline for good the moment they're created: a
# draft whose own total is already under MIN_SPLIT_VALUE can never
# produce a keep-side that clears the gate on a further split.
SPLIT_REMAINDER_TAG = env_first("SPLIT_REMAINDER_TAG", default="split-remainder") or "split-remainder"
SPLIT_150_TAG = env_first("SPLIT_150_TAG", default="split-150") or "split-150"

LAUNCH_TAG_PREFIX = (env_first("LAUNCH_TAG_PREFIX", default="launch-") or "launch-").casefold()

# This script's own concurrency lock. Deliberately separate from v2-processing
# so the two pipelines can never contend over the same draft.
PROCESSING_TAG = env_first("BO_PROCESSING_TAG", default="bo-split-processing") or "bo-split-processing"

NEEDS_REVIEW_TAG = env_first("NEEDS_REVIEW_TAG", default="needs-review") or "needs-review"

# Generation display tags: split1 -> split2 -> split3 ... Purely descriptive/
# reporting. NOT used for querying (see SPLIT_150_TAG above) and NOT used to
# gate any decision -- only to label which generation a draft is on, mirroring
# the PO suffix depth.
GENERATION_TAG_RE = re.compile(r"^split(\d+)$", re.IGNORECASE)


def generation_tag_for(n: int) -> str:
    return f"split{n}"


def current_generation_number(tags: List[str]) -> int:
    for t in tags or []:
        m = GENERATION_TAG_RE.match(str(t).strip())
        if m:
            return int(m.group(1))
    # Should never happen for a draft that matched tag:split-150 in the
    # query -- every backorder descendant gets a generation tag at
    # creation. Defaulting to 1 is a safe fallback, not an expected path.
    return 1


# PO suffix: flat sequential, no cap. "PO123" -> "PO123 - BO1" -> "PO123 - BO2" ...
PO_SUFFIX_RE = re.compile(r"^(.*?)\s*-\s*BO(\d+)$", re.IGNORECASE)


def build_next_po_number(current_po: str) -> str:
    base = (current_po or "").strip()
    match = PO_SUFFIX_RE.match(base)
    if match:
        root, depth = match.group(1).strip(), int(match.group(2))
        return f"{root} - BO{depth + 1}"
    return f"{base} - BO1" if base else "BACKORDER-1"


# Exact-match / substring customer exclusions -- same mechanism/defaults as
# shopify-adjust-orders-v2.py, applied identically here.
EXCLUDED_CUSTOMERS = parse_csv_set(env_first("EXCLUDED_CUSTOMERS", default=""), casefold=True)
DEFAULT_EXCLUDED_SUBSTRINGS = {
    "faire",
    "faire marketplace",
    "customer samples",
    "tjx canada",
    "tjx companies",
    "replacements and customer care",
    "replacements customer care customer care",
    "noreen batdorf",
    "norman's hallmark",
}
EXCLUDED_CUSTOMER_SUBSTRINGS = parse_csv_set(
    env_first("EXCLUDED_CUSTOMER_SUBSTRINGS", default=""),
    casefold=True,
) or set()
EXCLUDED_CUSTOMER_SUBSTRINGS = set(EXCLUDED_CUSTOMER_SUBSTRINGS).union(DEFAULT_EXCLUDED_SUBSTRINGS)

# Tags that can cause other automations to convert a draft into an order.
# Stripped before duplicating, same reasoning as v2.
CONVERSION_TRIGGER_TAGS = parse_csv_set(
    env_first("CONVERSION_TRIGGER_TAGS", default="instock-ready"),
    casefold=False,
)

# Linking fields. LINK_CUSTOM_ATTR_* / ORIGINAL_DRAFT_ID_METAFIELD_* carry the
# TRUE ROOT split0 order's PO/draft id forward through every generation,
# never the immediate parent's. PO_METAFIELD_* is different: it mirrors this
# draft's OWN (suffixed) PO number, same as v2.
LINK_CUSTOM_ATTR_PO_KEY = env_first("LINK_CUSTOM_ATTR_PO_KEY", default="original_poNumber") or "original_poNumber"
LINK_CUSTOM_ATTR_DRAFTID_KEY = env_first("LINK_CUSTOM_ATTR_DRAFTID_KEY", default="original_draft_id") or "original_draft_id"

PO_METAFIELD_NAMESPACE = env_first("PO_METAFIELD_NAMESPACE", default="b2b") or "b2b"
PO_METAFIELD_KEY = env_first("PO_METAFIELD_KEY", default="po_number") or "po_number"
PO_METAFIELD_TYPE = env_first("PO_METAFIELD_TYPE", default="single_line_text_field") or "single_line_text_field"

ORIGINAL_DRAFT_ID_METAFIELD_NAMESPACE = env_first("ORIGINAL_DRAFT_ID_METAFIELD_NAMESPACE", default="custom") or "custom"
ORIGINAL_DRAFT_ID_METAFIELD_KEY = env_first("ORIGINAL_DRAFT_ID_METAFIELD_KEY", default="original_draft_id") or "original_draft_id"
ORIGINAL_DRAFT_ID_METAFIELD_TYPE = env_first("ORIGINAL_DRAFT_ID_METAFIELD_TYPE", default="single_line_text_field") or "single_line_text_field"

# ASSUMPTION (flagged, not confirmed in design discussion): carrying forward
# the legacy partial-instock-split.py's 7-day ship-date window, looser than
# shopify-adjust-orders-v2.py's same-day-or-past rule. If this should instead
# be dropped, or match the stricter v2 rule, this is the one place to change.
SHIP_DATE_METAFIELD_NAMESPACE = env_first("SHIP_DATE_METAFIELD_NAMESPACE", default="b2b") or "b2b"
SHIP_DATE_METAFIELD_KEY = env_first("SHIP_DATE_METAFIELD_KEY", default="ship_date") or "ship_date"
SHIP_DATE_WINDOW_DAYS = env_int("SHIP_DATE_WINDOW_DAYS", default=7)

print("SHOPIFY_SHOP =", SHOP)
print("API_VERSION  =", API_VERSION)
print("DRY_RUN =", DRY_RUN)
print("MIN_SPLIT_VALUE (keep-side gate) =", MIN_SPLIT_VALUE)
print("MIN_BACKORDER_HOLD_VALUE (backorder hold gate) =", MIN_BACKORDER_HOLD_VALUE)
print("SPLIT_150_TAG (also = query pool filter) =", SPLIT_150_TAG)
print("SPLIT_REMAINDER_TAG =", SPLIT_REMAINDER_TAG)
print("LAUNCH_TAG_PREFIX =", LAUNCH_TAG_PREFIX)
print("PROCESSING_TAG =", PROCESSING_TAG)
print("NEEDS_REVIEW_TAG =", NEEDS_REVIEW_TAG)
print("SHIP_DATE_WINDOW_DAYS =", SHIP_DATE_WINDOW_DAYS)

if not SHOP or not TOKEN:
    raise SystemExit(
        "Missing shop/token env vars. Accepted names:\n"
        "  SHOPIFY_SHOP or SHOPIFY_STORE\n"
        "  SHOPIFY_ADMIN_ACCESS_TOKEN or SHOPIFY_TOKEN"
    )
if not LOCATION_ID:
    raise SystemExit("Missing location env var. Accepted names:\n  SHOPIFY_LOCATION_ID or LOCATION_ID")

GRAPHQL_URL = f"https://{SHOP}/admin/api/{API_VERSION}/graphql.json"

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("partial-instock-split-v2")


def normalize_draft_name(name: str) -> str:
    if not name:
        return ""
    s = str(name).strip()
    s = s.replace("Draft", "").strip()
    if s.startswith("#"):
        s = s[1:]
    return s.strip().upper()


def candidate_customer_labels(draft: Dict[str, Any]) -> List[str]:
    out: List[str] = []
    for addr_key in ("shippingAddress", "billingAddress"):
        addr = draft.get(addr_key) or {}
        for field in ("company", "name"):
            v = addr.get(field)
            if v:
                out.append(str(v))
    email = draft.get("email")
    if email:
        out.append(str(email))
    return out


def is_excluded_draft(draft: Dict[str, Any]) -> Tuple[bool, List[str]]:
    customer_candidates = candidate_customer_labels(draft)
    customer_candidate_norms = {normalize_text(x) for x in customer_candidates}
    exact_matches = sorted(customer_candidate_norms.intersection(EXCLUDED_CUSTOMERS))

    blob = normalize_text(" | ".join(customer_candidates))
    substring_matches = contains_any_substring(blob, EXCLUDED_CUSTOMER_SUBSTRINGS)

    reasons: List[str] = []
    if exact_matches:
        reasons.append(f"exact customer match: {', '.join(exact_matches)}")
    if substring_matches:
        reasons.append(f"substring match: {', '.join(substring_matches)}")
    return bool(exact_matches or substring_matches), reasons


def build_draft_name_query(names: List[str]) -> str:
    vals: List[str] = []
    seen = set()
    for n in names:
        raw = str(n).strip()
        if not raw:
            continue
        base = raw.lstrip("#").strip()
        for c in (raw, base, f"#{base}"):
            c = c.strip()
            if not c:
                continue
            key = c.lower()
            if key in seen:
                continue
            seen.add(key)
            vals.append(c)
    parts = []
    for v in vals:
        parts.append(f'name:"{v}"')
        if "#" not in v:
            parts.append(f"name:{v}")
    return " OR ".join(parts)


# ----------------------------
# GRAPHQL
# ----------------------------
def gql(query: str, variables: Optional[Dict[str, Any]] = None, *, attempts: int = 5) -> Dict[str, Any]:
    headers = {"Content-Type": "application/json", "X-Shopify-Access-Token": TOKEN}
    last_err: Optional[Exception] = None
    for i in range(attempts):
        try:
            resp = requests.post(
                GRAPHQL_URL,
                headers=headers,
                json={"query": query, "variables": variables or {}},
                timeout=60,
            )
            if resp.status_code in (429, 503):
                sleep_s = min(2 ** i, 10)
                logger.warning("Throttled (HTTP %s). Sleeping %ss and retrying...", resp.status_code, sleep_s)
                time.sleep(sleep_s)
                continue
            if resp.status_code != 200:
                raise RuntimeError(f"HTTP {resp.status_code} calling Shopify GraphQL.\nResponse:\n{resp.text}")
            data = resp.json()
            if "errors" in data and data["errors"]:
                raise RuntimeError(f"GraphQL errors:\n{data['errors']}")
            return data["data"]
        except Exception as e:
            last_err = e
            sleep_s = min(2 ** i, 10)
            logger.warning("GraphQL call failed (attempt %s/%s): %s", i + 1, attempts, e)
            if i < attempts - 1:
                time.sleep(sleep_s)
    raise RuntimeError(f"GraphQL call failed after {attempts} attempts: {last_err}")


QUERY_DRAFTS = """
query($first:Int!, $after:String, $query:String) {
  draftOrders(first:$first, after:$after, query:$query, reverse:true) {
    edges { cursor node { id name tags } }
    pageInfo { hasNextPage endCursor }
  }
}
"""

QUERY_DRAFT_DETAIL = """
query($id:ID!, $locationId:ID!, $shipDateNamespace: String!, $shipDateKey: String!) {
  draftOrder(id:$id) {
    id
    name
    poNumber
    email
    shippingAddress { company name }
    billingAddress { company name }
    tags
    presentmentCurrencyCode
    customAttributes { key value }
    ship_date_meta: metafield(namespace: $shipDateNamespace, key: $shipDateKey) { value }
    metafields(first:250) { nodes { namespace key type value } }
    lineItems(first:250) {
      nodes {
        quantity
        title
        appliedDiscount {
          description title value valueType
          amountV2 { amount currencyCode }
        }
        originalUnitPriceWithCurrency { amount currencyCode }
        originalUnitPriceSet { shopMoney { amount currencyCode } }
        priceOverride { amount currencyCode }
        variant {
          id
          product { tags title }
          inventoryItem {
            tracked
            inventoryLevel(locationId:$locationId) {
              quantities(names:["available"]) { name quantity }
            }
          }
        }
      }
    }
  }
}
"""

MUTATION_DUPLICATE = """
mutation($id: ID!) {
  draftOrderDuplicate(id: $id) {
    draftOrder { id name }
    userErrors { field message }
  }
}
"""

MUTATION_UPDATE = """
mutation($id:ID!, $input:DraftOrderInput!) {
  draftOrderUpdate(id:$id, input:$input) {
    draftOrder { id name tags poNumber lineItems(first: 250) { edges { node { id } } } }
    userErrors { message field }
  }
}
"""

MUTATION_DELETE = """
mutation($id:ID!) {
  draftOrderDelete(input:{id:$id}) {
    deletedId
    userErrors { field message }
  }
}
"""


def fetch_draft_detail(draft_id: str) -> Dict[str, Any]:
    data = gql(
        QUERY_DRAFT_DETAIL,
        {
            "id": draft_id,
            "locationId": LOCATION_ID,
            "shipDateNamespace": SHIP_DATE_METAFIELD_NAMESPACE,
            "shipDateKey": SHIP_DATE_METAFIELD_KEY,
        },
    )
    return data.get("draftOrder") or {}


# ----------------------------
# MONEY / INPUT HELPERS (identical to v2)
# ----------------------------
def money_input(m: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not m:
        return None
    amt = m.get("amount")
    if amt is None:
        return None
    out = {"amount": str(amt)}
    if m.get("currencyCode"):
        out["currencyCode"] = m["currencyCode"]
    return out


def applied_discount_input(ad: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not ad:
        return None
    out: Dict[str, Any] = {
        "description": ad.get("description"),
        "title": ad.get("title"),
        "value": ad.get("value"),
        "valueType": ad.get("valueType"),
    }
    if ad.get("amountV2") and ad["amountV2"].get("amount") is not None:
        out["amount"] = str(ad["amountV2"]["amount"])
    return {k: v for k, v in out.items() if v is not None} or None


def merge_custom_attributes(existing: List[Dict[str, Any]], additions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    merged: Dict[str, str] = {}
    for item in existing or []:
        k, v = item.get("key"), item.get("value")
        if k:
            merged[str(k)] = "" if v is None else str(v)
    for item in additions or []:
        k, v = item.get("key"), item.get("value")
        if k:
            merged[str(k)] = "" if v is None else str(v)
    return [{"key": k, "value": v} for k, v in merged.items()]


def merge_metafields(existing: List[Dict[str, Any]], additions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    merged: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for item in list(existing or []) + list(additions or []):
        ns = (item.get("namespace") or "").strip()
        key = (item.get("key") or "").strip()
        if not ns or not key:
            continue
        value = item.get("value")
        if value is None:
            continue
        merged[(ns, key)] = {
            "namespace": ns,
            "key": key,
            "type": (item.get("type") or "").strip(),
            "value": str(value),
        }
    return list(merged.values())


def build_line_input(line: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {"quantity": int(line.get("quantity") or 0)}
    if line.get("variant"):
        out["variantId"] = line["variant"]["id"]
        po = money_input(line.get("priceOverride"))
        if po:
            out["priceOverride"] = po
        else:
            oup = money_input(line.get("originalUnitPriceWithCurrency"))
            if not oup:
                oup = money_input((line.get("originalUnitPriceSet") or {}).get("shopMoney"))
            if oup:
                out["priceOverride"] = oup
    else:
        out["title"] = line.get("title") or "Custom item"
        oup = money_input(line.get("originalUnitPriceWithCurrency"))
        if oup:
            out["originalUnitPriceWithCurrency"] = oup
    lad = applied_discount_input(line.get("appliedDiscount"))
    if lad:
        out["appliedDiscount"] = lad
    return {k: v for k, v in out.items() if v is not None}


# ----------------------------
# ROOT LINKAGE
# ----------------------------
def get_root_linkage(live_draft: Dict[str, Any]) -> Tuple[str, str]:
    """
    Returns (root_po, root_draft_id) -- the TRUE split0 order this draft
    ultimately descends from, no matter how many generations deep. Every
    split-150/split-remainder draft should already carry these from when
    it was first created; we simply read and pass them through unchanged.
    Falls back to this draft's own identity only if the fields are somehow
    missing (should not happen in practice, but fail-safe rather than
    fail-loud here since losing lineage is not itself a processing error).
    """
    custom_attrs = {a.get("key"): a.get("value") for a in (live_draft.get("customAttributes") or [])}
    metafields = {
        (m.get("namespace"), m.get("key")): m.get("value")
        for m in (live_draft.get("metafields") or {}).get("nodes", [])
    }
    root_po = custom_attrs.get(LINK_CUSTOM_ATTR_PO_KEY) or live_draft.get("poNumber") or ""
    root_draft_id = (
        custom_attrs.get(LINK_CUSTOM_ATTR_DRAFTID_KEY)
        or metafields.get((ORIGINAL_DRAFT_ID_METAFIELD_NAMESPACE, ORIGINAL_DRAFT_ID_METAFIELD_KEY))
        or live_draft.get("id")
    )
    return root_po, root_draft_id


def build_linking_fields(*, root_po: str, root_draft_id: str, own_new_po: str) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    ca_add = [
        {"key": LINK_CUSTOM_ATTR_PO_KEY, "value": root_po},
        {"key": LINK_CUSTOM_ATTR_DRAFTID_KEY, "value": root_draft_id},
    ]
    mf_add: List[Dict[str, Any]] = [
        {
            "namespace": ORIGINAL_DRAFT_ID_METAFIELD_NAMESPACE,
            "key": ORIGINAL_DRAFT_ID_METAFIELD_KEY,
            "type": ORIGINAL_DRAFT_ID_METAFIELD_TYPE,
            "value": root_draft_id,
        },
        {
            "namespace": PO_METAFIELD_NAMESPACE,
            "key": PO_METAFIELD_KEY,
            "type": PO_METAFIELD_TYPE,
            "value": own_new_po,
        },
    ]
    return ca_add, mf_add


# ----------------------------
# SHIP DATE GATE (7-day window -- see ASSUMPTION note above)
# ----------------------------
def parse_ship_date_value(raw: Optional[str]) -> Optional[datetime.date]:
    if not raw:
        return None
    text = str(raw).strip()
    if not text:
        return None
    try:
        if "T" in text:
            return datetime.datetime.fromisoformat(text.replace("Z", "+00:00")).date()
        return datetime.date.fromisoformat(text)
    except Exception:
        return None


def ship_date_is_eligible(raw_ship_date: Optional[str]) -> bool:
    ship_date = parse_ship_date_value(raw_ship_date)
    if raw_ship_date is None or str(raw_ship_date).strip() == "":
        return True
    if ship_date is None:
        return False
    cutoff = datetime.date.today() + datetime.timedelta(days=SHIP_DATE_WINDOW_DAYS)
    return ship_date < cutoff


# ----------------------------
# RULE ENGINE (identical to shopify-adjust-orders-v2.py)
# ----------------------------
def get_available_qty(line: Dict[str, Any]) -> Optional[int]:
    try:
        variant = line.get("variant") or {}
        inv_item = variant.get("inventoryItem") or {}
        if inv_item.get("tracked") is False:
            return None
        level = inv_item.get("inventoryLevel")
        if not level:
            return 0
        for q in (level.get("quantities") or []):
            if q.get("name") == "available":
                return int(q.get("quantity") or 0)
        return 0
    except Exception:
        return None


def has_launch_tag(line: Dict[str, Any]) -> bool:
    variant = line.get("variant") or {}
    tags = (variant.get("product") or {}).get("tags") or []
    return any(str(t).strip().casefold().startswith(LAUNCH_TAG_PREFIX) for t in tags)


def is_fully_in_stock(line: Dict[str, Any]) -> bool:
    variant = line.get("variant")
    if not variant:
        return True
    qty = int(line.get("quantity") or 0)
    available = get_available_qty(line)
    if available is None:
        return True
    return available >= qty


def get_line_unit_price(line: Dict[str, Any]) -> Decimal:
    override = line.get("priceOverride") or {}
    if override.get("amount") is not None:
        return to_decimal(override.get("amount"))
    custom_price = line.get("originalUnitPriceWithCurrency") or {}
    if custom_price.get("amount") is not None:
        return to_decimal(custom_price.get("amount"))
    variant_price = (line.get("originalUnitPriceSet") or {}).get("shopMoney") or {}
    if variant_price.get("amount") is not None:
        return to_decimal(variant_price.get("amount"))
    return Decimal("0")


def line_value(line: Dict[str, Any]) -> Decimal:
    qty = int(line.get("quantity") or 0)
    return get_line_unit_price(line) * Decimal(qty)


def classify_lines(lines: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    keep: List[Dict[str, Any]] = []
    backorder: List[Dict[str, Any]] = []
    for line in lines:
        if not has_launch_tag(line) and is_fully_in_stock(line):
            keep.append(line)
        else:
            backorder.append(line)
    return keep, backorder


def sum_value(lines: List[Dict[str, Any]]) -> Decimal:
    total = Decimal("0")
    for line in lines:
        total += line_value(line)
    return total


def pick_split_band_tag(bo_value: Decimal) -> str:
    if bo_value >= MIN_SPLIT_VALUE:
        return SPLIT_150_TAG
    return SPLIT_REMAINDER_TAG


# ----------------------------
# MUTATION WRAPPERS (identical to v2)
# ----------------------------
def draft_duplicate(original_id: str) -> Dict[str, Any]:
    if DRY_RUN:
        return {"id": "DRY_RUN_DUPLICATE", "name": "DRY_RUN_DUPLICATE", "tags": []}
    res = gql(MUTATION_DUPLICATE, {"id": original_id}, attempts=1)["draftOrderDuplicate"]
    errs = res.get("userErrors") or []
    if errs:
        raise RuntimeError(f"draftOrderDuplicate userErrors: {errs}")
    d = res.get("draftOrder")
    if not d:
        raise RuntimeError("draftOrderDuplicate returned no draftOrder")
    return d


def draft_update_return(draft_id: str, input_data: Dict[str, Any], label: str) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    if DRY_RUN:
        logger.info("DRY RUN — would update %s: %s", label, draft_id)
        return [], {}
    res = gql(MUTATION_UPDATE, {"id": draft_id, "input": input_data})["draftOrderUpdate"]
    errs = res.get("userErrors") or []
    d = res.get("draftOrder") or {}
    if not errs:
        logger.info("Updated %s: %s | poNumber=%s", label, d.get("name"), d.get("poNumber"))
    return errs, d


def draft_delete(draft_id: str, label: str) -> None:
    if DRY_RUN:
        logger.info("DRY RUN — would delete %s: %s", label, draft_id)
        return
    try:
        res = gql(MUTATION_DELETE, {"id": draft_id})["draftOrderDelete"]
        errs = res.get("userErrors") or []
        if errs:
            logger.warning("draftOrderDelete userErrors (%s): %s", label, errs)
        else:
            logger.info("Deleted %s: %s", label, draft_id)
    except Exception as e:
        logger.warning("Failed to delete %s %s: %s", label, draft_id, e)


# ----------------------------
# TAG / LOCK HELPERS
# ----------------------------
def with_tag(tags: List[str], tag: str) -> List[str]:
    out = list(tags or [])
    if tag not in out:
        out.append(tag)
    return out


def without_tag(tags: List[str], tag: str) -> List[str]:
    return [t for t in (tags or []) if t != tag]


def without_tags(tags: List[str], tags_to_remove: Set[str]) -> List[str]:
    remove = set(tags_to_remove or set())
    return [t for t in (tags or []) if t not in remove]


def try_tag_needs_review(draft_id: str, tags: List[str], reason: str = "") -> None:
    desired_tags = with_tag(list(tags or []), NEEDS_REVIEW_TAG)
    desired_tags = without_tag(desired_tags, PROCESSING_TAG)
    if DRY_RUN:
        logger.info("DRY RUN — would tag %s with '%s' (%s)", draft_id, NEEDS_REVIEW_TAG, reason)
        return
    errs, _ = draft_update_return(draft_id, {"tags": desired_tags}, label=f"tag {NEEDS_REVIEW_TAG}")
    if errs:
        logger.warning("Could not tag %s with '%s' (%s): %s", draft_id, NEEDS_REVIEW_TAG, reason, errs)


def claim_processing_lock(draft: Dict[str, Any]) -> bool:
    tags = list(draft.get("tags") or [])
    if PROCESSING_TAG in tags or NEEDS_REVIEW_TAG in tags:
        return False
    if DRY_RUN:
        logger.info("DRY RUN — would add processing tag to %s", draft.get("name"))
        return True
    new_tags = without_tags(with_tag(tags, PROCESSING_TAG), CONVERSION_TRIGGER_TAGS)
    errs, updated = draft_update_return(draft["id"], {"tags": new_tags}, label="claim processing lock")
    if errs:
        raise RuntimeError(f"Failed to claim processing lock: {errs}")
    return PROCESSING_TAG in set(updated.get("tags") or [])


def release_processing_lock(draft_id: str, tags: List[str]) -> None:
    if DRY_RUN:
        logger.info("DRY RUN — would remove processing tag from %s", draft_id)
        return
    cleaned = without_tag(tags, PROCESSING_TAG)
    errs, _ = draft_update_return(draft_id, {"tags": cleaned}, label="release processing lock")
    if errs:
        logger.warning("Failed to release processing lock for %s: %s", draft_id, errs)


# ----------------------------
# DRAFT PROCESSOR
# ----------------------------
def process_draft(draft_id: str) -> str:
    draft = fetch_draft_detail(draft_id)
    name = draft.get("name", draft_id)
    existing_tags = list(draft.get("tags") or [])

    # Defensive re-check even though the query already filters on this --
    # belt and suspenders, same philosophy as v2's allow-list check.
    if SPLIT_150_TAG not in existing_tags:
        logger.info("%s: SKIP (missing '%s' tag).", name, SPLIT_150_TAG)
        return "skipped"
    if NEEDS_REVIEW_TAG in existing_tags:
        logger.info("%s: SKIP (tag '%s' present).", name, NEEDS_REVIEW_TAG)
        return "skipped"
    if PROCESSING_TAG in existing_tags:
        logger.info("%s: SKIP (tag '%s' present — concurrent run?).", name, PROCESSING_TAG)
        return "skipped"

    excluded, exclusion_reasons = is_excluded_draft(draft)
    if excluded:
        logger.info("%s: SKIP (excluded customer: %s).", name, " ; ".join(exclusion_reasons))
        return "skipped"

    raw_ship_date = ((draft.get("ship_date_meta") or {}).get("value") or "").strip()
    if not ship_date_is_eligible(raw_ship_date):
        logger.info("%s: SKIP (ship date %r not yet eligible).", name, raw_ship_date)
        return "skipped"

    if not claim_processing_lock(draft):
        logger.info("%s: SKIP (could not claim processing lock).", name)
        return "skipped"

    processing_released = False
    try:
        live = fetch_draft_detail(draft_id)  # re-fetch fresh after claiming the lock
        lines = (live.get("lineItems") or {}).get("nodes") or []
        original_tags = list(live.get("tags") or [])

        keep_lines, backorder_lines = classify_lines(lines)

        # CASE 3: fully resolved. Nothing here to do -- check-draft-orders.py
        # and release-instock-orders.py handle it independently from here.
        # No tag changes at all, per design.
        if not backorder_lines:
            logger.info("%s: no backorder lines remain. Leaving for check-draft-orders.py.", name)
            release_processing_lock(draft_id, original_tags)
            processing_released = True
            return "resolved"

        keep_value = sum_value(keep_lines)
        bo_value = sum_value(backorder_lines)
        keep_ok = keep_value >= MIN_SPLIT_VALUE
        bo_hold_ok = bo_value >= MIN_BACKORDER_HOLD_VALUE

        logger.info(
            "%s: projected keep=%s (ok=%s @ $%s) backorder=%s (hold_ok=%s @ $%s)",
            name, keep_value, keep_ok, MIN_SPLIT_VALUE, bo_value, bo_hold_ok, MIN_BACKORDER_HOLD_VALUE,
        )

        # CASE 1: nothing clears the gate yet. No-op, no tag change, try
        # again tomorrow -- this is NOT terminal, unlike v2's initial eval.
        if not keep_ok or not bo_hold_ok:
            logger.info("%s: gate not cleared, no split attempted. Leaving as-is.", name)
            release_processing_lock(draft_id, original_tags)
            processing_released = True
            return "no-op"

        # --- both gates cleared: attempt the split ---
        root_po, root_draft_id = get_root_linkage(live)
        own_new_po = build_next_po_number(live.get("poNumber") or "")
        parent_generation = current_generation_number(original_tags)
        child_generation_tag = generation_tag_for(parent_generation + 1)
        original_custom_attributes = live.get("customAttributes") or []
        original_metafields = (live.get("metafields") or {}).get("nodes") or []
        original_lines = list(lines)

        try:
            child = draft_duplicate(draft_id)
        except Exception as e:
            logger.error("%s: non-idempotent duplicate mutation failed; not retrying. Tagging '%s'. Error: %s", name, NEEDS_REVIEW_TAG, e)
            try_tag_needs_review(draft_id, original_tags, reason="duplicate mutation failed")
            processing_released = True
            raise

        try:
            ca_add, mf_add = build_linking_fields(root_po=root_po, root_draft_id=root_draft_id, own_new_po=own_new_po)
            # Child inherits parent's tags MINUS: parent's own generation tag
            # (child gets its own), conversion-trigger tags, and the
            # processing lock -- then gains its own generation tag. The
            # value-band tag (split-150/split-remainder) is deliberately
            # NOT set yet -- that only happens after verification, below.
            child_base_tags = without_tags(
                list(original_tags),
                CONVERSION_TRIGGER_TAGS.union({PROCESSING_TAG, generation_tag_for(parent_generation)}),
            )
            child_input = {
                "lineItems": [build_line_input(l) for l in backorder_lines],
                "poNumber": own_new_po,
                "tags": with_tag(child_base_tags, child_generation_tag),
                "customAttributes": merge_custom_attributes(original_custom_attributes, ca_add),
                "metafields": merge_metafields(original_metafields, mf_add),
            }
            child = draft_update_return(child["id"], child_input, label="child (backorder) update")[1] or child

            # Parent keeps ONLY the newly-shippable lines and releases the
            # lock in the same call. Every other tag (including its own
            # generation tag and value-band tag) is left completely
            # untouched, per design.
            parent_input = {
                "lineItems": [build_line_input(l) for l in keep_lines],
                "tags": without_tag(original_tags, PROCESSING_TAG),
            }
            draft_update_return(draft_id, parent_input, label="parent (ship-now) update")
        except Exception:
            logger.exception("%s: split mutation failed, rolling back child.", name)
            draft_delete(child["id"], label="rollback child after failed update")
            release_processing_lock(draft_id, original_tags)
            processing_released = True
            raise

        # --- verify actual totals ---
        if DRY_RUN:
            actual_keep_ok, actual_bo_hold_ok, actual_bo_value = keep_ok, bo_hold_ok, bo_value
        else:
            refreshed_parent = fetch_draft_detail(draft_id)
            refreshed_child = fetch_draft_detail(child["id"])
            actual_keep_value = sum_value((refreshed_parent.get("lineItems") or {}).get("nodes") or [])
            actual_bo_value = sum_value((refreshed_child.get("lineItems") or {}).get("nodes") or [])
            actual_keep_ok = actual_keep_value >= MIN_SPLIT_VALUE
            actual_bo_hold_ok = actual_bo_value >= MIN_BACKORDER_HOLD_VALUE
            logger.info(
                "%s: actual keep=%s (ok=%s) backorder=%s (hold_ok=%s)",
                name, actual_keep_value, actual_keep_ok, actual_bo_value, actual_bo_hold_ok,
            )

        if not actual_keep_ok or not actual_bo_hold_ok:
            # Unwind: NOT terminal here, unlike v2. No tag applied -- just
            # revert and try again tomorrow.
            logger.warning("%s: actual values failed the gate post-verification, unwinding (no tag change).", name)
            draft_delete(child["id"], label="unwind child (actual values below threshold)")
            restore_input = {"lineItems": [build_line_input(l) for l in original_lines]}
            draft_update_return(draft_id, restore_input, label="restore parent lines after unwind")
            release_processing_lock(draft_id, original_tags)
            processing_released = True
            return "unwound"

        # --- success: assign the child's real value-band tag now ---
        band_tag = pick_split_band_tag(actual_bo_value)
        child_current_tags = list(child.get("tags") or [])
        if band_tag not in child_current_tags:
            child = draft_update_return(child["id"], {"tags": with_tag(child_current_tags, band_tag)}, label=f"tag child {band_tag}")[1] or child

        logger.info("%s: split succeeded (child %s, %s, backorder=%s).", name, child.get("name") or child.get("id"), band_tag, actual_bo_value)
        processing_released = True
        return "split"

    finally:
        if not processing_released:
            try:
                release_processing_lock(draft_id, list(draft.get("tags") or []))
            except Exception:
                logger.exception("%s: CRITICAL — could not release processing lock in final cleanup.", name)


# ----------------------------
# MAIN
# ----------------------------
def chunk_list(items: List[str], size: int) -> List[List[str]]:
    return [items[i:i + size] for i in range(0, len(items), size)]


def build_open_ended_query() -> str:
    # The ENTIRE pool for this script: open drafts tagged with the $150+
    # value band, minus needs-review and minus anything currently locked by
    # a concurrent run. Deliberately NO generation-tag enumeration and NO
    # "already evaluated" exclusion -- every eligible draft gets walked
    # every run, forever, per the no-shortcut-exclusion principle.
    parts = [
        "status:open",
        f"tag:{SPLIT_150_TAG}",
        f"-tag:{NEEDS_REVIEW_TAG}",
        f"-tag:{PROCESSING_TAG}",
    ]
    return " ".join(parts)


def main() -> None:
    targets = {normalize_draft_name(n) for n in DRAFT_ORDER_NAMES} if DRAFT_ORDER_NAMES else set()
    collected: List[Dict[str, Any]] = []
    scanned = 0

    if DRAFT_ORDER_NAMES:
        for chunk in chunk_list(DRAFT_ORDER_NAMES, 12):
            name_query = build_draft_name_query(chunk)
            # Even in scoped test mode, still require the split-150 tag.
            query = f"status:open tag:{SPLIT_150_TAG} ({name_query})" if name_query else f"status:open tag:{SPLIT_150_TAG}"
            after = None
            while True:
                resp = gql(QUERY_DRAFTS, {"first": 250, "after": after, "query": query}).get("draftOrders") or {}
                edges = resp.get("edges") or []
                if not edges:
                    break
                for e in edges:
                    node = e.get("node") or {}
                    if node:
                        collected.append(node)
                        scanned += 1
                page_info = resp.get("pageInfo") or {}
                after = page_info.get("endCursor")
                if not page_info.get("hasNextPage"):
                    break
    else:
        query = build_open_ended_query()
        logger.info("Open-ended query: %s", query)
        page_size = min(250, MAX_DRAFTS)
        after = None
        while True:
            resp = gql(QUERY_DRAFTS, {"first": page_size, "after": after, "query": query}).get("draftOrders") or {}
            edges = resp.get("edges") or []
            if not edges:
                break
            for e in edges:
                node = e.get("node") or {}
                if node:
                    collected.append(node)
                    scanned += 1
                    if scanned >= MAX_DRAFTS:
                        break
            if scanned >= MAX_DRAFTS:
                break
            page_info = resp.get("pageInfo") or {}
            after = page_info.get("endCursor")
            if not page_info.get("hasNextPage"):
                break

    if not collected:
        logger.info("No drafts found.")
        return

    dedup: Dict[str, Dict[str, Any]] = {}
    for d in collected:
        did = d.get("id")
        if did and did not in dedup:
            dedup[did] = d
    drafts = list(dedup.values())
    if DRAFT_ORDER_NAMES:
        drafts = [d for d in drafts if normalize_draft_name(d.get("name", "")) in targets]

    logger.info("Found %s draft(s) after filtering. DRY_RUN=%s (scanned %s rows)", len(drafts), DRY_RUN, scanned)

    outcomes: Dict[str, List[str]] = {"split": [], "resolved": [], "no-op": [], "unwound": [], "skipped": []}
    failed: List[Tuple[str, str]] = []
    for d in drafts:
        draft_name = d.get("name", d.get("id", "(unknown)"))
        try:
            status = process_draft(d["id"])
            outcomes.setdefault(status, []).append(draft_name)
        except Exception as e:
            failed.append((draft_name, str(e)))
            logger.error("%s: FAILED — %s", draft_name, e)

    logger.info("")
    logger.info("Run summary")
    for key, names in outcomes.items():
        logger.info("%s: %s", key.upper(), len(names))
        if names:
            logger.info("  %s", ", ".join(names))
    logger.info("FAILED: %s", len(failed))
    if failed:
        for draft_name, err in failed:
            logger.info("  %s: %s", draft_name, err)


if __name__ == "__main__":
    main()

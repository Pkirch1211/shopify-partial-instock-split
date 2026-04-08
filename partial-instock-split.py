import csv
import logging
import os
import re
import sys
import time
from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

import requests
from dotenv import load_dotenv

load_dotenv()

# ----------------------------
# Config
# ----------------------------
SHOPIFY_SHOP = os.getenv("SHOPIFY_SHOP", "").strip()
SHOPIFY_TOKEN = os.getenv("SHOPIFY_TOKEN", "").strip()
SHOPIFY_API_VERSION = os.getenv("SHOPIFY_API_VERSION", "2025-07").strip()
SHOPIFY_LOCATION_ID = os.getenv("SHOPIFY_LOCATION_ID", "").strip()
DRY_RUN = os.getenv("DRY_RUN", "true").strip().lower() == "true"

MIN_AVAILABLE_LINES = int(os.getenv("MIN_AVAILABLE_LINES", "2").strip())
MIN_AVAILABLE_VALUE = Decimal(os.getenv("MIN_AVAILABLE_VALUE", "100").strip())
MIN_AVAILABLE_PERCENT = Decimal(os.getenv("MIN_AVAILABLE_PERCENT", "0.30").strip())
MIN_REMAINING_LINES = int(os.getenv("MIN_REMAINING_LINES", "2").strip())
MAX_SPLIT_DEPTH = int(os.getenv("MAX_SPLIT_DEPTH", "2").strip())

EXCLUDE_SKUS: Set[str] = {
    s.strip().upper()
    for s in os.getenv("EXCLUDE_SKUS", "").split(",")
    if s.strip()
}

PARTIAL_PARENT_TAG = os.getenv("PARTIAL_PARENT_TAG", "partial-instock-split-done").strip()
PARTIAL_CHILD_TAG = os.getenv("PARTIAL_CHILD_TAG", "partial-instock-child").strip()
PROCESSING_TAG = os.getenv("PROCESSING_TAG", "partial-instock-processing").strip()
READY_TAG = os.getenv("READY_TAG", "instock-ready").strip()
NEEDS_REVIEW_TAG = os.getenv("NEEDS_REVIEW_TAG", "needs-review").strip()
SUBMITTED_TAG = os.getenv("SUBMITTED_TAG", "order-submitted").strip()
LINEAGE_NAMESPACE = os.getenv("LINEAGE_NAMESPACE", "automation").strip()

_raw_draft_order_names = os.getenv("DRAFT_ORDER_NAMES", "").strip()
# Set DRAFT_ORDER_NAMES=ALL to run open-ended across all eligible drafts.
# Set to a comma-separated list (e.g. D15483,D15484) to scope to specific drafts.
DRAFT_ORDER_NAMES_ALL = _raw_draft_order_names.upper() == "ALL"
DRAFT_ORDER_NAMES = (
    set()
    if DRAFT_ORDER_NAMES_ALL
    else {x.strip() for x in _raw_draft_order_names.split(",") if x.strip()}
)

CSV_LOG_PATH = Path(
    os.getenv("CSV_LOG_PATH", "logs/partial_instock_split_history.csv")
).resolve()

# NOTE:
# We intentionally do NOT exclude PARTIAL_CHILD_TAG.
# A child draft should be eligible for one more split pass until MAX_SPLIT_DEPTH is reached.
EXCLUDE_TAGS = {
    READY_TAG,
    NEEDS_REVIEW_TAG,
    PARTIAL_PARENT_TAG,
    SUBMITTED_TAG,
}

REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "30").strip())
SLEEP_SECONDS = float(os.getenv("SLEEP_SECONDS", "0.35").strip())
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "4").strip())

ALLOW_OPEN_ENDED = os.getenv("ALLOW_OPEN_ENDED", "false").strip().lower() == "true"

if not SHOPIFY_SHOP or not SHOPIFY_TOKEN or not SHOPIFY_LOCATION_ID:
    raise ValueError("Missing SHOPIFY_SHOP, SHOPIFY_TOKEN, or SHOPIFY_LOCATION_ID")

if not DRAFT_ORDER_NAMES and not DRAFT_ORDER_NAMES_ALL:
    raise ValueError(
        "DRAFT_ORDER_NAMES is not set. Set it to a comma-separated list of draft order names "
        "(e.g. D15483,D15484) to scope specific drafts, or set it to ALL to run open-ended."
    )

BASE_URL = f"https://{SHOPIFY_SHOP}/admin/api/{SHOPIFY_API_VERSION}/graphql.json"
HEADERS = {
    "Content-Type": "application/json",
    "X-Shopify-Access-Token": SHOPIFY_TOKEN,
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


# ----------------------------
# Helpers
# ----------------------------
def sleep_brief() -> None:
    time.sleep(SLEEP_SECONDS)


def to_decimal(value: Any, default: str = "0") -> Decimal:
    try:
        if value is None or value == "":
            return Decimal(default)
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return Decimal(default)


def normalize_tags(tags: Any) -> List[str]:
    if tags is None:
        return []
    if isinstance(tags, str):
        raw = tags.split(",")
    elif isinstance(tags, Sequence):
        raw = list(tags)
    else:
        return []

    cleaned: List[str] = []
    seen: Set[str] = set()
    for tag in raw:
        val = str(tag).strip()
        if not val:
            continue
        key = val.lower()
        if key in seen:
            continue
        seen.add(key)
        cleaned.append(val)
    return cleaned


def add_tags(existing: Any, *new_tags: str) -> List[str]:
    tags = normalize_tags(existing)
    seen = {t.lower() for t in tags}
    for tag in new_tags:
        tag = (tag or "").strip()
        if not tag or tag.lower() in seen:
            continue
        tags.append(tag)
        seen.add(tag.lower())
    return tags


def remove_tags(existing: Any, *tags_to_remove: str) -> List[str]:
    remove_set = {t.strip().lower() for t in tags_to_remove if t and t.strip()}
    return [t for t in normalize_tags(existing) if t.lower() not in remove_set]


def chunks(items: Sequence[Any], size: int) -> List[Sequence[Any]]:
    return [items[i : i + size] for i in range(0, len(items), size)]


def safe_single_line_text(value: Any, limit: int = 255) -> str:
    text = str(value or "").strip()
    text = re.sub(r"\s+", " ", text)
    return text[:limit]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def ensure_log_dir() -> None:
    CSV_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)


def parse_ship_date_value(raw_value: str) -> Optional[date]:
    value = (raw_value or "").strip()
    if not value:
        return None

    candidates = [value]
    if value.endswith("Z"):
        candidates.append(value[:-1] + "+00:00")
    if "T" in value:
        candidates.append(value.split("T", 1)[0])

    for candidate in candidates:
        try:
            if "T" in candidate or "+" in candidate:
                return datetime.fromisoformat(candidate).date()
            return date.fromisoformat(candidate)
        except ValueError:
            continue
    return None


def format_percent(value: Decimal) -> str:
    return f"{(value * Decimal('100')):.2f}"


def get_nested(d: Dict[str, Any], *keys: str) -> Any:
    cur: Any = d
    for key in keys:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(key)
        if cur is None:
            return None
    return cur


# ----------------------------
# CSV logging
# ----------------------------
CSV_HEADERS = [
    "timestamp_utc",
    "parent_draft_id",
    "parent_draft_name",
    "parent_po",
    "child_draft_id",
    "child_draft_name",
    "child_po",
    "ship_date",
    "available_line_count",
    "remaining_line_count",
    "available_value",
    "total_value",
    "available_percent",
    "split_depth_parent",
    "split_depth_child",
    "reason",
    "dry_run",
]


def append_split_log_row(
    parent: Dict[str, Any],
    child: Dict[str, Any],
    eval_data: Dict[str, Any],
    ship_date: str,
    child_po: str,
) -> None:
    ensure_log_dir()
    file_exists = CSV_LOG_PATH.exists()

    row = {
        "timestamp_utc": utc_now_iso(),
        "parent_draft_id": parent.get("id", ""),
        "parent_draft_name": parent.get("name", ""),
        "parent_po": parent.get("poNumber", ""),
        "child_draft_id": child.get("id", ""),
        "child_draft_name": child.get("name", ""),
        "child_po": child.get("poNumber") or child_po,
        "ship_date": ship_date or "",
        "available_line_count": str(eval_data["available_count"]),
        "remaining_line_count": str(eval_data["remaining_count"]),
        "available_value": str(eval_data["available_value"]),
        "total_value": str(eval_data["total_value"]),
        "available_percent": format_percent(eval_data["available_percent"]),
        "split_depth_parent": str(split_depth_from_po(parent.get("poNumber") or "")),
        "split_depth_child": str(split_depth_from_po(child.get("poNumber") or child_po)),
        "reason": "partial_instock_split",
        "dry_run": str(DRY_RUN).lower(),
    }

    with CSV_LOG_PATH.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)

    logger.info("Appended split log row to %s", CSV_LOG_PATH)


# ----------------------------
# Shopify GraphQL
# ----------------------------
def graphql(query: str, variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    payload = {"query": query, "variables": variables or {}}
    last_error: Optional[Exception] = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.post(
                BASE_URL,
                headers=HEADERS,
                json=payload,
                timeout=REQUEST_TIMEOUT,
            )
            response.raise_for_status()
            data = response.json()
            if data.get("errors"):
                raise RuntimeError(f"GraphQL errors: {data['errors']}")
            return data["data"]
        except Exception as exc:
            last_error = exc
            logger.warning("GraphQL attempt %s/%s failed: %s", attempt, MAX_RETRIES, exc)
            if attempt < MAX_RETRIES:
                time.sleep(min(attempt * 2, 8))

    raise RuntimeError(f"GraphQL failed after retries: {last_error}")


# ----------------------------
# Queries / mutations
# ----------------------------
DRAFTS_QUERY = """
query GetDraftOrders($cursor: String, $query: String!) {
  draftOrders(first: 100, after: $cursor, query: $query) {
    pageInfo {
      hasNextPage
      endCursor
    }
    nodes {
      id
      name
      status
      tags
      poNumber
      note2
      customAttributes {
        key
        value
      }
      visibleToCustomer
      reserveInventoryUntil
      metafields(first: 100) {
        nodes {
          namespace
          key
          type
          value
        }
      }
      lineItems(first: 100) {
        nodes {
          id
          title
          sku
          quantity
          originalUnitPriceWithCurrency {
            amount
            currencyCode
          }
          originalTotalSet {
            presentmentMoney {
              amount
              currencyCode
            }
          }
          appliedDiscount {
            title
            value
            valueType
            description
            amountV2 {
              amount
              currencyCode
            }
          }
          variant {
            id
            title
            sku
            inventoryItem {
              id
            }
          }
        }
      }
    }
  }
}
"""

INVENTORY_QUERY = """
query InventoryLevels($inventoryItemIds: [ID!]!, $locationId: ID!) {
  nodes(ids: $inventoryItemIds) {
    ... on InventoryItem {
      id
      sku
      inventoryLevel(locationId: $locationId) {
        quantities(names: ["available"]) {
          name
          quantity
        }
      }
    }
  }
}
"""

DUPLICATE_DRAFT_MUTATION = """
mutation DraftOrderDuplicate($id: ID!) {
  draftOrderDuplicate(id: $id) {
    draftOrder {
      id
      name
      poNumber
      tags
    }
    userErrors {
      field
      message
    }
  }
}
"""

UPDATE_DRAFT_MUTATION = """
mutation DraftOrderUpdate($id: ID!, $input: DraftOrderInput!) {
  draftOrderUpdate(id: $id, input: $input) {
    draftOrder {
      id
      name
      tags
      poNumber
    }
    userErrors {
      field
      message
    }
  }
}
"""

DELETE_DRAFT_MUTATION = """
mutation DraftOrderDelete($id: ID!) {
  draftOrderDelete(input: { id: $id }) {
    deletedId
    userErrors {
      field
      message
    }
  }
}
"""


# ----------------------------
# Data access
# ----------------------------
def build_query() -> str:
    excluded = " ".join(f"-tag:{tag}" for tag in sorted(EXCLUDE_TAGS) if tag)
    return f"status:open {excluded}".strip()


def fetch_open_drafts() -> List[Dict[str, Any]]:
    query = build_query()
    logger.info("Open-ended query: %s", query)
    drafts: List[Dict[str, Any]] = []
    cursor: Optional[str] = None

    while True:
        data = graphql(DRAFTS_QUERY, {"cursor": cursor, "query": query})
        bucket = data["draftOrders"]
        nodes = bucket["nodes"]
        drafts.extend(nodes)
        logger.info("Fetched %s draft(s); running total=%s", len(nodes), len(drafts))
        if not bucket["pageInfo"]["hasNextPage"]:
            break
        cursor = bucket["pageInfo"]["endCursor"]
        sleep_brief()

    # Client-side guard: drop anything Shopify returned that isn't actually open.
    # Completed drafts (converted to orders) can slip through the status:open query filter.
    before_status_filter = len(drafts)
    drafts = [d for d in drafts if str(d.get("status", "")).upper() == "OPEN"]
    if len(drafts) < before_status_filter:
        logger.info(
            "Dropped %s non-open draft(s) after client-side status filter",
            before_status_filter - len(drafts),
        )

    if DRAFT_ORDER_NAMES:
        target_lower = {name.lstrip("#").lower() for name in DRAFT_ORDER_NAMES}
        filtered = [
            d for d in drafts
            if str(d.get("name", "")).strip().lstrip("#").lower() in target_lower
        ]
        logger.info(
            "DRAFT_ORDER_NAMES=%s | matched %s of %s fetched draft(s)",
            sorted(DRAFT_ORDER_NAMES),
            len(filtered),
            len(drafts),
        )
        drafts = filtered
    else:
        logger.info("DRAFT_ORDER_NAMES=ALL")

    return drafts


def fetch_inventory_availability(inventory_item_ids: Sequence[str]) -> Dict[str, int]:
    ids = [x for x in inventory_item_ids if x]
    results: Dict[str, int] = {}

    for batch in chunks(ids, 100):
        data = graphql(
            INVENTORY_QUERY,
            {"inventoryItemIds": list(batch), "locationId": SHOPIFY_LOCATION_ID},
        )
        for node in data.get("nodes", []):
            if not node:
                continue
            quantity = 0
            level = node.get("inventoryLevel") or {}
            for q in level.get("quantities") or []:
                if q.get("name") == "available":
                    quantity = int(q.get("quantity") or 0)
                    break
            results[node.get("id", "")] = quantity
        sleep_brief()

    return results


def get_metafield_value(draft: Dict[str, Any], namespace: str, key: str) -> Optional[str]:
    for mf in ((draft.get("metafields") or {}).get("nodes") or []):
        if (
            str(mf.get("namespace") or "").strip() == namespace
            and str(mf.get("key") or "").strip() == key
        ):
            value = mf.get("value")
            return None if value is None else str(value)
    return None


def update_draft(draft_id: str, input_payload: Dict[str, Any]) -> Dict[str, Any]:
    if DRY_RUN:
        logger.info("DRY RUN | would update draft %s with %s", draft_id, input_payload)
        return {"id": draft_id}

    data = graphql(UPDATE_DRAFT_MUTATION, {"id": draft_id, "input": input_payload})
    payload = data["draftOrderUpdate"]
    errors = payload.get("userErrors") or []
    if errors:
        raise RuntimeError(f"draftOrderUpdate failed: {errors}")
    return payload["draftOrder"]


def duplicate_draft(parent_id: str) -> Dict[str, Any]:
    """
    Duplicate a draft order via draftOrderDuplicate.
    This copies the draft wholesale — including purchasingEntity / B2B company
    context, customer, addresses, payment terms, note — without needing to
    manually specify any of those fields in DraftOrderInput.
    """
    if DRY_RUN:
        logger.info("DRY RUN | would duplicate draft %s", parent_id)
        return {
            "id": "gid://shopify/DraftOrder/DRY_RUN",
            "name": "#DRYRUN",
            "poNumber": "",
            "tags": [],
        }

    data = graphql(DUPLICATE_DRAFT_MUTATION, {"id": parent_id})
    payload = data["draftOrderDuplicate"]
    errors = payload.get("userErrors") or []
    if errors:
        raise RuntimeError(f"draftOrderDuplicate failed: {errors}")
    return payload["draftOrder"]


def delete_draft(draft_id: str) -> None:
    """Delete a draft order — used for rollback if child update fails."""
    if DRY_RUN:
        logger.info("DRY RUN | would delete draft %s", draft_id)
        return

    try:
        data = graphql(DELETE_DRAFT_MUTATION, {"id": draft_id})
        payload = data["draftOrderDelete"]
        errors = payload.get("userErrors") or []
        if errors:
            logger.warning("draftOrderDelete userErrors for %s: %s", draft_id, errors)
        else:
            logger.info("Deleted draft %s (rollback)", draft_id)
    except Exception as exc:
        logger.warning("Failed to delete draft %s during rollback: %s", draft_id, exc)


# ----------------------------
# Ship date gate
# ----------------------------
def should_skip_for_ship_date(draft: Dict[str, Any]) -> Tuple[bool, str]:
    raw_ship_date = get_metafield_value(draft, "b2b", "ship_date")
    if not raw_ship_date:
        return False, ""

    parsed_ship_date = parse_ship_date_value(raw_ship_date)
    if not parsed_ship_date:
        return False, f"Unable to parse b2b.ship_date={raw_ship_date}"

    today = date.today()
    cutoff = today + timedelta(days=7)
    if parsed_ship_date >= cutoff:
        return True, f"b2b.ship_date {parsed_ship_date.isoformat()} is >= cutoff {cutoff.isoformat()}"

    return False, ""


# ----------------------------
# PO hierarchy / lineage
# ----------------------------
def parse_po_root_and_suffix(po_number: str) -> Tuple[str, Optional[str], str]:
    po = (po_number or "").strip()
    if not po:
        return "", None, ""

    # Match:
    #   ROOT-BO
    #   ROOT BO
    #   ROOT-BO1
    #   ROOT BO1.2
    match = re.match(r"^(.*?)([-\s]+)(BO(?:\d+)?(?:\.\d+)*)$", po, flags=re.IGNORECASE)
    if match:
        root = match.group(1).strip()
        sep = match.group(2)
        suffix = match.group(3).upper()
        return root, suffix, sep

    # Match standalone:
    #   BO
    #   BO1
    #   BO1.2
    match = re.match(r"^(BO(?:\d+)?(?:\.\d+)*)$", po, flags=re.IGNORECASE)
    if match:
        return "", match.group(1).upper(), ""

    return po, None, ""


def split_depth_from_po(po_number: str) -> int:
    _, suffix, _ = parse_po_root_and_suffix(po_number)
    if not suffix:
        return 0

    body = suffix[2:]  # remove BO
    if not body:
        return 1

    if body.startswith("."):
        return len([part for part in body[1:].split(".") if part]) + 1

    return len([part for part in body.split(".") if part])


def split_root_po(po_number: str) -> str:
    root, suffix, _ = parse_po_root_and_suffix(po_number)
    if suffix and not root:
        return ""
    return root or (po_number or "").strip()


def next_child_suffix(parent_po: str) -> str:
    _, suffix, _ = parse_po_root_and_suffix(parent_po)
    if not suffix:
        return "BO"

    depth = split_depth_from_po(parent_po)
    if depth == 1:
        return f"{suffix}.2"

    return f"{suffix}.1"


def build_child_po(parent_po: str) -> str:
    root, suffix, sep = parse_po_root_and_suffix(parent_po)

    if suffix:
        next_suffix = next_child_suffix(parent_po)
        return f"{root}{sep}{next_suffix}" if root else next_suffix

    next_suffix = next_child_suffix(parent_po)
    return f"{parent_po.strip()}-{next_suffix}" if parent_po.strip() else next_suffix


def can_split_more(parent_po: str) -> Tuple[bool, str]:
    depth = split_depth_from_po(parent_po)
    if depth >= MAX_SPLIT_DEPTH:
        return False, f"Split depth {depth} already at MAX_SPLIT_DEPTH {MAX_SPLIT_DEPTH}"
    return True, ""


# ----------------------------
# Draft transformation
# ----------------------------
def get_line_unit_price(line: Dict[str, Any]) -> Decimal:
    price = get_nested(line, "originalUnitPriceWithCurrency", "amount")
    if price is not None:
        return to_decimal(price)

    total = get_nested(line, "originalTotalSet", "presentmentMoney", "amount")
    qty = int(line.get("quantity") or 0)
    if qty > 0 and total is not None:
        return to_decimal(total) / Decimal(qty)

    return Decimal("0")


def build_discount_payload(discount: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not discount:
        return None

    payload = {
        "title": discount.get("title"),
        "description": discount.get("description"),
        "value": discount.get("value"),
        "valueType": discount.get("valueType"),
        "amount": get_nested(discount, "amountV2", "amount"),
    }
    payload = {k: v for k, v in payload.items() if v not in (None, "")}
    return payload or None


def build_line_payload(line: Dict[str, Any]) -> Dict[str, Any]:
    variant = line.get("variant") or {}
    payload: Dict[str, Any] = {
        "quantity": int(line.get("quantity") or 0),
    }

    if variant.get("id"):
        payload["variantId"] = variant["id"]
    else:
        payload["title"] = (
            line.get("title")
            or variant.get("sku")
            or line.get("sku")
            or "Untitled"
        )
        payload["originalUnitPrice"] = str(get_line_unit_price(line))
        if line.get("sku"):
            payload["sku"] = line["sku"]

    discount_payload = build_discount_payload(line.get("appliedDiscount"))
    if discount_payload:
        payload["appliedDiscount"] = discount_payload

    return payload


def build_child_metafields(parent: Dict[str, Any], child_po: str) -> List[Dict[str, str]]:
    """
    Build metafields for the child draft.
    Copies all metafields from parent (except stale lineage fields),
    then appends fresh lineage fields for this split.
    """
    parent_po = (parent.get("poNumber") or "").strip()
    root_po = split_root_po(parent_po)
    split_depth = split_depth_from_po(child_po)

    existing: List[Dict[str, str]] = []
    seen_keys: Set[Tuple[str, str]] = set()

    for mf in ((parent.get("metafields") or {}).get("nodes") or []):
        namespace = str(mf.get("namespace") or "").strip()
        key = str(mf.get("key") or "").strip()
        mf_type = str(mf.get("type") or "").strip()
        value = mf.get("value")

        if not namespace or not key or not mf_type or value is None:
            continue

        # Drop stale lineage fields — we'll write fresh ones below
        if namespace == LINEAGE_NAMESPACE and key.startswith("partial_split_"):
            continue

        existing.append(
            {
                "namespace": namespace,
                "key": key,
                "type": mf_type,
                "value": str(value),
            }
        )
        seen_keys.add((namespace, key))

    lineage_fields = [
        {
            "namespace": LINEAGE_NAMESPACE,
            "key": "partial_split_root_po",
            "type": "single_line_text_field",
            "value": safe_single_line_text(root_po),
        },
        {
            "namespace": LINEAGE_NAMESPACE,
            "key": "partial_split_parent_po",
            "type": "single_line_text_field",
            "value": safe_single_line_text(parent_po),
        },
        {
            "namespace": LINEAGE_NAMESPACE,
            "key": "partial_split_child_po",
            "type": "single_line_text_field",
            "value": safe_single_line_text(child_po),
        },
        {
            "namespace": LINEAGE_NAMESPACE,
            "key": "partial_split_parent_draft_id",
            "type": "single_line_text_field",
            "value": safe_single_line_text(parent.get("id")),
        },
        {
            "namespace": LINEAGE_NAMESPACE,
            "key": "partial_split_parent_draft_name",
            "type": "single_line_text_field",
            "value": safe_single_line_text(parent.get("name")),
        },
        {
            "namespace": LINEAGE_NAMESPACE,
            "key": "partial_split_reason",
            "type": "single_line_text_field",
            "value": "partial_instock",
        },
        {
            "namespace": LINEAGE_NAMESPACE,
            "key": "partial_split_depth",
            "type": "number_integer",
            "value": str(split_depth),
        },
    ]

    for field in lineage_fields:
        key_pair = (field["namespace"], field["key"])
        if key_pair in seen_keys:
            existing = [
                x for x in existing
                if (x["namespace"], x["key"]) != key_pair
            ]
        existing.append(field)

    return existing


def build_child_update_payload(
    parent: Dict[str, Any],
    available_lines: List[Dict[str, Any]],
    child_po: str,
) -> Dict[str, Any]:
    """
    Build the update payload applied to the duplicated child draft.

    Because we use draftOrderDuplicate, the child already has the correct:
      - customer / B2B purchasingEntity / company association
      - shipping + billing addresses
      - payment terms
      - note

    We only need to update: line items, tags, PO number, metafields,
    and custom attributes.
    """
    tags = normalize_tags(parent.get("tags", []))
    tags = remove_tags(tags, PROCESSING_TAG, PARTIAL_PARENT_TAG)
    tags = add_tags(tags, PARTIAL_CHILD_TAG)

    # Carry forward parent custom attributes
    custom_attributes = [
        {
            "key": str(attr.get("key", "")).strip(),
            "value": str(attr.get("value", "")).strip(),
        }
        for attr in (parent.get("customAttributes") or [])
        if str(attr.get("key", "")).strip()
    ] or None

    payload: Dict[str, Any] = {
        "lineItems": [build_line_payload(line) for line in available_lines],
        "tags": tags,
        "poNumber": child_po,
        "metafields": build_child_metafields(parent, child_po),
    }

    if custom_attributes:
        payload["customAttributes"] = custom_attributes

    return payload


def build_parent_update_payload(
    parent: Dict[str, Any],
    remaining_lines: List[Dict[str, Any]],
) -> Dict[str, Any]:
    tags = normalize_tags(parent.get("tags", []))
    tags = remove_tags(tags, PROCESSING_TAG)
    tags = add_tags(tags, PARTIAL_PARENT_TAG)

    return {
        "lineItems": [build_line_payload(line) for line in remaining_lines],
        "tags": tags,
    }


# ----------------------------
# SKU exclusion check
# ----------------------------
def has_excluded_sku(draft: Dict[str, Any]) -> bool:
    if not EXCLUDE_SKUS:
        return False
    for line in ((draft.get("lineItems") or {}).get("nodes") or []):
        sku = (
            line.get("sku")
            or get_nested(line, "variant", "sku")
            or ""
        ).strip().upper()
        if sku and sku in EXCLUDE_SKUS:
            return True
    return False


# ----------------------------
# Evaluation
# ----------------------------
def evaluate_draft(draft: Dict[str, Any], availability_by_item: Dict[str, int]) -> Dict[str, Any]:
    lines = (draft.get("lineItems") or {}).get("nodes") or []
    available_lines: List[Dict[str, Any]] = []
    remaining_lines: List[Dict[str, Any]] = []
    total_value = Decimal("0")
    available_value = Decimal("0")

    for line in lines:
        qty = int(line.get("quantity") or 0)
        unit_price = get_line_unit_price(line)
        line_total = unit_price * Decimal(qty)
        total_value += line_total

        inventory_item_id = get_nested(line, "variant", "inventoryItem", "id")
        available_qty = availability_by_item.get(inventory_item_id or "", 0)

        snapshot = deepcopy(line)
        snapshot["_available_qty"] = available_qty
        snapshot["_line_total"] = str(line_total)

        if inventory_item_id and available_qty >= qty:
            available_lines.append(snapshot)
            available_value += line_total
        else:
            remaining_lines.append(snapshot)

    available_percent = Decimal("0")
    if total_value > 0:
        available_percent = available_value / total_value

    return {
        "all_lines_count": len(lines),
        "available_lines": available_lines,
        "remaining_lines": remaining_lines,
        "available_count": len(available_lines),
        "remaining_count": len(remaining_lines),
        "available_value": available_value,
        "total_value": total_value,
        "available_percent": available_percent,
    }


def should_split(draft: Dict[str, Any], eval_data: Dict[str, Any]) -> Tuple[bool, List[str]]:
    reasons: List[str] = []

    if eval_data["all_lines_count"] == 0:
        reasons.append("No line items")
        return False, reasons

    if eval_data["available_count"] == eval_data["all_lines_count"]:
        reasons.append("All lines fully available; leave for instock-ready workflow")
        return False, reasons

    if eval_data["available_count"] < MIN_AVAILABLE_LINES:
        reasons.append(
            f"Available line count {eval_data['available_count']} < MIN_AVAILABLE_LINES {MIN_AVAILABLE_LINES}"
        )

    if eval_data["available_value"] <= MIN_AVAILABLE_VALUE:
        reasons.append(
            f"Available value {eval_data['available_value']} <= MIN_AVAILABLE_VALUE {MIN_AVAILABLE_VALUE}"
        )

    if eval_data["available_percent"] < MIN_AVAILABLE_PERCENT:
        reasons.append(
            f"Available percent {eval_data['available_percent']:.2%} < MIN_AVAILABLE_PERCENT {MIN_AVAILABLE_PERCENT:.0%}"
        )

    if eval_data["remaining_count"] < MIN_REMAINING_LINES:
        reasons.append(
            f"Remaining line count {eval_data['remaining_count']} < MIN_REMAINING_LINES {MIN_REMAINING_LINES}"
        )

    if eval_data["remaining_count"] < 1:
        reasons.append("No remaining lines would stay on parent")

    can_split, depth_reason = can_split_more(draft.get("poNumber") or "")
    if not can_split:
        reasons.append(depth_reason)

    return (len(reasons) == 0), reasons


def claim_draft(draft: Dict[str, Any]) -> None:
    tags = normalize_tags(draft.get("tags", []))
    if PROCESSING_TAG.lower() in {t.lower() for t in tags}:
        raise RuntimeError(f"{draft.get('name')} already has processing tag")
    update_draft(draft["id"], {"tags": add_tags(tags, PROCESSING_TAG)})


def process_draft(draft: Dict[str, Any], availability_by_item: Dict[str, int]) -> None:
    name = draft.get("name", draft.get("id", "<unknown>"))
    tags = normalize_tags(draft.get("tags", []))
    ship_date_raw = get_metafield_value(draft, "b2b", "ship_date") or ""

    if any(t.lower() == PROCESSING_TAG.lower() for t in tags):
        logger.info("%s | skipped | already processing", name)
        return

    if has_excluded_sku(draft):
        logger.info("%s | skipped | contains excluded SKU", name)
        return

    logger.info("\nProcessing %s (DRY_RUN=%s)", name, DRY_RUN)

    skip_for_ship_date, ship_date_reason = should_skip_for_ship_date(draft)
    if skip_for_ship_date:
        logger.info("%s | skipped | %s", name, ship_date_reason)
        return

    claim_draft(draft)

    eval_data = evaluate_draft(draft, availability_by_item)
    should_run, reasons = should_split(draft, eval_data)

    logger.info(
        "%s | po=%s | split_depth=%s | ship_date=%s | available_count=%s | remaining_count=%s | available_value=%s | total_value=%s | available_percent=%.2f%%",
        name,
        draft.get("poNumber") or "",
        split_depth_from_po(draft.get("poNumber") or ""),
        ship_date_raw,
        eval_data["available_count"],
        eval_data["remaining_count"],
        eval_data["available_value"],
        eval_data["total_value"],
        float(eval_data["available_percent"] * Decimal("100")),
    )

    if not should_run:
        logger.info("%s | no split | reasons=%s", name, reasons)
        update_draft(draft["id"], {"tags": remove_tags(draft.get("tags", []), PROCESSING_TAG)})
        return

    child_po = build_child_po(draft.get("poNumber") or "")
    logger.info("%s | thresholds passed | child_po=%s", name, child_po)

    # Step 1: Duplicate the parent.
    # draftOrderDuplicate copies everything — B2B purchasingEntity/company,
    # customer, addresses, payment terms, note — no need to pass any of those
    # fields manually in DraftOrderInput.
    child = duplicate_draft(draft["id"])
    logger.info("%s | duplicated -> %s", name, child.get("name") or child.get("id"))

    # Step 2: Update the duplicate with child-specific fields only.
    # If the update fails, roll back by deleting the duplicate.
    child_update = build_child_update_payload(draft, eval_data["available_lines"], child_po)
    try:
        child = update_draft(child["id"], child_update)
    except Exception as exc:
        logger.error(
            "%s | child update failed — rolling back duplicate %s: %s",
            name, child.get("id"), exc,
        )
        delete_draft(child["id"])
        raise

    logger.info(
        "%s | child updated -> %s (%s)",
        name,
        child.get("name") or child.get("id"),
        child.get("poNumber") or child_po,
    )

    # Step 3: Update the parent with remaining lines + done tag.
    parent_input = build_parent_update_payload(draft, eval_data["remaining_lines"])
    update_draft(draft["id"], parent_input)
    logger.info("%s | parent updated with remaining lines", name)

    append_split_log_row(
        parent=draft,
        child=child,
        eval_data=eval_data,
        ship_date=ship_date_raw,
        child_po=child_po,
    )


def collect_inventory_ids(drafts: List[Dict[str, Any]]) -> List[str]:
    ids: List[str] = []
    seen: Set[str] = set()

    for draft in drafts:
        for line in ((draft.get("lineItems") or {}).get("nodes") or []):
            inv_id = get_nested(line, "variant", "inventoryItem", "id")
            if inv_id and inv_id not in seen:
                ids.append(inv_id)
                seen.add(inv_id)

    return ids


# ----------------------------
# Main
# ----------------------------
def main() -> None:
    logger.info("SHOPIFY_SHOP=%s", SHOPIFY_SHOP)
    logger.info("SHOPIFY_API_VERSION=%s", SHOPIFY_API_VERSION)
    logger.info("DRY_RUN=%s", DRY_RUN)
    logger.info("ALLOW_OPEN_ENDED=%s", ALLOW_OPEN_ENDED)
    logger.info("MIN_AVAILABLE_LINES=%s", MIN_AVAILABLE_LINES)
    logger.info("MIN_AVAILABLE_VALUE=%s", MIN_AVAILABLE_VALUE)
    logger.info("MIN_AVAILABLE_PERCENT=%s", MIN_AVAILABLE_PERCENT)
    logger.info("MIN_REMAINING_LINES=%s", MIN_REMAINING_LINES)
    logger.info("MAX_SPLIT_DEPTH=%s", MAX_SPLIT_DEPTH)
    logger.info("EXCLUDE_SKUS=%s", sorted(EXCLUDE_SKUS) if EXCLUDE_SKUS else "(none)")
    logger.info("LINEAGE_NAMESPACE=%s", LINEAGE_NAMESPACE)
    logger.info("CSV_LOG_PATH=%s", CSV_LOG_PATH)

    drafts = fetch_open_drafts()
    if not drafts:
        logger.info("No eligible draft orders found.")
        return

    inventory_ids = collect_inventory_ids(drafts)
    availability = fetch_inventory_availability(inventory_ids)
    logger.info("Fetched inventory availability for %s inventory item(s)", len(availability))

    processed = 0
    failed = 0

    for draft in drafts:
        try:
            process_draft(draft, availability)
            processed += 1
        except Exception as exc:
            failed += 1
            logger.exception("%s | FAILED | %s", draft.get("name", draft.get("id")), exc)

    logger.info("----- DONE -----")
    logger.info("Processed: %s", processed)
    logger.info("Failed: %s", failed)


if __name__ == "__main__":
    main()

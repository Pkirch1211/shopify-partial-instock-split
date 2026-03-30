# shopify-partial-instock-split

Evaluates open Shopify draft orders for meaningful **partial in-stock** fulfillment opportunities.

If a draft is **not fully in stock**, this script checks whether the fully available portion of the order is operationally meaningful. If it is, the script creates a new child draft containing the fully available SKUs and leaves the remaining SKUs on the parent draft.

## What it does

For each eligible open draft order:

1. Skips drafts that should not be touched
2. Skips drafts whose `b2b.ship_date` is **7 days or more in the future**
3. Evaluates each line item against current available inventory at the configured Shopify location
4. Buckets lines into:
   - fully available
   - not fully available
5. Splits the order only if all of these are true:
   - at least `MIN_AVAILABLE_LINES` lines are fully available
   - fully available value is greater than `MIN_AVAILABLE_VALUE`
   - fully available value is at least `MIN_AVAILABLE_PERCENT` of total draft value
   - at least one line remains on the parent
   - split depth has not exceeded `MAX_SPLIT_DEPTH`

## Split hierarchy

This repo supports your existing PO patterns:

- `ROOT -> ROOT-BO`
- `ROOT-BO -> ROOT-BO.2`
- `ROOT-BO1 -> ROOT-BO1.2`
- `ROOT-BO3 -> ROOT-BO3.2`

And then it stops at the configured max depth.

## Logging

Every successful split appends a row to:

```text
logs/partial_instock_split_history.csv

# Data quality report

Source rows: 97 policies · 300 quotes · 79 transactions
Rows excluded from reporting (draft, lapsed or unbooked): 23

### Stage 0 — raw extracts, before standardisation

| Check | Result | Value |
|---|---|---|
| quote_id is unique | ❌ fail | 6 duplicate(s) |
| policy_no is unique | ❌ fail | 1 duplicate(s) |
| every policy_no exists in the parent | ❌ fail | 5 unmatched |
| product_code free of padding | ❌ fail | 117 padded value(s) |
| product_code consistently cased | ❌ fail | 91 mixed-case value(s) |

### Stage 1 — source data, after standardisation

| Check | Result | Value |
|---|---|---|
| row count above the floor | ✅ pass | 300 rows |
| quote_id is unique | ✅ pass | 0 duplicate(s) |
| policy_no is unique | ✅ pass | 0 duplicate(s) |
| product_code is unique | ✅ pass | 0 duplicate(s) |
| no nulls in product_code | ✅ pass | 0 null(s) |
| every quote_id exists in the parent | ✅ pass | 0 unmatched |

### Stage 2 — the joined fact table

| Check | Result | Value |
|---|---|---|
| policy_no is unique | ✅ pass | 0 duplicate(s) |
| no nulls in product_name | ✅ pass | 0 null(s) |
| no nulls in channel | ✅ pass | 0 null(s) |
| booked_on is not before issued_on | ✅ pass | 0 row(s) out of order |
| premium reconciles to the transaction system | ✅ pass | difference 0.00 |

### Stage 3 — the aggregated outputs

| Check | Result | Value |
|---|---|---|
| aggregate by product ties to the fact | ✅ pass | difference 0.00 |
| aggregate by month ties to the fact | ✅ pass | difference 0.00 |


**Overall: all checks passed**

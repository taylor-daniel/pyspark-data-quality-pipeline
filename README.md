# PySpark Data Quality Pipeline

A small, runnable pipeline that takes four messy source extracts, standardises them,
joins them into a fact table, classifies and aggregates it — and **checks itself at
every stage**, including a reconciliation against the system the business already trusts.

It is the sanitised, synthetic-data version of the pattern behind my
[Digital Sales & Revenue Analytics case study](https://github.com/taylor-daniel/data-analytics-portfolio/tree/main/case-studies/digital-sales-revenue-analytics):
quote, policy, product and transaction data from an insurance book, reported by product
and channel. **No real data appears anywhere in this repository** — `src/generate_data.py`
invents it, deliberately dirty.

```
raw extracts → standardise → join → filter → classify → aggregate → reconcile → outputs
                    ↑            ↑        ↑         ↑          ↑           ↑
                 checks       checks   checks    checks     checks      checks
```

---

## Why this exists

Most pipeline demos show the happy path. The hard part of this work is not the join —
it is knowing the join is right, and being able to prove it to someone who will act on
the number. So this repository leads with the checks:

| Stage | What is checked | Why it matters |
|---|---|---|
| 0 · raw | duplicates, orphan keys, padded and mixed-case keys | Measures the mess the pipeline absorbs. **Expected to fail** — that is the baseline. |
| 1 · standardised | the same checks, plus row-count floor and null keys | If these do not pass, nothing downstream is trustworthy |
| 2 · fact table | uniqueness, nulls, date ordering, **premium reconciled to the transaction system** | The reconciliation is what earns adoption |
| 3 · outputs | each aggregate ties back to the fact table | Catches a grouping that quietly drops rows |

A failing check does not stop the world quietly: `src/run.py` exits non-zero, so a
scheduler can act on it.

## Quickstart

```bash
python -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt

# run against the committed sample data
python -m src.run --raw sample-data --out data/out

# or generate a larger synthetic set first
python -m src.run --generate --quotes 4000
```

Requires Python 3.9+ and a JDK (PySpark needs Java 11 or newer).

Output:

```
Stage 0 — raw extracts, before standardisation
  [FAIL] quote_id is unique                 80 duplicate(s)
  [FAIL] product_code free of padding       1412 padded value(s)
  ...
Stage 1 — source data, after standardisation
  [PASS] quote_id is unique                 0 duplicate(s)
  ...
Stage 2 — the joined fact table
  [PASS] premium reconciles to the transaction system   difference 0.00
```

A full run writes `docs/quality-report.md` — a committed example is
[here](docs/quality-report.md) — plus four CSV outputs:

| Output | Grain |
|---|---|
| `fact_policies` | one row per issued, booked policy |
| `revenue_by_product_and_channel` | product × channel |
| `revenue_by_month` | booking month |
| `conversion_by_channel` | quotes, policies and conversion % per channel |

## The mess it has to handle

`generate_data.py` reproduces, on purpose, what operational extracts actually contain:

- **Padded and mixed-case keys** — ` mtr-01 ` and `MTR-01` are the same product
- **Duplicate rows** — the same quote arriving twice in one extract
- **Draft and lapsed policies** — real rows that must never reach revenue reporting
- **Related product codes** — `MTR-01` and `MTR-02` roll up to one product group
- **Two different dates** — a policy is issued on one day and booked on another, so
  monthly revenue uses the booking date and the pipeline asserts the ordering
- **Orphan transactions** — booked premium for a policy missing from the policy extract

## Design decisions worth noting

- **Standardise before joining.** A trailing space is the most common reason two correct
  datasets refuse to match, so keys are trimmed, collapsed and upper-cased first.
- **Filter after joining.** Draft policies are removed *after* the join, so the rows
  dropped can be counted and reported rather than silently vanishing.
- **Derive dimensions from the key.** The policy number carries the product group and the
  channel marker, so both are recovered from the key and then cross-checked against the
  free-text channel field (`channel_matches_source`).
- **Reconcile, don't assert.** The pipeline compares its own premium total against the
  transaction records for the same policies, and reports the difference.

## Project layout

```
src/
  generate_data.py   synthetic, deliberately dirty source extracts
  pipeline.py        standardise · classify · join · filter · aggregate
  quality.py         the checks, as composable functions returning a report
  run.py             end-to-end run + markdown report
tests/               12 tests: transformation rules and each check's failure mode
sample-data/         small synthetic CSVs, committed so the repo runs out of the box
docs/                an example quality report from a real run
```

## Tests

```bash
pip install pytest && pytest -q
```

Every check has a test that makes it fail, which is the only way to know a check works.
The transformation tests run on tiny hand-written frames, so they read as documentation
of the rules. One of them caught a genuine bug while this repo was being written: the
derived channel and the source channel were compared case-sensitively, so correct rows
were flagged as mismatched.

## Related

- [Case study: Digital Sales & Revenue Analytics](https://github.com/taylor-daniel/data-analytics-portfolio/tree/main/case-studies/digital-sales-revenue-analytics)
- [Portfolio](https://taylor-daniel.github.io) · [All case studies](https://github.com/taylor-daniel/data-analytics-portfolio)

## Licence

MIT — see [LICENSE](LICENSE).

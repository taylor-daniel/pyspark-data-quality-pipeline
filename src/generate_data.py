"""Generate synthetic source data for the pipeline demo.

The data is deliberately messy, in the same ways operational insurance data tends to be:
inconsistent casing and padding, duplicate rows, product codes that differ by suffix,
draft records that should never reach reporting, and a transaction date that does not
always match the policy issue date.

No real data is used anywhere in this repository.
"""
from __future__ import annotations

import argparse
import csv
import random
from datetime import date, timedelta
from pathlib import Path

CHANNELS = ["Intermediary portal", "Customer app"]
PRODUCT_GROUPS = {
    "MTR": ("Motor", ["MTR-01", "MTR-01 ", "mtr-01", "MTR-02"]),
    "TRV": ("Travel", ["TRV-11", "TRV-11", "trv-12"]),
    "HHD": ("Household", ["HHD-21", "HHD-21 ", "HHD-22"]),
}
STATUSES = ["ISSUED", "issued", "Issued", "DRAFT", "LAPSED"]


def _pad(value: str, rng: random.Random) -> str:
    """Randomly reintroduce the padding and casing noise real extracts carry."""
    if rng.random() < 0.18:
        return f"  {value} "
    if rng.random() < 0.12:
        return value.lower()
    return value


def generate(out_dir: Path, n_quotes: int = 4000, seed: int = 42) -> dict[str, Path]:
    rng = random.Random(seed)
    out_dir.mkdir(parents=True, exist_ok=True)
    start = date(2025, 1, 1)

    quotes, policies, transactions = [], [], []
    product_rows = []

    for group, (label, codes) in PRODUCT_GROUPS.items():
        for code in {c.strip().upper() for c in codes}:
            product_rows.append(
                {"product_code": code, "product_group": group, "product_name": label}
            )

    for i in range(1, n_quotes + 1):
        group = rng.choices(list(PRODUCT_GROUPS), weights=[5, 3, 2])[0]
        _, codes = PRODUCT_GROUPS[group]
        code = rng.choice(codes)
        channel = rng.choices(CHANNELS, weights=[6, 4])[0]
        quoted_on = start + timedelta(days=rng.randint(0, 364))
        quote_id = f"Q{i:06d}"

        quotes.append(
            {
                "quote_id": quote_id,
                "product_code": _pad(code, rng),
                "channel": _pad(channel, rng),
                "quoted_on": quoted_on.isoformat(),
                "quoted_premium": round(rng.uniform(12_000, 480_000), 2),
            }
        )

        # roughly a third of quotes convert; the rest never become policies
        if rng.random() < 0.34:
            status = rng.choices(STATUSES, weights=[6, 2, 2, 2, 1])[0]
            issued_on = quoted_on + timedelta(days=rng.randint(0, 21))
            # the policy number carries the product group and the channel marker
            policy_no = f"{group}/{'IP' if channel == CHANNELS[0] else 'CA'}/{i:06d}"
            policies.append(
                {
                    "policy_no": policy_no,
                    "quote_id": quote_id,
                    "product_code": _pad(code, rng),
                    "status": status,
                    "issued_on": issued_on.isoformat(),
                }
            )
            if status.strip().upper() == "ISSUED":
                # the transaction system books the premium, sometimes a few days later
                booked_on = issued_on + timedelta(days=rng.randint(0, 9))
                transactions.append(
                    {
                        "txn_id": f"T{i:06d}",
                        "policy_no": policy_no,
                        "booked_on": booked_on.isoformat(),
                        "premium": round(rng.uniform(12_000, 480_000), 2),
                    }
                )

    # duplicates: the same rows arriving twice in an extract
    quotes += rng.sample(quotes, k=max(1, len(quotes) // 50))
    if policies:
        policies += rng.sample(policies, k=max(1, len(policies) // 60))

    # a handful of transactions for policies that are not in the policy extract
    for n in range(1, 6):
        transactions.append(
            {
                "txn_id": f"T9{n:05d}",
                "policy_no": f"MTR/IP/99{n:04d}",
                "booked_on": (start + timedelta(days=rng.randint(0, 364))).isoformat(),
                "premium": round(rng.uniform(20_000, 90_000), 2),
            }
        )

    paths = {}
    for name, rows in [
        ("quotes", quotes),
        ("policies", policies),
        ("products", product_rows),
        ("transactions", transactions),
    ]:
        path = out_dir / f"{name}.csv"
        with path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)
        paths[name] = path
    return paths


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", default="data/raw", help="output directory")
    parser.add_argument("--quotes", type=int, default=4000, help="number of quotes to generate")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    written = generate(Path(args.out), n_quotes=args.quotes, seed=args.seed)
    for name, path in written.items():
        print(f"{name:13} -> {path}")

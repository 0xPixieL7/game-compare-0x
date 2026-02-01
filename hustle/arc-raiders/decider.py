#!/usr/bin/env python3
"""ARC Raiders Hustle — Autonomous Decision Layer

Purpose
- Consume the solver output and produce an action decision:
  - `EXECUTE_TRADE <ITEM>` when rules pass
  - `PASS` when rules fail

This provides *capacity to decide* (not just report).

Inputs
- By default, imports `solver.py` and calls `calculate_roi()`.
- Optionally checks that required market prices exist in `prices.json` to reduce risk.

Rules (strict, override via CLI flags)
- ROI >= 50%
- Profit >= 0.50 (USD)
- Risk check: all direct ingredients for the chosen craft exist in prices.json

Usage
  python3 decider.py
  python3 decider.py --min-roi 75 --min-profit 1.0

Exit codes
- 0: EXECUTE_TRADE
- 2: PASS
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Dict, List, Any, Tuple


HERE = os.path.dirname(os.path.abspath(__file__))
PRICES_PATH = os.path.join(HERE, "prices.json")


def load_raw_price_names(path: str = PRICES_PATH) -> set[str]:
    """Loads raw item names present in prices.json (normalized similarly to solver)."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)
    except Exception:
        return set()

    names: set[str] = set()
    for item in raw or []:
        name = (item.get("name") or "").strip()
        if not name:
            continue
        # normalize bundle patterns loosely: strip any (xN) suffix
        if "(" in name and ")" in name and "x" in name.lower():
            base = name.split("(")[0].strip()
            if base:
                names.add(base)
                continue
        names.add(name)
    return names


def best_candidate(results: List[Dict[str, Any]]) -> Dict[str, Any] | None:
    if not results:
        return None
    # solver sorts by ROI desc, but we want "profit first" tie-break ROI (as requested later)
    return sorted(results, key=lambda r: (r.get("Profit", 0), r.get("ROI", 0)), reverse=True)[0]


def risk_check_ingredients_present(product: str, raw_names: set[str]) -> Tuple[bool, List[str]]:
    """Ensure direct ingredients for the product have explicit market presence.

    This is a conservative liquidity check: if an input doesn't exist in the data layer,
    we treat it as higher risk and return FAIL.
    """
    try:
        import solver  # local file in same folder
    except Exception:
        return False, ["solver_import_failed"]

    recipe = getattr(solver, "RECIPES", {}).get(product)
    if not recipe:
        return False, ["missing_recipe"]

    missing = []
    for ing in recipe.get("ingredients", {}).keys():
        if ing not in raw_names:
            missing.append(ing)
    return (len(missing) == 0), missing


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--min-roi", type=float, default=50.0)
    ap.add_argument("--min-profit", type=float, default=0.50)
    ap.add_argument("--no-risk-check", action="store_true", help="Skip price-layer ingredient presence checks")
    args = ap.parse_args()

    # 1) Load solver results
    try:
        import solver  # same directory
        results = solver.calculate_roi()
    except Exception as e:
        print(f"PASS  # solver_error: {e}")
        return 2

    cand = best_candidate(results)
    if not cand:
        print("PASS  # no_candidates")
        return 2

    product = cand.get("Product") or cand.get("ITEM") or ""
    roi = float(cand.get("ROI", 0) or 0)
    profit = float(cand.get("Profit", 0) or 0)

    # 2) Expert rules
    if roi < args.min_roi:
        print(f"PASS  # roi<{args.min_roi}: {roi}")
        return 2

    if profit < args.min_profit:
        print(f"PASS  # profit<{args.min_profit}: {profit}")
        return 2

    # 3) Risk check
    if not args.no_risk_check:
        raw_names = load_raw_price_names()
        ok, missing = risk_check_ingredients_present(product, raw_names)
        if not ok:
            print(f"PASS  # risk_check_failed: missing_prices={','.join(missing)}")
            return 2

    # 4) Decision
    print(f"EXECUTE_TRADE {product}  # profit={profit:.2f} roi={roi:.2f}%")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

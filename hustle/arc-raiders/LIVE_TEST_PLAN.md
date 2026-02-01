# LIVE_TEST_PLAN — ARC Raiders Hustle (Tempest IV)

Purpose: **Verify real-world arbitrage** by executing **one full craft → sale loop** for Tempest IV and confirming that the market price is real + liquid (you can actually buy/sell at the expected prices).

This is intentionally small: **1 unit** of Tempest IV.

---

## Sources (where to check prices)

### Primary market page
The project’s indexer is aimed at Odealo’s ARC Raiders listings:
- URL: https://odealo.com/games/arc-raiders/items
- (This is also the page referenced in `hustle/arc-raiders/indexer.py`.)

### If Odealo differs / is down
Use this search query and compare 2+ marketplaces:
- Search query: `ARC Raiders Tempest IV price` (and repeat for each ingredient)

Record the price you actually see in the moment.

---

## What we’re testing (current proof-machine assumptions)

From our data layer + solver:
- Tempest IV market sell price: **~$4.50** (from `prices.json`)
- Tempest III market price: **estimated** via coin ratio (Tempest II → Tempest III scaling) → ~$0.93
- Ingredients are cheap enough that the solver predicts positive profit on Tempest IV.

**Goal:** confirm you can actually execute these buys and sells at close to these prices.

---

## Step-by-step: ONE Tempest IV loop

### Step 0 — Create a “trade log” entry (2 minutes)
Open a note and capture:
- Date/time
- Marketplace used (Odealo or other)
- Your region/server if applicable
- Any fees (marketplace cut, listing fee, etc.)

### Step 1 — Verify prices before buying (5 minutes)
On the marketplace listing pages, confirm the *current* unit prices:

1) **Advanced Mechanical Components**
- Target buy: **2 units**
- Expected unit price: ~$0.04 (from `prices.json`)
- Source: Odealo listing page for ARC Raiders items (search within page)

2) **Medium Gun Parts**
- Target buy: **3 units**
- Expected unit price: ~$0.05 (from `prices.json`)
- Source: Odealo listing page for ARC Raiders items

3) **Tempest III**
- Target buy: **1 unit**
- Expected price: **~$0.93** (ESTIMATED — not scraped)
- Source:
  - Preferred: marketplace listing for Tempest III (real)
  - Fallback: craft path (below) if Tempest III can be crafted reliably

4) **Tempest IV** (sell target)
- Target sell: **1 unit**
- Expected sell price: **~$4.50** (from `prices.json`)
- Source: Odealo listing page (Tempest IV)

If any of these are missing/unavailable, stop and record **which item is illiquid**.

### Step 2 — Execute buys (10–20 minutes)
Buy exactly:
- **2× Advanced Mechanical Components**
- **3× Medium Gun Parts**
- **1× Tempest III**

Record:
- the exact prices paid
- whether you had to buy from multiple sellers
- how long it took to find a seller

### Step 3 — If Tempest III is not buyable, craft it instead (optional branch)
Our current recipe chain in `solver.py` assumes:
- Tempest III = **1× Tempest II + 2× High-Grade Gun Parts**

So if Tempest III is illiquid:
1) Check if Tempest II + High-Grade Gun Parts are liquid.
2) Buy/craft them.
3) Craft Tempest III.

Record this as a **liquidity failure** for Tempest III (because the loop depends on it).

### Step 4 — Craft Tempest IV (in-game)
Craft using:
- 2× Advanced Mechanical Components
- 3× Medium Gun Parts
- 1× Tempest III

Record:
- crafting station/location used
- any crafting time/cost

### Step 5 — Sell Tempest IV (key proof step)
List/sell **1× Tempest IV**.

Record:
- sell price you achieved
- time-to-sell (minutes)
- if you had to discount to get a buyer
- any fees/cuts

---

## Pass/Fail criteria (Proof of Profit)

A “PASS” requires:
- You successfully bought/crafted all inputs
- You successfully sold Tempest IV
- **Net profit is positive after fees**
- Time-to-buy + time-to-sell is reasonable (you define what “reasonable” is; start with <60 minutes)

A “FAIL” (still useful) is any of:
- Any ingredient has **no sellers** / can’t be purchased
- Tempest IV can’t be sold near the observed market price
- Fees wipe out profit
- Time-to-sell is too long (liquidity is weak)

---

## What I need from you after you run it
Reply with:
- Marketplace used
- Prices you paid for each ingredient
- Whether Tempest III was bought or crafted
- Sale price achieved for Tempest IV
- Fees (if any)
- Time-to-sell

Then I’ll update the solver assumptions (and the “profit engine” status) using your real execution data.

# ARC Raiders Hustle — Web Frontend (Phase 2)

## Goal
Build a simple, high-conversion web funnel:

1) **Landing** → explains the value prop and credibility
2) **Calculator** → shows profit/ROI for top crafts (from the solver output)
3) **Lead Capture** → collect interested users (email/WhatsApp) for alerts or a paid tier

Target outcome: a deployable static site (can start as HTML/CSS/JS), optimized for mobile.

---

## Pages

### 1) Landing Page
**Objective:** Convert cold traffic into “Try the calculator”.

Must include:
- One clear headline (what it does)
- Subheadline (why it matters)
- Primary CTA: **Open Calculator**
- Social proof section (placeholders are fine): “Updated from real market prices”, “Top crafts updated daily”, etc.
- Trust notes: “No login required”, “Fast”, “Mobile-first”

Suggested sections:
- Hero (headline + CTA)
- "How it works" (3 steps)
- "What you get" (ROI + profit alerts)
- FAQ
- Footer

---

### 2) Calculator Page
**Objective:** Show the top 3 profitable crafts with transparent math.

Inputs (MVP):
- Prices source: `prices.json` (or pasted JSON)
- Recipes: embedded (same as solver) OR loaded from a JSON file
- Optional: fees % (marketplace cut), crafting time, risk buffer

Outputs:
- Table: Item | Cost | Revenue | Profit | ROI
- Highlight the top 3
- “Update prices” button (manual refresh for now)

Important:
- Show whether any item relies on estimated prices (flag as “ESTIMATED”)

---

### 3) Lead Capture Page (or modal)
**Objective:** Capture a contact for notifications.

Fields:
- Email OR WhatsApp number
- Optional: preferred items (Tempest IV, etc.)

Copy:
- “Get alerts when profit spikes”
- “No spam. Unsubscribe anytime.”

Storage (Phase 2 MVP):
- Start with a simple form POST to:
  - Google Forms, or
  - a lightweight serverless endpoint (later)

---

## Design Requirements
- Mobile-first, fast load
- Large tap targets
- Minimal steps to value (Landing → Calculator in 1 click)
- Dark mode preferred (game audience)

---

## Deliverables
- A folder structure suitable for static deployment:
  - `index.html` (landing)
  - `calculator.html`
  - `lead.html` (or modal)
  - `styles.css`
  - `app.js`
- A small JSON contract for calculator input/output

---

## Notes for Feature Architect
- Keep the first version static: no auth, no database.
- The “Proof Machine” output should be embedded into the calculator in a way that can later swap to live refresh.
- Prioritize **clarity + trust + speed** over fancy UI.

# Task List: Navigation Restructuring & Landing Page Restoration

## Relevant Files
- `routes/web.php` - Define the new `/arc-raiders` route.
- `resources/js/pages/welcome.tsx` - Restoration of the original game discovery landing page.
- `resources/js/pages/ArcRaiders.tsx` - New dedicated page for ARC Raiders with payment integration.
- `resources/js/components/Header.tsx` - Add global navigation for ARC Raiders.
- `resources/js/components/GameCard.tsx` - (or equivalent) Ensure cards link to show pages.
- `app/Http/Controllers/LandingController.php` - Ensure data flow for spotlight/rows is active.

### Notes
- **Payments**: For Solana and EVM crypto payments, consider using a unified provider like **Helio** or separate integrations (Solana Pay + RainbowKit/Wagmi).
- **Performance**: Use `React.lazy` for high-fidelity 3D components to ensure the initial page load is extremely fast.
- **Link Integrity**: Verify that `toUrl(home())` and other route helpers correctly resolve after the restructuring.

## Tasks
- [ ] **1.0 Frontend Page Restructuring**
  - [ ] 1.1 Finalize `resources/js/pages/ArcRaiders.tsx` using the current ARC Raiders logic.
  - [ ] 1.2 Restore `resources/js/pages/welcome.tsx` with the original imports (`SpotlightCarousel`, `EndlessCarousel`, `Header`).
  - [ ] 1.3 Ensure `LandingController@index` returns the required props (`hero`, `spotlightGames`, `rows`) for the restored landing.
- [ ] **2.0 ARC Raiders Payment Integration**
  - [ ] 2.1 Implement Credit Card checkout (e.g., Stripe integration).
  - [ ] 2.2 Implement Crypto payment support for **Solana** (Solana Pay or Helio).
  - [ ] 2.3 Implement Crypto payment support for **EVM Chains** (Ethereum, Base, Polygon).
  - [ ] 2.4 Add "Support the Project" or "Buy Tools" UI section to the ARC Raiders page.
- [ ] **3.0 Game Card Navigation Restoration**
  - [ ] 3.1 Identify the card component used in `EndlessCarousel` (likely `GameCard.tsx`).
  - [ ] 3.2 Wrap the card UI in an Inertia `<Link>` component pointing to `/games/{id}`.
  - [ ] 3.3 Verify that clicking a card from any row (Action, RPG, etc.) navigates to the correct show page.
- [ ] **4.0 Routing & Global Navigation**
  - [ ] 4.1 Update `routes/web.php` to define `Route::get('/arc-raiders', ...)->name('arc-raiders')`.
  - [ ] 4.2 Update `resources/js/components/Header.tsx` to add "ARC Raiders" to the `navigation` array.
  - [ ] 4.3 Ensure the site logo/Home link correctly routes to the restored `/` root.
- [ ] **5.0 Performance & Integrity Audit**
  - [ ] 5.1 Optimize 3D spatial components with `Suspense` and `lazy` loading.
  - [ ] 5.2 Verify that all links in the Header, Footer, and Game Rows are functional.
  - [ ] 5.3 Test page load speed across all primary routes to ensure "World-Class" responsiveness.

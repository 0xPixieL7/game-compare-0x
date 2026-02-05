# PRD: Navigation Restructuring & Landing Page Restoration

## 1. Goal
Restore the original game-focused landing page as the primary root (`/`) experience, while promoting the ARC Raiders content to a dedicated, high-visibility link in the global header.

## 2. User Stories
- As a **visitor**, I want to arrive at a visually stunning landing page featuring game spotlights and rows, so I can immediately explore game comparisons.
- As an **ARC Raiders player**, I want to find the specialized calculator and profit guides easily from any page via the header.
- As a **developer**, I want a clean separation between general game discovery and specialized niche content (like ARC Raiders).

## 3. Functional Requirements
- **Root Restoration**: The `/` route must render the original `welcome.tsx` experience (Spotlight Carousel + Endless Game Rows).
- **ARC Raiders Migration**: The current ARC Raiders landing content must be moved to its own page (`ArcRaiders.tsx`) and served via a new route (`/arc-raiders`).
- **Global Navigation Update**: The `Header.tsx` component must be updated to include an "ARC Raiders" link in the main navigation links.
- **Consistent Branding**: Both the restored landing page and the ARC Raiders page must use the unified `Header` and `Footer` components.

## 4. Technical Implementation
- **Controller**: Update `LandingController@index` to ensure it continues providing the necessary data for the game rows and spotlight.
- **Routing**: Define `Route::get('/arc-raiders', ...)` in `web.php`.
- **Frontend**:
    - Restore `welcome.tsx` using the original imports (`SpotlightCarousel`, `EndlessCarousel`, `Header`).
    - Update `Header.tsx` to add the new link.
    - Ensure `ArcRaiders.tsx` correctly implements the `Header` component for navigation.

## 5. Acceptance Criteria
- [ ] Root `/` displays the game spotlight and endless scroll rows.
- [ ] `/arc-raiders` displays the ARC Raiders profit calculator landing.
- [ ] The header bar contains a functional link to "ARC Raiders".
- [ ] The "Home" logo link correctly returns to the game discovery landing.

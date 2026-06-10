---
plan: remote-ui-access/04-mobile-responsive
kind: leaf
status: planned
complexity: medium
depends: []
parallel: false
branch: feat/ui-mobile-responsive
pr: ""
---

# Mobile responsiveness pass + ui_smoke mobile-viewport run

## Goal

Make Dashboard/Data/Historical/Live usable on a phone (≈390-px viewport): wide tables
stack into cards, tap targets are finger-sized, the nav and its dropdowns work on a
narrow screen. Add a mobile-viewport pass to `ui_smoke.py`.

## Files to change
- `dccd/interfaces/ui/templates/base.html`
  - The `<meta name="viewport">` already exists (line 5) — good. Add a `@media
    (max-width: 640px)` block to the inline `<style>`:
    - `nav` already `flex-wrap`s; ensure brand + menu buttons stay tappable
      (≥44px height), reduce horizontal padding, let the nav wrap cleanly.
    - Make `.nav-menu-items` dropdowns full-width / properly positioned on narrow
      screens (they're `position:absolute` — verify they don't overflow the viewport).
    - Generic table rule: under the breakpoint, allow horizontal scroll
      (`overflow-x:auto` wrappers) **or** stacked rows; pick the approach per table
      below.
  - **Coordinate with leaf 02**: 02 edits `base.html` nav (Logout button). This leaf
    is kept serial (not `parallel`) so the two don't collide — execute it before 02 or
    after 02 merges, rebasing onto the latest `base.html`.
- `dccd/interfaces/ui/templates/{dashboard,data,historical,live}.html`
  - Wrap their wide tables in a responsive container and add per-page `@media` tweaks
    (KPI cards stack to one column; accordion rows reflow; the Historical/Live
    action buttons — Run/Delete/Start/Stop — get adequate spacing/size on touch).
  - Keep desktop layout byte-identical above the breakpoint (only add `@media`
    rules; do not restructure the default styles).
- `doc/dev/ui_smoke.py`
  - Add a mobile-viewport pass: after the existing desktop checks, set
    `await page.set_viewport_size({"width": 390, "height": 844})` (iPhone-ish) and
    re-run the core navigation checks (Dashboard loads, nav dropdowns open and route,
    Data/Historical/Live render). Add a check that no element overflows the viewport
    width (e.g. evaluate `document.documentElement.scrollWidth <=
    window.innerWidth + 1` on each page) and `step(...)` it.

## Steps
1. Add the `@media (max-width:640px)` rules to `base.html` and per-page templates.
2. Pick a consistent table strategy (recommend: horizontal-scroll wrappers for dense
   data tables on Data/Historical/Live; card-stack for the Dashboard KPIs).
3. Extend `ui_smoke.py` with the 390-px pass + the no-horizontal-overflow assertion.
4. Keep all changes additive (media queries) — desktop unchanged.

## Tests
- No unit test (pure templates/CSS). The gate is `ui_smoke.py` (now including the
  mobile pass) plus the manual real-device check below. (`pytest` must still pass —
  no Python behaviour changed.)

## Verification on real data
- Start an isolated `dccd ui` on a temp config/data; run
  `python doc/dev/ui_smoke.py http://127.0.0.1:<port>` → all steps PASS, **including
  the new mobile-viewport pass and the no-horizontal-overflow check**.
- On a **real phone** (over Tailscale/the leaf-01 proxy): load each page, confirm no
  horizontal scrolling of the whole page, dropdowns open and route, the
  Run/Start/Stop/Delete buttons are tappable, tables are readable (scroll or stacked).
  Capture a screenshot or note per page in the PR.

## Closeout
- CHANGELOG (`Changed`): "Responsive layout for Dashboard/Data/Historical/Live on
  narrow (mobile) viewports; `ui_smoke.py` gains a mobile-viewport pass (#NN)".
- ADR: none — UI/CSS pass, no architectural decision (state "none — presentation
  only" at finish).
- Status/roadmap: note mobile pass in `06-status.md`; roadmap removal deferred to leaf
  05.

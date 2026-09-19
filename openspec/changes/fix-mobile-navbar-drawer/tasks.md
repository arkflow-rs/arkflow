## 1. CSS fix

- [x] 1.1 In `docs/src/css/custom.css` (~line 707), remove `backdrop-filter: blur(12px) saturate(160%)` from the `.navbar` rule and re-host it on a new `.navbar::before` layer (`content: ''; position: absolute; inset: 0; z-index: -1; backdrop-filter: blur(12px) saturate(160%)`), keeping the translucent `background-color` and `border-bottom` on `.navbar` itself; add a short comment stating the constraint (an ancestor of the fixed `navbar-sidebar` must not carry `backdrop-filter`/`filter`/`transform`, or the mobile drawer clips to the navbar box)
- [x] 1.2 Confirm the `@supports not (backdrop-filter …)` fallback block needs no change (solid background still on `.navbar`; the pseudo's declaration is inert without support) and leave it untouched

## 2. Visual verification (dev server)

- [x] 2.1 At a 375×667 viewport on a docs page (e.g. `/docs/get-started/quickstart`), tap the hamburger and confirm the drawer covers the full viewport height with Docs/SQL/API/Blog, the version selector, and the docs sidebar entries visible and tappable; repeat on the landing page; also confirm light and dark themes
- [x] 2.2 Confirm the navbar bar itself renders unchanged — translucent tint over blurred content plus hairline border — at phone and desktop (>996px) widths, in light and dark themes
- [x] 2.3 Confirm scrolled-state behavior is unchanged: navbar hides on scroll down and reappears on scroll up (`hideOnScroll` stays), and no white flash or paint artifact appears where the pseudo layer sits

## 3. Gates

- [x] 3.1 Run `pnpm docs:check` and `pnpm build` in `docs/` and confirm both pass unchanged

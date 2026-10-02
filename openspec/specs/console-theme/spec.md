## Purpose

Define the console's visual theme system: dual themes on shared semantic design tokens, a persisted three-state theme switcher, monospace data typography, and accessible status colors.

## Requirements

### Requirement: Visual theme system

The console SHALL ship a dark theme (default) and a light theme built on shared semantic design tokens (background, surface, border, text, muted, accent, and status colors for ok/danger/warn). A theme switcher in the top bar SHALL offer dark, light, and follow-system; the choice SHALL persist in `localStorage`, and the first visit SHALL follow the operating system preference. Data values (identifiers, metrics, timestamps) SHALL render in a monospace face with tabular numerals in tables. All state colors SHALL meet a text contrast ratio of at least 4.5:1 in both themes.

#### Scenario: Switch to light theme
- **WHEN** an operator selects the light theme in the top bar
- **THEN** every surface, border, text, and status color re-renders using the light token mapping, and the choice persists across reloads

#### Scenario: First visit follows the system
- **WHEN** an operator opens the console for the first time with no stored theme preference
- **THEN** the console renders using the operating system's color scheme preference

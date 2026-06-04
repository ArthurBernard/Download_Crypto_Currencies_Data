# Documentation plan — toward numpy/PyTorch-grade docs

Goal: docs a new user can *learn* from, an expert can *trust*, and a contributor
can *extend* — not just an auto-generated API dump with a nice theme.

The current site is visually fine but **structurally a reference**: it explains
*what exists*, rarely *how to do a task* or *why things work the way they do*,
and its API reference is only as good as the (currently thin) docstrings. Big
libraries succeed because they separate four documentation modes (the
**Diátaxis** model) and invest most in the first two:

| Mode | Question it answers | We have | Target |
|------|--------------------|---------|--------|
| **Tutorials** (learning) | "Teach me, step by step" | ❌ none | 2–3 |
| **How-to guides** (tasks) | "How do I do X?" | ❌ none | 8–12 |
| **Reference** (information) | "What are the exact args?" | 🟡 thin docstrings | full + tested |
| **Explanation** (understanding) | "Why / how does it work?" | 🟡 architecture only | 5–6 concepts |

---

## Part A — Critical analysis, page by page

Honest assessment of what ships today.

### `index.rst` — landing
- 🟡 Decent hero/cards, but the feature list is generic and **duplicates** the
  exchanges table that also lives in `exchanges.rst`.
- ❌ No "**what problem does this solve / why dccd vs ccxt**" framing.
- ❌ No *above-the-fold* win: a 5-line snippet **with its output** that proves
  value in 10 seconds.
- ❌ Cards don't map to the four doc modes (no "Tutorials" / "How-to" entry).

### `installation.rst`
- 🟢 Covers extras + shell completion.
- ❌ No "verify it worked" with **shown output**; no troubleshooting; no rationale
  for each extra; no note on Python 3.11–3.13 / Polars/pyarrow wheels.

### `quickstart.rst`
- 🟡 Has Python/CLI/stream snippets.
- ❌ Snippets show **no output** — numpy/PyTorch always show the result (a
  DataFrame head, row counts). The reader can't tell what success looks like.
- ❌ No narrative ("what you just did", "next steps"); no progression
  (backfill → read → inspect → plot).
- ❌ Not runnable as doctests, so it can silently rot.

### `architecture.rst`
- 🟢 The strongest page — layers, data flow, design rules, grid cards.
- 🟡 ASCII diagram instead of a real figure; no link to source; could host the
  "capabilities model" explanation (currently nowhere).

### `exchanges.rst`
- 🟢 Good capability + fidelity matrices, per-exchange notes.
- ❌ No per-exchange **code example**; no link from each adapter's API page back
  to this guide.

### `cli.rst`
- ❌ **Hand-written → drifts from the Typer app.** Options are summarised in prose,
  not exhaustive; no per-command option tables, no example output, no exit codes.
  Should be **generated** (`sphinx-click` / typer) so it can't lie.

### `http-api.rst`
- ❌ **Hand-written tables → drift from FastAPI.** No request/response **schemas**
  (the Pydantic models), no real JSON response examples, no error/status matrix.
  FastAPI already produces an OpenAPI spec — the reference should be **rendered
  from it** (`sphinxcontrib-openapi` or a committed `openapi.json`), not retyped.

### `web-ui.rst`
- 🟢 Page-by-page guide + one screenshot.
- 🟡 One screenshot only; no per-page images, no annotated tour, no GIF of a
  backfill running.

### `configuration.rst`
- ❌ **Not exhaustive and hand-maintained.** Missing a complete field reference
  (every key, type, default, validation rule). Should be **generated from the
  `AppConfig` Pydantic schema** (e.g. `autodoc-pydantic`) so defaults/constraints
  are always correct, plus a copy-pasteable annotated example.

### `api.rst`
- 🟢 Now autosummary tables per layer + 37 generated object pages (good bones).
- ❌ **The bottleneck is the docstrings, not the RST.** Only 12/40 modules have
  `Examples`; most public functions/classes have a one-line summary, partial
  `Parameters`, **no `Examples`, no `Notes`, no `See Also`, no `References`**.
  Generated pages are therefore sparse. This is the single biggest quality gap.

### `changelog.rst`
- 🟢 Includes `CHANGELOG.md`. Fine.

### Cross-cutting gaps
- ❌ **No tutorials, no how-to guides, no concept pages** beyond architecture.
- ❌ **No tested examples** — `sphinx.ext.doctest` is not enabled, so every
  `>>>` can rot. numpy/PyTorch run docs examples in CI.
- ❌ **No examples gallery / cookbook**.
- ❌ Hand-written reference (CLI/HTTP/config) **drifts** from code.
- ❌ Weak cross-linking; no `See Also`; no intersphinx links to polars/pydantic
  in prose.
- ❌ No doc **CI** (build-with-`-W`, doctest, linkcheck), no versioned site story.

---

## Part B — The quality bar (what "numpy-grade" means here)

1. **Diátaxis structure** — separate Tutorials / How-to / Reference / Explanation;
   never mix "teach" and "look up" on one page.
2. **Every public object has a complete numpydoc docstring**: one-line summary →
   extended description → `Parameters` → `Returns`/`Yields` → `Raises` →
   `See Also` → `Notes` → `Examples` (with output) → `References` where relevant.
3. **Examples are executed in CI** (doctest), so they cannot rot and double as
   tests.
4. **Reference is generated, not retyped** (CLI from Typer, HTTP from OpenAPI,
   config from the Pydantic schema, API from docstrings).
5. **Show, don't tell** — outputs, tables, figures, screenshots, copy buttons.
6. **Navigable & cross-linked** — `See Also`, intersphinx to polars/pydantic/
   python, a real search, "edit on GitHub", last-updated, versions.
7. **Builds clean with `-W`** (warnings = errors) and passes `linkcheck`.

---

## Part C — Target information architecture

```
Home (value prop + 10-second win + mode cards)
├─ Tutorials                 (learning, end-to-end, runnable)
│   ├─ Your first backfill (OHLC → read → plot)
│   ├─ Streaming live trades to Parquet
│   └─ Running the daemon + web UI
├─ How-to guides             (task recipes, short)
│   ├─ Backfill deep trade history (and cancel a runaway)
│   ├─ Schedule daily collection
│   ├─ Read & analyse stored data in Polars/Pandas
│   ├─ Sync data to S3/GCS with rclone
│   ├─ Migrate v2 data to v3
│   ├─ Protect the web UI with a token
│   ├─ Add a new exchange adapter
│   └─ Derive OHLC from trades
├─ Explanation               (concepts)
│   ├─ Architecture (hexagonal)            ← keep, enrich
│   ├─ The capabilities model
│   ├─ Pagination: windows vs cursors
│   ├─ Timestamps, timezones & alignment
│   ├─ Storage layout, dedup & integrity
│   └─ Exchanges: capabilities & fidelity  ← move from reference
├─ Reference                 (generated)
│   ├─ Python API (autosummary, rich docstrings)
│   ├─ CLI (generated from Typer)
│   ├─ HTTP API (rendered from OpenAPI)
│   ├─ Configuration (generated from AppConfig schema)
│   └─ Web UI
├─ Examples gallery          (cookbook, optional Phase 4)
└─ Changelog · Contributing
```

---

## Part D — Docstring standard & coverage (the core work)

This is ~70% of the value. Every public symbol in `dccd/{domain,transport,
sources,storage,application}` and `dccd.Client` gets a full numpydoc docstring.

Template (function):

```python
def backfill(spec, *, registry, store, ...):
    r"""Download historical data for *spec* into the Parquet store.

    <2–4 sentences: what it does, when to use it, key behaviour (resume,
    cursor draining for trades, cancellation).>

    Parameters
    ----------
    spec : JobSpec
        ...
    Returns
    -------
    dict
        ``{'run_id', 'rows_written', 'start_ns', 'end_ns'}`` on success ...
    Raises
    ------
    NoCapability
        If the exchange can't serve this data type/history.
    See Also
    --------
    stream : live collection. read : load stored data.
    Notes
    -----
    Trades are cursor-paginated and drain the full window; ``start='last'`` on an
    empty dataset uses a bounded look-back.
    Examples
    --------
    >>> import asyncio
    >>> from dccd import Client
    >>> async def go():
    ...     async with Client() as c:
    ...         r = await c.backfill('binance', 'BTC/USDT', 'ohlc', span=3600,
    ...                              start='2024-01-01')
    ...         return r['rows_written']
    >>> asyncio.run(go())  # doctest: +SKIP
    """
```

Rules: English only; runnable `Examples` (mark network ones `# doctest: +SKIP`,
keep at least one pure-domain example per module that actually runs); add
`See Also` cross-links; `Notes` for caveats (rate limits, recent-only, dedup).

Priority order: ① `dccd.Client` & `application.operations` (most-read) → ②
`domain` (records, symbol, capability, transforms — pure, easy doctests) → ③
`storage` & `transport.paginate` → ④ the 7 `sources` adapters.

---

## Part E — Tooling & config upgrades

- Enable **`sphinx.ext.doctest`**; add a `make doctest` and run it in CI.
- **`autodoc-pydantic`** → generate the config reference (fields, defaults,
  constraints) from `AppConfig`.
- **`sphinx-click`** (or Typer's util) → generate the CLI reference.
- **OpenAPI**: commit `openapi.json` (dump from `create_app().openapi()`), render
  with `sphinxcontrib-openapi`, or at minimum embed the Swagger UI link + example
  responses generated from the Pydantic models.
- **intersphinx**: add polars & pydantic mappings; cross-link types in prose.
- **`sphinx-design`** already present — use dropdowns/tabs (pip/conda, Python/CLI).
- **MyST** (optional) if we want notebook-style tutorials (`myst-nb`).
- **Theme polish**: furo already good; add `sphinx-copybutton` (present),
  "edit this page", `html_last_updated_fmt`, version switcher on RTD.
- **Doc CI**: build with `-W` (warnings as errors) + `linkcheck` + `doctest` on
  every PR; publish previews.

---

## Part F — Phased execution (with acceptance criteria)

**Phase 1 — Reference correctness (generated, can't drift)**  [S–M]
- Enable doctest; add autodoc-pydantic (config), sphinx-click (CLI), OpenAPI
  render (HTTP). Add intersphinx polars/pydantic. Doc CI: `-W` + doctest +
  linkcheck.
- *Done when*: CLI/HTTP/config pages are generated; `make doctest` passes; build
  is `-W` clean.

**Phase 2 — Docstrings (the core)**  [L]
- Full numpydoc pass in priority order (Part D), with runnable Examples.
- *Done when*: every public symbol has Parameters/Returns/Raises/Examples;
  `interrogate` ≥ 95%; doctest green; the generated API pages are rich.

**Phase 3 — Narrative: tutorials + how-to + concepts**  [L]
- Write the Tutorials (3), How-to guides (8+), and Concept pages (5–6) from
  Part C. Each tutorial runnable end-to-end; each how-to ≤ 1 screen.
- *Done when*: a newcomer can go from install → first dataset → analysis using
  only the tutorials; the IA matches Part C.

**Phase 4 — Polish & gallery**  [M]
- Per-page UI screenshots, an examples gallery/cookbook, value-prop landing with
  a 10-second win, version switcher, "edit on GitHub", last-updated.
- *Done when*: the home page sells the lib; gallery has ≥ 6 runnable recipes.

**Cross-cutting acceptance**: `sphinx-build -W` clean · `make doctest` green ·
`linkcheck` green · `interrogate` ≥ 95% · every page maps to exactly one Diátaxis
mode.

---

## What I'd do first (smallest high-impact slice)

1. Turn on doctest + the three generators (CLI/HTTP/config) so the reference
   stops drifting and examples are tested — **Phase 1**.
2. Rich docstrings for `Client` + `operations` + `domain` (the 80/20 of reads) —
   start of **Phase 2**.
3. One real tutorial ("Your first backfill", with output) — proves the new shape.

These three land a visibly different, trustworthy doc in the first iteration.
```

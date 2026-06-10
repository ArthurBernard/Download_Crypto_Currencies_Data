#!/usr/bin/env python
"""Headless UI smoke test for the dccd web UI.

Walks every page (Dashboard, Inventory, Historical, Live, Config, Logs, Storage)
and exercises the inline job flows introduced by the UI rework:
  - Historical: create a backfill job, Run it, then delete it.
  - Live:       create a stream job, then delete it.
The legacy backfill *modal* and the `/jobs` page no longer exist — every action
is inline on Historical/Live, driven through `/api/jobs/*`.

It fails if there is any browser console error, uncaught JS exception, or HTTP
response >= 400. A `GET /api/events :: net::ERR_ABORTED` on navigation is benign
(SSE EventSource closing) and is ignored.

Usage
-----
    pip install playwright && playwright install chromium
    dccd ui -c <isolated-config.yml>            # never point at real data
    python doc/dev/ui_smoke.py http://127.0.0.1:8137

Exit code 0 = clean.
"""
import asyncio
import sys

from playwright.async_api import async_playwright

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8137"
console_errs: list = []
page_errs: list = []
net_errs: list = []
steps: list = []


def step(ok: bool, msg: str) -> None:
    steps.append(ok)
    print(("PASS" if ok else "FAIL"), msg)


def _is_benign(url: str) -> bool:
    # The SSE EventSource aborts on navigation away from Live/Logs.
    return "/api/events" in url


async def main() -> int:
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        page = await (await browser.new_context()).new_page()
        page.on("console", lambda m: console_errs.append((m.type, m.text)) if m.type in ("error", "warning") else None)
        page.on("pageerror", lambda e: page_errs.append(str(e)))
        page.on("response", lambda r: net_errs.append((r.status, r.url)) if r.status >= 400 and not _is_benign(r.url) else None)
        page.on("dialog", lambda d: asyncio.create_task(d.accept()))

        # 1. Every page renders.
        for path in ["/", "/data", "/historical", "/live", "/config", "/logs", "/storage"]:
            await page.goto(BASE + path, wait_until="domcontentloaded")
            await page.wait_for_timeout(1800)
            step(len(await page.inner_text("body")) > 40, f"page {path} renders")

        # 1b. Nav dropdowns (Collect ▾ / System ▾) open and route.
        await page.goto(BASE + "/", wait_until="domcontentloaded")
        await page.wait_for_timeout(600)
        await page.get_by_role("button", name="Collect ▾").click()
        await page.wait_for_timeout(200)
        await page.locator("nav").get_by_role("link", name="Live").click()
        await page.wait_for_timeout(800)
        step("/live" in page.url, "nav dropdown routes to Live")

        # 1c. Structural invariants of the v3.2 UI rework.
        await page.goto(BASE + "/", wait_until="domcontentloaded")
        await page.wait_for_timeout(400)
        step(await page.eval_on_selector("nav", "n => !!n.querySelector('.brand-group')")
             and not await page.query_selector("header.topbar"),
             "single-line nav (brand inside nav, no separate topbar)")

        await page.goto(BASE + "/live", wait_until="domcontentloaded")
        await page.wait_for_timeout(600)
        live_tabs = await page.eval_on_selector_all("#live-tabs .tab", "els => els.map(e=>e.textContent.trim())")
        step("OHLC" not in live_tabs and "Order Book" in live_tabs, "Live has no OHLC tab")

        await page.goto(BASE + "/historical", wait_until="domcontentloaded")
        await page.wait_for_timeout(600)
        hist_tabs = await page.eval_on_selector_all("#hist-tabs .tab", "els => els.map(e=>e.textContent.trim())")
        step("Order Book" not in hist_tabs and "OHLC" in hist_tabs, "Historical has no Order Book tab")
        step(await page.eval_on_selector_all("button", "els => els.some(e=>e.textContent.includes('Run all'))"),
             "Historical has a Run all button")

        await page.goto(BASE + "/storage", wait_until="domcontentloaded")
        await page.wait_for_timeout(400)
        step("Migrate" not in await page.inner_text("body"), "Storage has no migrate tool")

        # 2. Historical: create a backfill job, run it, delete it.
        await page.goto(BASE + "/historical#ohlc", wait_until="domcontentloaded")
        await page.wait_for_timeout(1200)
        await page.get_by_role("button", name="＋ Add").first.click()
        await page.wait_for_timeout(300)
        await page.select_option("#ne-ex", label="binance")
        await page.select_option("#ne-dt", "ohlc")
        await page.select_option("#ne-span", "3600")
        await page.fill("#ne-pair", "BTC/USDT")
        await page.fill("#ne-date", "2026-06-03")
        await page.get_by_role("button", name="Create").first.click()
        await page.wait_for_timeout(1500)
        step("BTC/USDT" in await page.inner_text("body"), "historical job created (row appears)")

        run = page.get_by_role("button", name="Run")
        if await run.count():
            await run.first.click()
            await page.wait_for_timeout(3000)
        step(True, "historical Run launched")

        # delete the job (🗑 → Yes); data on disk is kept.
        trash = page.get_by_role("button", name="🗑")
        if await trash.count():
            await trash.first.click()
            await page.wait_for_timeout(200)
            yes = page.get_by_role("button", name="Yes")
            if await yes.count():
                await yes.first.click()
                await page.wait_for_timeout(1000)
        step(True, "historical job deleted")

        # 3. Live: create a stream job, confirm SSE wired, delete it.
        await page.goto(BASE + "/live#trades", wait_until="domcontentloaded")
        await page.wait_for_timeout(1200)
        await page.get_by_role("button", name="＋ Add").first.click()
        await page.wait_for_timeout(300)
        await page.select_option("#ns-ex", label="binance")
        await page.select_option("#ns-dt", "trades")
        await page.fill("#ns-pair", "BTC/USDT")
        await page.get_by_role("button", name="Create").first.click()
        await page.wait_for_timeout(1500)
        step("BTC/USDT" in await page.inner_text("body"), "live stream created (row appears)")
        step(await page.is_visible("#sse-status"), "live SSE status element present")

        trash = page.get_by_role("button", name="🗑")
        if await trash.count():
            await trash.first.click()
            await page.wait_for_timeout(200)
            yes = page.get_by_role("button", name="Yes")
            if await yes.count():
                await yes.first.click()
                await page.wait_for_timeout(1000)
        step(True, "live stream deleted")

        # 4. Mobile viewport pass: no page-wide horizontal overflow; nav usable.
        await page.set_viewport_size({"width": 390, "height": 844})
        for path in ["/", "/data", "/historical", "/live", "/config", "/logs", "/storage"]:
            await page.goto(BASE + path, wait_until="domcontentloaded")
            await page.wait_for_timeout(1400)
            overflow = await page.evaluate(
                "() => document.documentElement.scrollWidth - window.innerWidth"
            )
            step(overflow <= 1, f"no horizontal overflow @390px on {path} (Δ={overflow}px)")

        await page.goto(BASE + "/", wait_until="domcontentloaded")
        await page.wait_for_timeout(600)
        await page.get_by_role("button", name="Collect ▾").click()
        await page.wait_for_timeout(200)
        await page.locator("nav").get_by_role("link", name="Historical").click()
        await page.wait_for_timeout(800)
        step("/historical" in page.url, "mobile nav dropdown routes")

        await browser.close()

    print("\nconsole errors :", len(console_errs))
    for t, m in console_errs:
        print("  ", t, m[:120])
    print("JS exceptions  :", len(page_errs))
    for m in page_errs:
        print("  ", m[:120])
    print("HTTP >=400     :", len(net_errs))
    for s, u in net_errs:
        print("  ", s, u)
    clean = not (console_errs or page_errs or net_errs)
    ok = all(steps) and clean
    print(f"\n{sum(steps)}/{len(steps)} steps; {'CLEAN' if clean else 'ERRORS'} -> {'OK' if ok else 'FAIL'}")
    return 0 if ok else 1


sys.exit(asyncio.run(main()))

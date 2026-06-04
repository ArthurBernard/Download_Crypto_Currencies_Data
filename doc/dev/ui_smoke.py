#!/usr/bin/env python
"""Headless UI smoke test for the dccd web UI.

Walks every page and the backfill modal (OHLC / trades-with-cancel / order book),
and fails if there is any browser console error, uncaught JS exception, or HTTP
response >= 400. A `GET /api/events :: net::ERR_ABORTED` on navigation is benign
(SSE EventSource closing) and is ignored.

Usage
-----
    pip install playwright && playwright install chromium
    dccd ui -c <isolated-config.yml>            # never point at real data
    python doc/dev/ui_smoke.py http://127.0.0.1:8137

Exit code 0 = clean. Seed a little data first (a small OHLC backfill) so the
inventory/dashboard have content to render.
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


async def wait_state(page, to=30):
    lbl = ""
    for _ in range(to * 2):
        await page.wait_for_timeout(500)
        if await page.is_visible("#bf-prog-label"):
            lbl = (await page.inner_text("#bf-prog-label")).strip().lower()
        if any(s in lbl for s in ("succeeded", "failed", "cancelled")):
            break
    return lbl


async def main() -> int:
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        page = await (await browser.new_context()).new_page()
        page.on("console", lambda m: console_errs.append((m.type, m.text)) if m.type in ("error", "warning") else None)
        page.on("pageerror", lambda e: page_errs.append(str(e)))
        page.on("response", lambda r: net_errs.append((r.status, r.url)) if r.status >= 400 else None)
        page.on("dialog", lambda d: asyncio.create_task(d.accept()))

        for path in ["/", "/inventory", "/jobs", "/config", "/logs", "/storage"]:
            await page.goto(BASE + path, wait_until="domcontentloaded")
            await page.wait_for_timeout(1800)
            step(len(await page.inner_text("body")) > 40, f"page {path} renders")

        # OHLC backfill via modal
        await page.goto(BASE + "/inventory", wait_until="domcontentloaded")
        await page.wait_for_timeout(1500)
        await page.get_by_role("button", name="Backfill…").first.click()
        await page.wait_for_timeout(300)
        await page.fill("#bf-symbol", "BTC/USDT")
        await page.select_option("#bf-type", "ohlc")
        await page.select_option("#bf-span", "3600")
        await page.select_option("#bf-start", "custom")
        await page.wait_for_timeout(150)
        await page.fill("#bf-date", "2026-06-03")
        await page.click("#bf-launch-btn")
        step("succeeded" in await wait_state(page), "modal OHLC backfill succeeds")
        if await page.is_visible(".modal-close"):
            await page.click(".modal-close")

        # trades backfill + cancel
        await page.get_by_role("button", name="Backfill…").first.click()
        await page.wait_for_timeout(300)
        await page.fill("#bf-symbol", "BTC/USDT")
        await page.select_option("#bf-type", "trades")
        await page.select_option("#bf-start", "custom")
        await page.wait_for_timeout(150)
        await page.fill("#bf-date", "2026-06-03")
        await page.click("#bf-launch-btn")
        await page.wait_for_timeout(2500)
        step(await page.is_visible("#bf-stop-btn"), "Stop button appears while running")
        await page.click("#bf-stop-btn")
        step("cancelled" in await wait_state(page, to=20), "trades backfill cancels via Stop")
        if await page.is_visible(".modal-close"):
            await page.click(".modal-close")

        # jobs: run + start/stop streams
        await page.goto(BASE + "/jobs", wait_until="domcontentloaded")
        await page.wait_for_timeout(1500)
        if await page.get_by_role("button", name="Run now").count():
            await page.get_by_role("button", name="Run now").first.click()
            await page.wait_for_timeout(2000)
        step(True, "jobs Run now")

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

"""Standalone debug runner for the `crawl_url_list` task of made_in_china_dag.

Same flow as `get_all_product_urls` in made_in_china_dag.py (no Airflow / DB deps):
open each listing page, wait for `h2.product-name a`, collect product links.
Every listing page is screenshotted (debug/listing/) and logged with HTTP status +
title, so we can see whether it's blocked (Cloudflare) or the markup changed.

Self-contained: LISTING_URL / PRODUCT_LINK_SELECTOR are copied from made_in_china_dag.py
and the Cloudflare helpers from fix_crawl_companies.py - keep them in sync by hand.

Usage:
    python fix_crawl_url_list.py --listing-limit 3
    python fix_crawl_url_list.py --listing-limit 3 --headless
    python fix_crawl_url_list.py --listing Excavator --max-pages 2
"""
import argparse
import asyncio
import math
import random
import re
from datetime import datetime
from pathlib import Path

import pandas as pd
from playwright.async_api import async_playwright

BASE_DIR = Path(__file__).resolve().parent
DEBUG_DIR = BASE_DIR / "debug" / "listing"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

# --------------------- Copied from made_in_china_dag.py ---------------------
# Each entry is (listing_url_template, max_pages); None -> DEFAULT_MAX_PAGES.
LISTING_URL = [
    ("https://www.made-in-china.com/catalog/item999i132/Tunneling-Machine-{page}.html", 1),
    ("https://www.made-in-china.com/catalog/item999i132/Truck-Crane-{page}.html", 1),
    ("https://www.made-in-china.com/catalog/item999i132/Overhead-Crane-{page}.html", 1),
    ("https://www.made-in-china.com/catalog/item999i132/Tower-Crane-{page}.html", 1),
    ("https://www.made-in-china.com/catalog/item999i132/Gantry-Crane-{page}.html", 1),
    ("https://www.made-in-china.com/catalog/item999i132/Crawler-Crane-{page}.html", 1),
    ("https://www.made-in-china.com/catalog/item999i132/Jib-Crane-{page}.html", 1),
    ("https://www.made-in-china.com/catalog/item999i132/Portal-Crane-{page}.html", 1),
    ("https://www.made-in-china.com/catalog/item999i132/Engineering-Construction-Machinery-Parts-{page}.html", 1),
    ("https://www.made-in-china.com/catalog/item999i132/Excavator-{page}.html", 1),
    ("https://www.made-in-china.com/catalog/item999i132/Loader-{page}.html", 1),
    ("https://www.made-in-china.com/catalog/item999i132/Concrete-Machinery-{page}.html", 1),
    ("https://www.made-in-china.com/catalog/item999i132/Paving-Machinery-{page}.html", 1),
    ("https://www.made-in-china.com/catalog/item999i132/Compactor-{page}.html", 1),
    ("https://www.made-in-china.com/catalog/item999i132/Pile-Driver-{page}.html", 1),
    ("https://www.made-in-china.com/catalog/item999i132/Earth-Moving-Machinery-{page}.html", 1),
    ("https://www.made-in-china.com/catalog/item999i132/Grader-{page}.html", 1),
    ("https://www.made-in-china.com/catalog/item999i132/Lifting-Table-{page}.html", 1),
    ("https://www.made-in-china.com/catalog/item999i132/Chain-Hoist-{page}.html", 1),
    ("https://www.made-in-china.com/catalog/item999i132/Winch-{page}.html", 1),
    ("https://www.made-in-china.com/catalog/item999i132/Lifting-Sling-{page}.html", 1),
    ("https://www.made-in-china.com/catalog/item999i132/Lifting-Jack-{page}.html", 1),
    ("https://www.made-in-china.com/catalog/item999i132/Other-Lifting-Equipment-{page}.html", 1),
    ("https://www.made-in-china.com/catalog/item999i132/Stacker-Reclaimer-{page}.html", 1),
    ("https://www.made-in-china.com/catalog/item999i132/Lifting-Magnet-{page}.html", 1),
    ("https://www.made-in-china.com/catalog/item999i132/Lifting-Tackle-{page}.html", 1),
    ("https://www.made-in-china.com/catalog/item999i132/Drum-Lifter-{page}.html", 1),
    ("https://www.made-in-china.com/catalog/item999i132/Concrete-Mixer-{page}.html", 1),
    ("https://www.made-in-china.com/catalog/item999i132/Mortar-Mixer-{page}.html", 1),
    ("https://www.made-in-china.com/catalog/item999i132/Mixing-Equipment-Components-{page}.html", 1),
    ("https://www.made-in-china.com/catalog/item999i132/Water-Well-Drilling-Rig-{page}.html", 1),
    ("https://www.made-in-china.com/catalog/item999i132/Engineering-Drilling-Rig-{page}.html", 1),
    ("https://www.made-in-china.com/catalog/item999i132/Mine-Drilling-Rig-{page}.html", 1),
    ("https://www.made-in-china.com/catalog/item999i132/Geological-Drilling-Rig-{page}.html", 1),
    ("https://www.made-in-china.com/catalog/item999i132/Mining-Machinery-Parts-{page}.html", 1),
    ("https://www.made-in-china.com/catalog/item999i132/Mineral-Separator-{page}.html", 1),
    ("https://www.made-in-china.com/catalog/item999i132/Mining-Crusher-{page}.html", 1),
    ("https://www.made-in-china.com/catalog/item999i132/Mining-Mill-{page}.html", 1),
    ("https://www.made-in-china.com/catalog/item999i132/Mining-Conveyor-{page}.html", 1),
    ("https://www.made-in-china.com/catalog/item999i132/Sand-Washer-{page}.html", 1),
    ("https://www.made-in-china.com/catalog/item999i132/Monorail-Crane-{page}.html", 1),
    ("https://www.made-in-china.com/catalog/item999i132/Jaw-Crusher-{page}.html", 1),
    ("https://www.made-in-china.com/catalog/item999i132/Hammer-Crusher-{page}.html", 1),
    ("https://www.made-in-china.com/catalog/item999i132/Cone-Crusher-{page}.html", 1),
    ("https://www.made-in-china.com/catalog/item999i132/Impact-Crusher-{page}.html", 1),
    ("https://www.made-in-china.com/catalog/item999i132/Blade-Crusher-{page}.html", 1),
    ("https://www.made-in-china.com/catalog/item999i132/Roller-Crusher-{page}.html", 1),
    ("https://www.made-in-china.com/catalog/item999i132/Metal-Recycling-Machine-{page}.html", 1),
    ("https://www.made-in-china.com/catalog/item999i132/Vibrating-Screen-{page}.html", 1),
    ("https://www.made-in-china.com/catalog/item999i132/Grading-Machine-{page}.html", 1),
    ("https://www.made-in-china.com/catalog/item999i132/Drum-Sieve-{page}.html", 1),
    ("https://www.made-in-china.com/catalog/item999i132/Used-Excavator-{page}.html", 1),
    ("https://www.made-in-china.com/catalog/item999i132/Used-Loader-{page}.html", 1),
    ("https://www.made-in-china.com/catalog/item999i132/Used-Crane-{page}.html", 1),
    ("https://www.made-in-china.com/catalog/item999i132/Used-Bulldozer-{page}.html", 1),
    ("https://www.made-in-china.com/catalog/item999i132/Other-Secondhand-Machinery-{page}.html", 1),
    ("https://www.made-in-china.com/catalog/item999i132/Used-Road-Roller-{page}.html", 1),
    ("https://www.made-in-china.com/catalog/item999i132/Used-Forklift-{page}.html", 1),
    ("https://www.made-in-china.com/catalog/item999i132/Used-Drilling-Rig-{page}.html", 1),
    ("https://www.made-in-china.com/catalog/item999i132/Water-Supply-Equipment-{page}.html", 1),
    ("https://www.made-in-china.com/catalog/item999i132/Screw-Compressor-{page}.html", 1),
    ("https://www.made-in-china.com/catalog/item999i132/Reciprocating-Compressor-{page}.html", 1),
    ("https://www.made-in-china.com/catalog/item999i132/Compressor-Unit-{page}.html", 1),
    ("https://www.made-in-china.com/catalog/item999i132/Scroll-Compressor-{page}.html", 1),
    ("https://www.made-in-china.com/catalog/item999i132/Other-Compressors-{page}.html", 1),
    ("https://www.made-in-china.com/catalog/item999i132/Rotary-Compressor-{page}.html", 1),
    ("https://www.made-in-china.com/catalog/item999i132/Centrifugal-Compressor-{page}.html", 1),
    ("https://www.made-in-china.com/catalog/item999i132/Centrifugal-Pump-{page}.html", 1),
    ("https://www.made-in-china.com/catalog/item999i132/Water-Pump-{page}.html", 1),
    ("https://www.made-in-china.com/catalog/item999i132/Diaphragm-Pump-{page}.html", 1),
    ("https://www.made-in-china.com/catalog/item999i132/Plunger-Pump-{page}.html", 1),
    ("https://www.made-in-china.com/catalog/item999i132/Screw-Pump-{page}.html", 1),
    ("https://www.made-in-china.com/catalog/item999i132/Self-Priming-Pump-{page}.html", 1),
    ("https://www.made-in-china.com/catalog/item999i132/Piston-Pump-{page}.html", 1),
    ("https://www.made-in-china.com/catalog/item999i132/Pressure-Pump-{page}.html", 1),
    ("https://www.made-in-china.com/catalog/item999i132/Jet-Pump-{page}.html", 1),
    ("https://www.made-in-china.com/catalog/item999i132/Peristaltic-Pump-{page}.html", 1),
    ("https://www.made-in-china.com/catalog/item999i132/Conveyor-{page}.html", 1),
    ("https://www.made-in-china.com/catalog/item999i132/Diesel-Generator-{page}.html", 1),
    ("https://www.made-in-china.com/catalog/item999i132/Gasoline-Generator-{page}.html", 1),
    ("https://www.made-in-china.com/catalog/item999i132/Gas-Generator-{page}.html", 1),
    ("https://www.made-in-china.com/catalog/item999i132/Other-Generators-{page}.html", 1),
]

PRODUCT_LINK_SELECTOR = "h2.product-name a"
START_PAGE = 1
DEFAULT_MAX_PAGES = 2

# Candidate selectors logged on every page - if PRODUCT_LINK_SELECTOR is 0 but one of
# these isn't, the listing markup changed rather than the page being blocked.
DEBUG_SELECTORS = [
    "h2.product-name a",
    ".product-name a",
    ".prod-info a",
    ".list-node",
    ".product-info",
    "a[href*='/product/']",
    "a[href*='/prod/']",
    "iframe[src*='challenges.cloudflare.com']",
    "input[name='cf-turnstile-response']",
]

CLOUDFLARE_MAX_CLICKS = 3
CLOUDFLARE_SOLVE_TIMEOUT = 20000


# --------------------- Utility Functions ---------------------
def clean_text(value):
    if value is None:
        return None
    return re.sub(r"\s+", " ", str(value)).strip()


def safe_name(url: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", url.split("//")[-1])[:120]


async def human_click(page, target_x, target_y):
    """Move the mouse along a randomized human-like curved path and click
    (async port of solve_altcha.py)."""
    viewport = page.viewport_size or {"width": 1280, "height": 720}
    start_x = random.uniform(0, viewport["width"])
    start_y = random.uniform(0, viewport["height"])
    await page.mouse.move(start_x, start_y)
    await page.wait_for_timeout(random.randint(150, 400))  # time to "notice" the page
    await page.mouse.move(start_x + random.uniform(-30, 30), start_y + random.uniform(-20, 20))
    await page.wait_for_timeout(random.randint(100, 300))  # idle wiggle before committing

    # randomized cubic-Bezier path with an ease-in-out speed profile and fading jitter
    distance = math.hypot(target_x - start_x, target_y - start_y)
    steps = max(15, min(40, int(distance / 8)))
    bow = random.uniform(0.15, 0.35) * distance * random.choice((-1, 1))
    mid_x = (start_x + target_x) / 2 + random.uniform(-bow, bow)
    mid_y = (start_y + target_y) / 2 + random.uniform(-bow, bow)
    c1x = start_x + (mid_x - start_x) * random.uniform(0.3, 0.6)
    c1y = start_y + (mid_y - start_y) * random.uniform(0.3, 0.6)
    c2x = target_x + (mid_x - target_x) * random.uniform(0.3, 0.6)
    c2y = target_y + (mid_y - target_y) * random.uniform(0.3, 0.6)

    for i in range(1, steps + 1):
        t = i / steps
        e = 4 * t ** 3 if t < 0.5 else 1 - (-2 * t + 2) ** 3 / 2
        x = (1 - e) ** 3 * start_x + 3 * (1 - e) ** 2 * e * c1x + 3 * (1 - e) * e ** 2 * c2x + e ** 3 * target_x
        y = (1 - e) ** 3 * start_y + 3 * (1 - e) ** 2 * e * c1y + 3 * (1 - e) * e ** 2 * c2y + e ** 3 * target_y
        jitter = 1.5 * (1 - t)
        await page.mouse.move(x + random.uniform(-jitter, jitter), y + random.uniform(-jitter, jitter))
        await page.wait_for_timeout(random.randint(4, 16))
    await page.mouse.move(target_x, target_y)

    await page.wait_for_timeout(random.randint(80, 220))  # brief hover before pressing
    await page.mouse.down()
    await page.wait_for_timeout(random.randint(40, 130))  # press duration
    await page.mouse.up()


async def find_cloudflare_checkbox(page):
    """Return (x, y) of the "Verify you are human" checkbox, or None.

    The checkbox lives in a cross-origin challenges.cloudflare.com iframe inside a
    closed shadow root, so it can't be located directly - use the iframe's box
    instead (checkbox sits ~22px from the widget's left edge, vertically centered)."""
    for frame in page.frames:
        if "challenges.cloudflare.com" not in frame.url:
            continue
        try:
            element = await frame.frame_element()
            box = await element.bounding_box()
        except Exception:
            continue
        if box and box["width"] > 50 and box["height"] > 20:
            print(f"[CLOUDFLARE] Widget box from iframe: {box}")
            return (
                box["x"] + 22 + random.uniform(-4, 4),
                box["y"] + box["height"] / 2 + random.uniform(-4, 4),
            )

    # Fallback: the shadow-host div sits next to the hidden cf-turnstile-response
    # input (random ids per page), so their common parent has the widget's box.
    try:
        container = page.locator("input[name='cf-turnstile-response']").first.locator("xpath=..")
        box = await container.bounding_box()
    except Exception:
        box = None
    if box and box["width"] > 50 and box["height"] > 20:
        print(f"[CLOUDFLARE] Widget box from cf-turnstile-response container: {box}")
        return (
            box["x"] + 22 + random.uniform(-4, 4),
            box["y"] + box["height"] / 2 + random.uniform(-4, 4),
        )
    return None


async def page_screenshot(page, name, full_page=False):
    try:
        DEBUG_DIR.mkdir(parents=True, exist_ok=True)
        path = DEBUG_DIR / f"{name}.png"
        await page.screenshot(path=str(path), full_page=full_page)
        print(f"  [DEBUG] Screenshot {path.name}: url={page.url}, title={await page.title()!r}")
    except Exception as e:
        print(f"  [DEBUG] Could not take screenshot {name}: {e}")


async def dump_html(page, name):
    try:
        DEBUG_DIR.mkdir(parents=True, exist_ok=True)
        path = DEBUG_DIR / f"{name}.html"
        path.write_text(await page.content(), encoding="utf-8")
        print(f"  [DEBUG] Saved {path.name}")
    except Exception as e:
        print(f"  [DEBUG] Could not save HTML {name}: {e}")


async def log_selector_presence(page):
    counts = []
    for sel in DEBUG_SELECTORS:
        try:
            n = await page.locator(sel).count()
        except Exception:
            n = -1
        counts.append(f"{sel}={n}")
    print("  [DEBUG] selectors: " + ", ".join(counts))


async def is_cloudflare_challenge(page):
    try:
        title = await page.title()
    except Exception:
        return False
    return "just a moment" in title.lower() or await page.locator(
        "input[name='cf-turnstile-response']"
    ).count() > 0


async def solve_cloudflare_challenge(page, wait_selector, shot_prefix):
    """Click the Cloudflare checkbox like a human until `wait_selector` shows up."""
    for attempt in range(1, CLOUDFLARE_MAX_CLICKS + 1):
        position = None
        for _ in range(10):
            position = await find_cloudflare_checkbox(page)
            if position:
                break
            await page.wait_for_timeout(1000)
        if not position:
            print(f"  [CLOUDFLARE] Attempt {attempt}: checkbox not found")
            await page_screenshot(page, f"{shot_prefix}_cf_no_checkbox_{attempt}")
            return False

        print(f"  [CLOUDFLARE] Attempt {attempt}: clicking checkbox at ({position[0]:.0f}, {position[1]:.0f})")
        await human_click(page, *position)
        await page.wait_for_timeout(2000)
        await page_screenshot(page, f"{shot_prefix}_cf_after_click_{attempt}")

        try:
            await page.wait_for_selector(wait_selector, timeout=CLOUDFLARE_SOLVE_TIMEOUT)
            print(f"  [CLOUDFLARE] Challenge passed after {attempt} click(s)")
            return True
        except Exception:
            print(f"  [CLOUDFLARE] Attempt {attempt}: still blocked, title={await page.title()!r}")
    return False


async def get_all_product_urls(listings, link_selector, start_page, default_max_pages,
                               headless=True, solve_cloudflare=True):
    seen = set()
    rows = []          # one row per product link
    page_reports = []  # one row per listing page

    async with async_playwright() as p:
        print(f"[PLAYWRIGHT] Launching Chromium for listing pages (headless={headless})")
        browser = await p.chromium.launch(headless=headless, args=["--no-sandbox"])

        for listing_idx, (listing_template, listing_max_pages) in enumerate(listings, 1):
            max_pages = listing_max_pages if listing_max_pages is not None else default_max_pages
            print(f"=== Listing {listing_idx}/{len(listings)}: {listing_template} (max_pages={max_pages}) ===")
            page = await browser.new_page(user_agent=USER_AGENT)
            page_num = start_page

            while page_num < start_page + max_pages:
                url = listing_template.format(page=page_num)
                shot = f"{listing_idx:03d}_{safe_name(url)}"
                report = {"listing": listing_template, "page": page_num, "url": url,
                          "status": None, "title": "", "final_url": "", "links": 0,
                          "cloudflare": False, "mic_captcha": False, "error": ""}
                print(f"[page {page_num}] {url}")

                try:
                    response = await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                    report["status"] = response.status if response else None
                    report["title"] = await page.title()
                    report["final_url"] = page.url
                    print(f"[page {page_num}] loaded: status={report['status']}, "
                          f"url={page.url}, title={report['title']!r}")

                    # made-in-china's own anti-bot: redirect to a slider-puzzle page
                    # ("Focus Captcha"), unrelated to Cloudflare.
                    if "captcha.made-in-china.com" in page.url:
                        report["mic_captcha"] = True
                        raise RuntimeError("redirected to made-in-china slider captcha (Focus Captcha)")

                    try:
                        await page.wait_for_selector(link_selector, timeout=15000)
                    except Exception:
                        if not await is_cloudflare_challenge(page):
                            raise
                        report["cloudflare"] = True
                        print(f"[page {page_num}] Cloudflare challenge detected")
                        await page_screenshot(page, f"{shot}_cloudflare")
                        if not solve_cloudflare or not await solve_cloudflare_challenge(page, link_selector, shot):
                            raise
                except Exception as exc:
                    report["error"] = clean_text(str(exc))[:500]
                    print(f"[page {page_num}] stopped: {exc}")
                    await log_selector_presence(page)
                    await page_screenshot(page, f"{shot}_error", full_page=True)
                    await dump_html(page, f"{shot}_error")
                    page_reports.append(report)
                    break

                await page_screenshot(page, f"{shot}_ok")
                links = await page.eval_on_selector_all(link_selector, "els => els.map(el => el.href)")
                report["links"] = len(links)
                page_reports.append(report)

                if not links:
                    print(f"[page {page_num}] no links found, stopping")
                    await log_selector_presence(page)
                    break

                new_links = [href for href in links if href not in seen]
                seen.update(new_links)
                rows.extend({"url": href, "listing": listing_template, "page": page_num} for href in new_links)
                print(f"[page {page_num}] found {len(new_links)} new links (total {len(seen)})")

                page_num += 1
                await asyncio.sleep(1)

            try:
                await page.close()
            except Exception:
                pass

        await browser.close()
        print("[PLAYWRIGHT] Listing browser closed")

    return rows, page_reports


def main():
    parser = argparse.ArgumentParser(description="Debug crawl_url_list of made_in_china DAG")
    parser.add_argument("--listing-limit", type=int, default=None,
                        help="Only crawl the first N listings of LISTING_URL")
    parser.add_argument("--listing", default=None,
                        help="Only crawl listings whose URL contains this text (e.g. Excavator)")
    parser.add_argument("--max-pages", type=int, default=None,
                        help="Override max pages per listing (default: value in LISTING_URL)")
    parser.add_argument("--output", default=None,
                        help="Output CSV of product URLs (default: url_list_output_<timestamp>.csv)")
    parser.add_argument("--headless", action="store_true", help="Run without browser window (default: visible)")
    parser.add_argument("--no-cloudflare-solve", action="store_true",
                        help="Don't try clicking the Cloudflare checkbox, just report it")
    args = parser.parse_args()

    listings = LISTING_URL
    if args.listing:
        listings = [item for item in listings if args.listing.lower() in item[0].lower()]
    if args.listing_limit:
        listings = listings[: args.listing_limit]
    if args.max_pages:
        listings = [(template, args.max_pages) for template, _ in listings]
    print(f"[INPUT] {len(listings)} listings, selector={PRODUCT_LINK_SELECTOR!r}")

    output_path = Path(args.output) if args.output else (
        BASE_DIR / f"url_list_output_{datetime.now():%Y%m%d_%H%M%S}.csv"
    )
    report_path = output_path.with_name(output_path.stem + "_pages.csv")

    rows, page_reports = asyncio.run(
        get_all_product_urls(
            listings, PRODUCT_LINK_SELECTOR, START_PAGE, DEFAULT_MAX_PAGES,
            headless=args.headless, solve_cloudflare=not args.no_cloudflare_solve,
        )
    )

    pd.DataFrame(rows, columns=["url", "listing", "page"]).to_csv(
        output_path, index=False, encoding="utf-8-sig", lineterminator="\n"
    )
    df_report = pd.DataFrame(page_reports)
    df_report.to_csv(report_path, index=False, encoding="utf-8-sig", lineterminator="\n")

    failed = df_report[df_report["error"] != ""] if not df_report.empty else df_report
    print(f"[DONE] product urls={len(rows)}, pages={len(page_reports)}, failed pages={len(failed)}")
    print(f"[DONE] output={output_path}, page report={report_path}")
    for _, r in failed.iterrows():
        print(f"  - {r['url']} status={r['status']} cloudflare={r['cloudflare']} mic_captcha={r['mic_captcha']} title={r['title']!r}")


if __name__ == "__main__":
    main()

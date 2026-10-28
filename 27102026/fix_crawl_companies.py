"""Standalone debug runner for the `crawl_companies_data` task of made_in_china_dag.

Logic is copied from made_in_china_dag.py (no Airflow / S3 / Postgres deps) so it
can be run locally with a visible browser to check whether the site markup changed.

Usage:
    python fix_crawl_companies.py
    python fix_crawl_companies.py --input company.csv --output company_output.csv --limit 5
    python fix_crawl_companies.py --headless          # run without browser window
    python fix_crawl_companies.py --pause-on-error    # keep browser open on failure for inspecting
"""
import argparse
import asyncio
import math
import os
import random
import re
from datetime import datetime
from pathlib import Path

import pandas as pd
from playwright.async_api import async_playwright

BASE_DIR = Path(__file__).resolve().parent

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

# Made-in-China login required to view supplier contact info.
MIC_LOGIN_URL = "https://login.made-in-china.com/sign-in/"
MIC_EMAIL = os.environ.get("MIC_EMAIL", "hnghiapy.work@gmail.com")
MIC_PASSWORD = os.environ.get("MIC_PASSWORD", "hnghia002")

COMPANY_DETAIL_TIMEOUT = 90
# Cloudflare "Verify you are human" checkbox on the login page: how many times to
# click it, and how long (ms) to wait for the login form after each click.
CLOUDFLARE_MAX_CLICKS = 3
CLOUDFLARE_SOLVE_TIMEOUT = 20000
COMPANY_PAGE_RECYCLE_EVERY = 50

OUTPUT_COLUMNS = [
    "company_url", "company_name", "address", "phone", "mobile", "website",
    "contact_person", "status", "error",
]

# Selectors the DAG depends on - logged per company so a markup change shows up immediately.
DEBUG_SELECTORS = [
    "h1",
    ".contact-unlogin",
    ".pad-header-unlogin",
    ".info-item",
    ".info-item .info-label",
    ".info-item .info-fields",
    ".info-cont-wp .item",
    ".info-cont-wp .item .label",
    ".info-cont-wp .item .info",
    "a.link-web",
    ".contact-customer-info .info-name",
    ".person .txt .name",
]

PAUSE_ON_ERROR = False
DEBUG_DIR = BASE_DIR / "debug"


# --------------------- Utility Functions ---------------------
def clean_text(value):
    if value is None:
        return None
    return re.sub(r"\s+", " ", str(value)).strip()


def load_company_urls(input_path: Path, limit=None) -> pd.DataFrame:
    df = pd.read_csv(input_path, dtype=str)
    # Accept either a `company_url` column or just take the first column.
    col = "company_url" if "company_url" in df.columns else df.columns[0]
    urls = (
        df[col].dropna().astype(str).str.strip().str.rstrip("/")
    )
    urls = urls[urls != ""].drop_duplicates()
    if limit:
        urls = urls.head(limit)
    print(f"[INPUT] Loaded {len(urls)} company URLs from {input_path} (column={col})")
    return pd.DataFrame({"company_url": urls.tolist()})


def append_row_to_csv(row: dict, output_path: Path):
    # Write each company right away so partial results survive a crash/Ctrl+C.
    df = pd.DataFrame([row], columns=OUTPUT_COLUMNS)
    write_header = not output_path.exists()
    df.to_csv(
        output_path,
        mode="a",
        header=write_header,
        index=False,
        encoding="utf-8-sig" if write_header else "utf-8",
        lineterminator="\n",
    )


def safe_name(url: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", url.split("//")[-1])[:120]


async def dump_debug(page, company_url, reason):
    """Save screenshot + HTML so the page structure can be inspected afterwards."""
    try:
        DEBUG_DIR.mkdir(parents=True, exist_ok=True)
        stem = DEBUG_DIR / f"{safe_name(company_url)}_{reason}"
        await page.screenshot(path=f"{stem}.png", full_page=True)
        Path(f"{stem}.html").write_text(await page.content(), encoding="utf-8")
        print(f"  [DEBUG] Saved {stem}.png / .html (url={page.url}, title={await page.title()!r})")
    except Exception as e:
        print(f"  [DEBUG] Could not dump debug files: {e}")


async def log_selector_presence(page):
    counts = []
    for sel in DEBUG_SELECTORS:
        try:
            n = await page.locator(sel).count()
        except Exception:
            n = -1
        counts.append(f"{sel}={n}")
    print("  [DEBUG] selectors: " + ", ".join(counts))


async def maybe_pause(page):
    if PAUSE_ON_ERROR:
        print("  [DEBUG] Paused - inspect the browser, then press Resume in Playwright Inspector")
        await page.pause()


# --------------------- Company Detail Crawl (from DAG) ---------------------
LOGIN_SHOT_DIR = DEBUG_DIR / "login"
_login_shot_index = 0


async def login_screenshot(page, step):
    """Screenshot every login step (saved as debug/login/NN_<step>.png) to see the actual UI."""
    global _login_shot_index
    _login_shot_index += 1
    try:
        LOGIN_SHOT_DIR.mkdir(parents=True, exist_ok=True)
        path = LOGIN_SHOT_DIR / f"{_login_shot_index:02d}_{step}.png"
        await page.screenshot(path=str(path), full_page=True)
        print(f"[LOGIN] Screenshot {path.name}: url={page.url}, title={await page.title()!r}")
    except Exception as e:
        print(f"[LOGIN] Could not take screenshot for step {step}: {e}")


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


async def solve_cloudflare_challenge(page, timeout=CLOUDFLARE_SOLVE_TIMEOUT):
    """Click the Cloudflare checkbox like a human until the login form shows up.
    Returns True when the challenge page is gone."""
    username_input = page.locator("input[autocomplete='username']")
    for attempt in range(1, CLOUDFLARE_MAX_CLICKS + 1):
        # The widget iframe takes a few seconds to render after the page loads.
        position = None
        for _ in range(10):
            position = await find_cloudflare_checkbox(page)
            if position:
                break
            await page.wait_for_timeout(1000)
        if not position:
            print(f"[CLOUDFLARE] Attempt {attempt}: checkbox iframe not found")
            await login_screenshot(page, f"cf_no_checkbox_{attempt}")
            return False

        print(f"[CLOUDFLARE] Attempt {attempt}: clicking checkbox at ({position[0]:.0f}, {position[1]:.0f})")
        await human_click(page, *position)
        await page.wait_for_timeout(2000)
        await login_screenshot(page, f"cf_after_click_{attempt}")

        try:
            await username_input.wait_for(state="visible", timeout=timeout)
            print(f"[CLOUDFLARE] Challenge passed after {attempt} click(s), title={await page.title()!r}")
            return True
        except Exception:
            print(f"[CLOUDFLARE] Attempt {attempt}: still blocked, title={await page.title()!r}")
            await login_screenshot(page, f"cf_still_blocked_{attempt}")
    return False


async def login_to_mic(page, email, password):
    print(f"[LOGIN] Opening login page: {MIC_LOGIN_URL}")
    response = await page.goto(MIC_LOGIN_URL, wait_until="domcontentloaded", timeout=30000)
    print(f"[LOGIN] Login page loaded: status={response.status if response else None}")
    await login_screenshot(page, "page_loaded")

    username_input = page.locator("input[autocomplete='username']")
    try:
        await username_input.wait_for(state="visible", timeout=15000)
        print(f"[LOGIN] Login form visible, title={await page.title()!r}")
    except Exception:
        # Cloudflare "Just a moment..." page with an interactive "Verify you are human"
        # checkbox (it never auto-passes, see debug/login/*waiting*.png).
        print(f"[LOGIN] Login form not visible, title={await page.title()!r} - trying Cloudflare checkbox")
        await login_screenshot(page, "cloudflare_challenge")
        if not await solve_cloudflare_challenge(page):
            await login_screenshot(page, "username_not_visible")
            raise
    # Same readonly-until-focused pattern as the password field below - click first for reliability.
    await username_input.click()
    try:
        await username_input.fill(email)
    except Exception:
        await username_input.type(email, delay=50)
    await login_screenshot(page, "email_filled")
    await page.click("button[ads-data='st:417']")
    print("[LOGIN] Clicked Continue")

    password_input = page.locator("input[name='password']")
    try:
        await password_input.wait_for(state="visible", timeout=15000)
    except Exception:
        await login_screenshot(page, "password_not_visible")
        raise
    await login_screenshot(page, "password_step")

    # The password field renders with readonly="" and only becomes editable
    # after a real focus/click event (anti-bot measure to block scripted
    # value-setting) - a plain click() removes it, then fill() works.
    await password_input.click()
    try:
        await page.wait_for_selector("input[name='password']:not([readonly])", timeout=10000)
    except Exception:
        print("[LOGIN] Password input still readonly after click")

    try:
        await password_input.fill(password)
    except Exception:
        # Fallback if fill() still gets blocked: simulate real keystrokes instead.
        await password_input.type(password, delay=50)
    await login_screenshot(page, "password_filled")

    await page.click("button[ads-data='st:58']")
    print("[LOGIN] Clicked Sign in")

    # login2026 page may render a Cloudflare Turnstile widget / numeric captcha
    # after clicking "Sign in" - log it so we know why the redirect never happens.
    await page.wait_for_timeout(3000)
    await login_screenshot(page, "after_sign_in")
    if await page.locator(".turnstile-widget, iframe[src*='challenges.cloudflare.com']").count():
        print("[LOGIN] Cloudflare Turnstile captcha is shown on login page")
        await dump_debug(page, "login", "turnstile")
        await maybe_pause(page)
    error_el = page.locator(".form-error")
    if await error_el.count():
        print(f"[LOGIN] Form error: {clean_text(await error_el.first.inner_text())}")

    try:
        await page.wait_for_url(lambda url: "login.made-in-china.com" not in url, timeout=30000)
    except Exception:
        print(f"[LOGIN] Still on login page after 30s: {page.url}")
    await page.wait_for_timeout(2000)
    await login_screenshot(page, "login_finished")


async def is_logged_in_mic(page):
    # `.pad-header-unlogin` does not exist on the login page, so the DAG's check
    # returns True even when login silently failed - also reject the login domain.
    if "login.made-in-china.com" in page.url:
        return False
    return await page.query_selector(".pad-header-unlogin") is None


async def ensure_logged_in_mic(page, email, password):
    try:
        await page.goto("https://www.made-in-china.com/", wait_until="domcontentloaded", timeout=30000)
        if await is_logged_in_mic(page):
            return
    except Exception as e:
        print(f"[CRAWL_COMPANY] Failed to check login state after recycle: {e}")

    print("[CRAWL_COMPANY] Session lost after page recycle, logging in again")
    await login_to_mic(page, email, password)
    if not await is_logged_in_mic(page):
        print("[CRAWL_COMPANY] Re-login after recycle failed")


async def extract_company_info_pairs(page):
    items = await page.eval_on_selector_all(
        ".info-item",
        """els => els.map(el => ({
            label: el.querySelector('.info-label') ? el.querySelector('.info-label').innerText.trim() : '',
            value: el.querySelector('.info-fields') ? el.querySelector('.info-fields').innerText.trim() : ''
        }))""",
    )
    pairs = [item for item in items if item["label"] and item["value"]]
    if pairs:
        return pairs

    # 2nd template: .info-cont-wp .item wraps a .label/.info pair instead
    items = await page.eval_on_selector_all(
        ".info-cont-wp .item",
        """els => els.map(el => ({
            label: el.querySelector('.label') ? el.querySelector('.label').innerText.trim() : '',
            value: el.querySelector('.info') ? el.querySelector('.info').innerText.trim() : ''
        }))""",
    )
    return [item for item in items if item["label"] and item["value"]]


def pick_by_label(pairs, *keywords, longest=False):
    matches = [p["value"] for p in pairs if any(k in p["label"].lower() for k in keywords)]
    if not matches:
        return ""
    return max(matches, key=len) if longest else matches[0]


def clean_phone(value):
    value = re.sub(r"\s+", " ", value).strip()
    match = re.match(r"[+\d][\d\s\-()]*\d", value)
    return match.group(0).strip() if match else value


async def resolve_company_website(page):
    handle = await page.query_selector("a.link-web")
    if not handle:
        return ""
    href = await handle.get_attribute("href") or ""
    if not href:
        return ""
    if href.startswith("//"):
        href = "https:" + href
    try:
        resp = await page.request.get(href, timeout=15000)
        return resp.url
    except Exception:
        return href


async def extract_contact_person_mic(page, pairs):
    value = pick_by_label(pairs, "contact person")
    if value:
        return value
    el = await page.query_selector(".contact-customer-info .info-name")
    if el:
        return (await el.inner_text()).strip()

    # 2nd template: contact name sits in the "person" card next to the avatar
    el = await page.query_selector(".person .txt .name")
    return (await el.inner_text()).strip() if el else ""


async def crawl_company_detail(page, company_url):
    row = {"company_url": company_url}

    contact_url = company_url + "/contact-info.html"
    await page.goto(contact_url, wait_until="domcontentloaded", timeout=30000)
    print(f"  [DEBUG] landed on: {page.url}")
    await page.wait_for_selector("h1", state="attached", timeout=15000)

    if await page.query_selector(".contact-unlogin"):
        print(f"  warning: still showing logged-out contact info for {company_url}")

    await log_selector_presence(page)

    h1 = await page.query_selector("h1")
    row["company_name"] = clean_text(await h1.inner_text()) if h1 else ""

    pairs = await extract_company_info_pairs(page)
    print(f"  [DEBUG] info pairs ({len(pairs)}): {[p['label'] for p in pairs]}")
    row["address"] = pick_by_label(pairs, "address", longest=True)
    row["phone"] = clean_phone(pick_by_label(pairs, "telephone", "tel:"))
    row["mobile"] = clean_phone(pick_by_label(pairs, "mobile"))
    row["website"] = pick_by_label(pairs, "website", "homepage") or await resolve_company_website(page)
    row["contact_person"] = await extract_contact_person_mic(page, pairs)

    return row


async def crawl_pending_company_details(df_companies: pd.DataFrame, email, password,
                                        output_path: Path, headless=False, slow_mo=0) -> tuple:
    print(f"[CRAWL_COMPANY] Start crawl_pending_company_details: rows={len(df_companies)}")
    details = []
    success_urls = []
    failed_urls = []

    if df_companies.empty:
        print("[CRAWL_COMPANY] No pending company URLs to crawl")
        return details, success_urls, failed_urls

    async with async_playwright() as p:
        print(f"[PLAYWRIGHT] Launching Chromium (headless={headless}, slow_mo={slow_mo})")
        browser = await p.chromium.launch(headless=headless, slow_mo=slow_mo)
        context = await browser.new_context(user_agent=USER_AGENT)
        page = await context.new_page()

        print("[CRAWL_COMPANY] Logging in to made-in-china.com")
        try:
            await login_to_mic(page, email, password)
        except Exception as e:
            print(f"[CRAWL_COMPANY] Login step raised: {e}")
            await dump_debug(page, "login", "login_error")
            await maybe_pause(page)
        if not await is_logged_in_mic(page):
            print("[CRAWL_COMPANY] Login failed, aborting company crawl")
            await dump_debug(page, "login", "login_failed")
            await maybe_pause(page)
            await browser.close()
            failed_urls = df_companies["company_url"].tolist()
            return details, success_urls, failed_urls
        print("[CRAWL_COMPANY] Logged in")

        for idx, (_, row) in enumerate(df_companies.iterrows(), 1):
            company_url = row["company_url"]
            print(f"[CRAWL_COMPANY] Crawling company {idx}/{len(df_companies)}: url={company_url}")

            try:
                data = await asyncio.wait_for(
                    crawl_company_detail(page, company_url), timeout=COMPANY_DETAIL_TIMEOUT
                )
                empty_fields = [k for k in ("company_name", "address", "phone", "mobile", "website", "contact_person")
                                if not data.get(k)]
                data["status"] = "success"
                data["error"] = f"empty: {', '.join(empty_fields)}" if empty_fields else ""
                details.append(data)
                success_urls.append(company_url)
                print(f"[CRAWL_COMPANY] Company data collected: {data}")
                # Name/address missing usually means the selectors no longer match.
                if not data.get("company_name") or not data.get("address"):
                    await dump_debug(page, company_url, "missing_fields")
                append_row_to_csv(data, output_path)
            except asyncio.TimeoutError:
                failed_urls.append(company_url)
                print(f"[CRAWL_COMPANY] Company crawl timed out after {COMPANY_DETAIL_TIMEOUT}s: {company_url}")
                await dump_debug(page, company_url, "timeout")
                append_row_to_csv(
                    {"company_url": company_url, "status": "failed",
                     "error": f"timeout after {COMPANY_DETAIL_TIMEOUT}s"},
                    output_path,
                )
                await maybe_pause(page)
                try:
                    await page.close()
                except Exception:
                    pass
                page = await context.new_page()
                await ensure_logged_in_mic(page, email, password)
            except Exception as e:
                failed_urls.append(company_url)
                print(f"[CRAWL_COMPANY] Company crawl failed: {company_url}, error={e}")
                await log_selector_presence(page)
                await dump_debug(page, company_url, "error")
                append_row_to_csv(
                    {"company_url": company_url, "status": "failed", "error": clean_text(e)[:500]},
                    output_path,
                )
                await maybe_pause(page)

            if idx % COMPANY_PAGE_RECYCLE_EVERY == 0 and idx < len(df_companies):
                print(f"[CRAWL_COMPANY] Recycling browser tab after {idx} companies")
                try:
                    await page.close()
                except Exception:
                    pass
                page = await context.new_page()
                await ensure_logged_in_mic(page, email, password)

        await browser.close()
        print("[PLAYWRIGHT] Company detail browser closed")

    print("[CRAWL_COMPANY] Completed company detail task: "
          f"success={len(success_urls)}, failed={len(failed_urls)}")
    return details, success_urls, failed_urls


def main():
    global PAUSE_ON_ERROR

    parser = argparse.ArgumentParser(description="Debug crawl_companies_data of made_in_china DAG")
    parser.add_argument("--input", default=str(BASE_DIR / "company.csv"),
                        help="CSV with company URLs (column `company_url` or first column)")
    parser.add_argument("--output", default=None,
                        help="Output CSV (default: company_output_<timestamp>.csv next to this script)")
    parser.add_argument("--limit", type=int, default=None, help="Only crawl the first N companies")
    parser.add_argument("--headless", action="store_true", help="Run without browser window (default: visible)")
    parser.add_argument("--slow-mo", type=int, default=0, help="Slow down each Playwright action by N ms")
    parser.add_argument("--pause-on-error", action="store_true",
                        help="Open Playwright Inspector and pause when a company fails")
    args = parser.parse_args()

    PAUSE_ON_ERROR = args.pause_on_error
    output_path = Path(args.output) if args.output else (
        BASE_DIR / f"company_output_{datetime.now():%Y%m%d_%H%M%S}.csv"
    )

    df_companies = load_company_urls(Path(args.input), args.limit)
    details, success_urls, failed_urls = asyncio.run(
        crawl_pending_company_details(
            df_companies, MIC_EMAIL, MIC_PASSWORD, output_path,
            headless=args.headless, slow_mo=args.slow_mo,
        )
    )

    print(f"[DONE] success={len(success_urls)}, failed={len(failed_urls)}, output={output_path}")
    if failed_urls:
        print("[DONE] Failed URLs:")
        for url in failed_urls:
            print(f"  - {url}")


if __name__ == "__main__":
    main()

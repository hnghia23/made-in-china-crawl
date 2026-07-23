"""Crawl made-in-china product detail pages (from links.csv) and write details to a CSV file."""

import json
import os
import sys
import time

import pandas as pd
from playwright.sync_api import sync_playwright

sys.stdout.reconfigure(encoding="utf-8", errors="replace")
sys.stderr.reconfigure(encoding="utf-8", errors="replace")

INPUT_FILE = "link.csv"
OUTPUT_FILE = "output/product.csv"
RETRY_FILE = "retry_links.csv"
DELAY = 2.0  # seconds to wait between product pages
LIMIT = 0  # only crawl the first N urls (0 = all)
HEADLESS = True

FIELDNAMES = [
    "url",
    "category",
    "brand",
    "model",
    "year",
    "hours",
    "country",
    "price",
    "currency",
    "images",
    "company_name",
    "company_url",
    "vendor_name"
]


def extract_product_json(page):
    scripts = page.eval_on_selector_all(
        "script[type='application/ld+json']", "els => els.map(el => el.textContent)"
    )
    for raw in scripts:
        raw = raw.strip()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if isinstance(data, dict) and data.get("@type") == "Product":
            return data
    return {}


def extract_category(page):
    # scoped to the breadcrumb bar, since some templates reuse the same
    # sr-QPWords-cont/sr-QPWords-item classes for an unrelated "hot searches" widget
    texts = page.eval_on_selector_all(
        ".detail-top-bar .sr-QPWords-cont li.sr-QPWords-item a", "els => els.map(el => el.innerText.trim())"
    )
    return texts[-1] if texts else ""


def extract_pairs_by_next_sibling(page, label_selector):
    pairs = page.eval_on_selector_all(
        label_selector,
        """els => els.map(el => ({
            label: el.innerText.trim(),
            value: el.nextElementSibling ? el.nextElementSibling.innerText.trim() : ''
        }))""",
    )
    return {pair["label"]: pair["value"] for pair in pairs if pair["label"]}


def extract_basic_info(page):
    # old template: label/value wrapped together in one div
    items = page.eval_on_selector_all(
        ".basic-info-list .bsc-item",
        """els => els.map(el => ({
            label: el.querySelector('.bac-item-label') ? el.querySelector('.bac-item-label').innerText.trim() : '',
            value: el.querySelector('.bac-item-value') ? el.querySelector('.bac-item-value').innerText.trim() : ''
        }))""",
    )
    info = {item["label"]: item["value"] for item in items if item["label"]}
    if info:
        return info

    # new template: <dt class="product-attrs-label">/<dd class="product-attrs-value"> siblings
    return extract_pairs_by_next_sibling(page, "dt.product-attrs-label")


def extract_gallery_images(page):
    raw_srcs = page.eval_on_selector_all(
        ".sr-proMainInfo-slide-pic.J-pic-list-wrap .J-pic-large-item[fsrc]",
        "els => els.map(el => el.getAttribute('fsrc'))",
    )
    seen = set()
    images = []
    for src in raw_srcs:
        if not src:
            continue
        if src.startswith("//"):
            src = "https:" + src
        if src not in seen:
            seen.add(src)
            images.append(src)
    return images


def extract_company(page):
    handle = page.query_selector(".title-txt a")
    if not handle:
        return "", ""
    name_el = handle.query_selector(".text-ellipsis")
    name = (name_el.inner_text() if name_el else handle.inner_text()).strip()
    url = handle.get_attribute("href") or ""
    return name, url


def extract_vendor_name(page):
    # old template: sidebar contact box; new template: bottom inquiry form recipient
    try:
        page.wait_for_selector(
            ".sr-side-contSupplier-name, .sr-sendMsg-name", state="attached", timeout=15000
        )
    except Exception:
        pass
    el = page.query_selector(".sr-side-contSupplier-name") or page.query_selector(".sr-sendMsg-name")
    return el.inner_text().strip() if el else ""


def crawl_product(page, url):
    row = {field: "" for field in FIELDNAMES}
    row["url"] = url

    page.goto(url, wait_until="domcontentloaded", timeout=30000)
    page.wait_for_selector("h1.J-baseInfo-name", state="attached", timeout=15000)

    product = extract_product_json(page)
    offers = product.get("offers") or {}
    row["price"] = offers.get("price", "")
    row["currency"] = offers.get("priceCurrency", "")
    images = product.get("image") or []
    if isinstance(images, str):
        images = [images]
    if not images:
        images = extract_gallery_images(page)
    row["images"] = str(images)
    if not images:
        raise ValueError("missing images")

    row["category"] = extract_category(page)
    if not row["category"]:
        raise ValueError("missing category")

    basic_info = extract_basic_info(page)
    row["brand"] = basic_info.get("Engine Brand") or basic_info.get("Trademark", "")
    row["model"] = basic_info.get("Model NO.", "")
    row["year"] = basic_info.get("Year", "")
    row["hours"] = basic_info.get("Working Hours", "")
    row["country"] = basic_info.get("Origin", "")

    company_name, company_url = extract_company(page)
    row["company_name"] = company_name
    row["company_url"] = company_url
    if not company_name or not company_url:
        raise ValueError("missing company info")

    row["vendor_name"] = extract_vendor_name(page)
    if not row["vendor_name"]:
        raise ValueError("missing vendor name")

    return row


def load_done_urls(output_path):
    """
    Resume support: a url counts as "done" as soon as its row is in
    output_path, since crawl_urls() only ever writes fully-successful rows
    (failed urls are never written here, see crawl_urls()).
    """
    if not os.path.exists(output_path):
        return set()
    try:
        df = pd.read_csv(output_path, usecols=["url"])
        return set(df["url"].dropna().astype(str).tolist())
    except Exception:
        return set()


def save_retry_links(urls, path):
    """Save urls that still failed after the retry pass, for a future run."""
    if not urls:
        if os.path.exists(path):
            os.remove(path)
        return
    retry_df = pd.DataFrame({"url": list(dict.fromkeys(urls))})
    retry_df.to_csv(path, index=False, encoding="utf-8-sig", lineterminator="\n")


def append_to_csv(row, output_path):
    """Append a single successful row to output_path, matching FIELDNAMES order."""
    file_exists = os.path.exists(output_path) and os.path.getsize(output_path) > 0

    df = pd.DataFrame([row], columns=FIELDNAMES)

    df.to_csv(
        output_path,
        mode="a",
        header=not file_exists,
        index=False,
        encoding="utf-8-sig",
        lineterminator="\n",
    )


def crawl_urls(urls, output_path, delay, headless):
    """
    Crawl a batch of urls, appending successful rows to output_path.
    A url that errors (missing fields, selector timeout, etc.) is skipped
    and returned in failed_urls instead of writing a partial row -- this
    keeps every row matching FIELDNAMES and lets the crawl continue past
    the failure instead of crashing.
    """
    failed_urls = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        page = browser.new_page(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
            )
        )

        for i, url in enumerate(urls, start=1):
            print(f"[{i}/{len(urls)}] {url}")
            try:
                row = crawl_product(page, url)
            except Exception as exc:
                print(f"  error, skipped: {exc}")
                failed_urls.append(url)
                time.sleep(delay)
                continue

            append_to_csv(row, output_path)
            time.sleep(delay)

        browser.close()

    return failed_urls


def crawl(input_paths, output_path, retry_path, delay, headless, limit):
    if isinstance(input_paths, str):
        input_paths = [input_paths]
    urls = (
        pd.concat([pd.read_csv(p)["url"] for p in input_paths])
        .dropna()
        .astype(str)
        .unique()
        .tolist()
    )

    if limit:
        urls = urls[:limit]

    done_urls = load_done_urls(output_path)
    todo_urls = [url for url in urls if url not in done_urls]

    print(f"{len(done_urls)} already crawled, {len(todo_urls)} left to crawl")
    if not todo_urls:
        print(f"Nothing to do, {output_path} is already up to date")
        return

    failed_urls = crawl_urls(todo_urls, output_path, delay, headless)
    print(f"\nFirst pass done: {len(todo_urls) - len(failed_urls)} saved, {len(failed_urls)} failed")

    if failed_urls:
        print(f"\nRetrying {len(failed_urls)} failed urls...\n")
        failed_urls = crawl_urls(failed_urls, output_path, delay, headless)
        print(f"Retry pass done, {len(failed_urls)} still failed")

    save_retry_links(failed_urls, retry_path)
    print(f"\nSaved rows to {output_path}")
    if failed_urls:
        print(f"{len(failed_urls)} urls still unresolved, saved to {retry_path}")


if __name__ == "__main__":
    crawl(
        input_paths=INPUT_FILE,
        output_path=OUTPUT_FILE,
        retry_path=RETRY_FILE,
        delay=DELAY,
        headless=HEADLESS,
        limit=LIMIT,
    )

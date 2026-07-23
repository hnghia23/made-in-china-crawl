"""Crawl part 5/6: output/links/link_09.csv, link_10.csv."""

from crawl_products import crawl

if __name__ == "__main__":
    crawl(
        input_paths=["output/links/link_09.csv", "output/links/link_10.csv"],
        output_path="output/product_5.csv",
        retry_path="retry_links_5.csv",
        delay=2.0,
        headless=True,
        limit=0,
    )

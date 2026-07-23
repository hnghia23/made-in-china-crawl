"""Crawl part 6/6: output/links/link_11.csv, link_12.csv."""

from crawl_products import crawl

if __name__ == "__main__":
    crawl(
        input_paths=["output/links/link_11.csv", "output/links/link_12.csv"],
        output_path="output/product_6.csv",
        retry_path="retry_links_6.csv",
        delay=2.0,
        headless=True,
        limit=0,
    )

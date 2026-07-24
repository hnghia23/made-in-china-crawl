"""Crawl part 1/6: output/links/link_07.csv."""

from crawl_products import crawl

if __name__ == "__main__":
    crawl(
        input_paths=["output/links/link_07.csv"],
        output_path="output/product_1.csv",
        retry_path="retry_links_1.csv",
        delay=2.0,
        headless=True,
        limit=0,
    )

"""Crawl part 3/6: output/links/link_09.csv."""

from crawl_products import crawl

if __name__ == "__main__":
    crawl(
        input_paths=["output/links/link_09.csv"],
        output_path="output/product_3.csv",
        retry_path="retry_links_3.csv",
        delay=2.0,
        headless=True,
        limit=0,
    )

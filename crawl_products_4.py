"""Crawl part 4/6: output/links/link_10.csv."""

from crawl_products import crawl

if __name__ == "__main__":
    crawl(
        input_paths=["output/links/link_10.csv"],
        output_path="output/product_4.csv",
        retry_path="retry_links_4.csv",
        delay=2.0,
        headless=True,
        limit=0,
    )

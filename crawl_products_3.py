"""Crawl part 3/6: output/links/link_05.csv, link_06.csv."""

from crawl_products import crawl

if __name__ == "__main__":
    crawl(
        input_paths=["output/links/link_05.csv", "output/links/link_06.csv"],
        output_path="output/product_3.csv",
        retry_path="retry_links_3.csv",
        delay=2.0,
        headless=True,
        limit=0,
    )

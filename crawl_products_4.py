"""Crawl part 4/6: output/links/link_07.csv, link_08.csv."""

from crawl_products import crawl

if __name__ == "__main__":
    crawl(
        input_paths=["output/links/link_07.csv", "output/links/link_08.csv"],
        output_path="output/product_4.csv",
        retry_path="retry_links_4.csv",
        delay=2.0,
        headless=True,
        limit=0,
    )

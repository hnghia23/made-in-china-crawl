"""Crawl part 2/6: output/links/link_03.csv, link_04.csv."""

from crawl_products import crawl

if __name__ == "__main__":
    crawl(
        input_paths=["output/links/link_03.csv", "output/links/link_04.csv"],
        output_path="output/product_2.csv",
        retry_path="retry_links_2.csv",
        delay=2.0,
        headless=True,
        limit=0,
    )

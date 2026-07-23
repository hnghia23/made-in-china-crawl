"""Split output/link.csv into fixed-size chunks under output/links/.

Each chunk has CHUNK_SIZE urls, except the last chunk which absorbs the
remainder (e.g. 24014 urls / 2000 -> 11 files of 2000 + 1 file of 2014).
"""

import os

import pandas as pd

INPUT_FILE = "output/link.csv"
OUTPUT_DIR = "output/links"
CHUNK_SIZE = 2000


def split():
    urls = pd.read_csv(INPUT_FILE)["url"].dropna().astype(str).tolist()

    num_files = len(urls) // CHUNK_SIZE
    remainder = len(urls) % CHUNK_SIZE
    if remainder:
        sizes = [CHUNK_SIZE] * (num_files - 1) + [CHUNK_SIZE + remainder]
    else:
        sizes = [CHUNK_SIZE] * num_files

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    start = 0
    for i, size in enumerate(sizes, start=1):
        chunk = urls[start:start + size]
        start += size
        path = os.path.join(OUTPUT_DIR, f"link_{i:02d}.csv")
        pd.DataFrame({"url": chunk}).to_csv(
            path, index=False, encoding="utf-8-sig", lineterminator="\n"
        )
        print(f"{path}: {len(chunk)} urls")


if __name__ == "__main__":
    split()

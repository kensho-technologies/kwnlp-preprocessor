# Copyright 2021-present Kensho Technologies, LLC.
"""Create a CSV that maps all page titles to page ids.

This is meant to be used on links in the full text of wikipedia.

For article titles it will map to the page id
For redirect titles it will map to the ultimate redirect article page id.
"""
import logging
import os

import pandas as pd

from kwnlp_preprocessor import argconfig

logger = logging.getLogger(__name__)


def main(
    wp_yyyymmdd: str,
    data_path: str = argconfig.DEFAULT_KWNLP_DATA_PATH,
    wiki: str = argconfig.DEFAULT_KWNLP_WIKI,
) -> None:

    wp_dump_path = os.path.join(data_path, f"wikipedia-derived-{wp_yyyymmdd}")

    # read page CSV
    # ====================================================================
    file_path = os.path.join(
        wp_dump_path,
        "kwnlp-sql",
        f"{wiki}-{wp_yyyymmdd}-page.csv",
    )
    logger.info(f"reading {file_path}")
    df_page = pd.read_csv(
        file_path,
        keep_default_na=False,
        usecols=["page_id", "page_title"],
    )

    # read ultimate-redirect CSV
    # ====================================================================
    file_path = os.path.join(
        wp_dump_path,
        "kwnlp-sql",
        f"kwnlp-{wiki}-{wp_yyyymmdd}-ultimate-redirect.csv",
    )
    logger.info(f"reading {file_path}")
    df_redirect = pd.read_csv(file_path, keep_default_na=False)

    # left join page and redirect
    # ====================================================================
    df = pd.merge(
        df_page,
        df_redirect,
        left_on="page_id",
        right_on="source_id",
        how="left",
    )
    df["is_redirect"] = ~df["source_id"].isnull()
    df = df.drop(columns=["source_id", "source_title"])

    mask = ~df["is_redirect"]
    df.loc[mask, "target_id"] = df.loc[mask, "page_id"]
    df["target_id"] = df["target_id"].astype(int)
    df.loc[mask, "target_title"] = df.loc[mask, "page_title"]
    df = df.rename(columns={"page_id": "source_id", "page_title": "source_title"})

    # sort and write output
    # ====================================================================
    df = df.sort_values("source_id")
    file_path = os.path.join(
        wp_dump_path,
        "kwnlp-sql",
        f"kwnlp-{wiki}-{wp_yyyymmdd}-title-mapper.csv",
    )
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    logger.info(f"writing {file_path}")
    df.to_csv(file_path, index=False)


if __name__ == "__main__":

    description = "create title mapper data"
    arg_names = ["wp_yyyymmdd", "data_path", "wiki", "loglevel"]
    parser = argconfig.get_argparser(description, arg_names)

    args = parser.parse_args()
    logging.basicConfig(level=args.loglevel)
    logger.info(f"args={args}")

    main(args.wp_yyyymmdd, data_path=args.data_path, wiki=args.wiki)

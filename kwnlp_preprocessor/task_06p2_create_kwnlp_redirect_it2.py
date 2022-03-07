# Copyright 2021-present Kensho Technologies, LLC.
"""Add source page titles and target page ids to redirect CSV."""
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

    # read redirect CSV
    # ====================================================================
    file_path = os.path.join(
        wp_dump_path,
        "kwnlp-sql",
        f"{wiki}-{wp_yyyymmdd}-redirect.csv",
    )
    logger.info(f"reading {file_path}")
    df_redirect = pd.read_csv(
        file_path,
        keep_default_na=False,
    )
    df_redirect = df_redirect.rename(columns={"rd_from": "source_id", "rd_title": "target_title"})

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

    # merge to add source titles
    # ====================================================================
    logger.info("merging to add source titles")
    df = pd.merge(
        df_redirect,
        df_page,
        left_on="source_id",
        right_on="page_id",
    )
    df = df.drop(columns=["page_id"])
    df = df.rename(columns={"page_title": "source_title"})

    # merge to add target ids
    # ====================================================================
    logger.info("merging to add target ids")
    df = pd.merge(
        df,
        df_page,
        left_on="target_title",
        right_on="page_title",
    )
    df = df.drop(columns=["page_title"])
    df = df.rename(columns={"page_id": "target_id"})
    df = df[["source_id", "source_title", "target_id", "target_title"]]

    # write output
    # ====================================================================
    file_path = os.path.join(
        wp_dump_path,
        "kwnlp-sql",
        f"{wiki}-{wp_yyyymmdd}-redirect-it2.csv",
    )
    logger.info(f"writing {file_path}")
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    df.to_csv(file_path, index=False)


if __name__ == "__main__":

    description = "add source title and target id to raw redirect data"
    arg_names = ["wp_yyyymmdd", "data_path", "wiki", "loglevel"]
    parser = argconfig.get_argparser(description, arg_names)

    args = parser.parse_args()
    logging.basicConfig(level=args.loglevel)
    logger.info(f"args={args}")

    main(args.wp_yyyymmdd, data_path=args.data_path, wiki=args.wiki)

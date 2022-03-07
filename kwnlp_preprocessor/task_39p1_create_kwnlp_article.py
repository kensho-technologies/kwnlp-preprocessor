# Copyright 2021-present Kensho Technologies, LLC.
"""Combine pre-wikitext parsing article CSV with post-wikitext parsing CSV."""
import logging
import os

import pandas as pd

from kwnlp_preprocessor import argconfig

logger = logging.getLogger(__name__)


# TODO: centralize this in argconfig
# have to change in task 24p1 create article_pre too
ROOT_NQIDS = [
    17442446,  # Wikimedia internal item
    14795564,  # point in time with respect to recurrent timeframe
    18340514,  # events in a specific year or time period
    5,  # human
    2221906,  # geographic location
    43229,  # organization
    4830453,  # business
]


def main(
    wp_yyyymmdd: str,
    data_path: str = argconfig.DEFAULT_KWNLP_DATA_PATH,
    wiki: str = argconfig.DEFAULT_KWNLP_WIKI,
) -> None:

    wp_dump_path = os.path.join(data_path, f"wikipedia-derived-{wp_yyyymmdd}")

    # read article-pre CSV as base
    # ====================================================================
    file_path = os.path.join(
        wp_dump_path,
        "kwnlp-sql",
        f"kwnlp-{wiki}-{wp_yyyymmdd}-article-pre.csv",
    )
    logger.info(f"reading {file_path}")
    df = pd.read_csv(
        file_path,
        keep_default_na=False,
    )

    # read and merge in-out counts
    # ====================================================================
    file_path = os.path.join(
        wp_dump_path,
        "in-out-counts",
        f"kwnlp-{wiki}-{wp_yyyymmdd}-in-out-counts.csv",
    )
    logger.info(f"reading {file_path}")
    df_ioc = pd.read_csv(file_path)

    df = pd.merge(df, df_ioc, on="page_id", how="left")
    df[["in_count", "out_count"]] = df[["in_count", "out_count"]].fillna(0).astype("int")
    df = df.rename(columns={"in_count": "in_link_count", "out_count": "out_link_count"})

    # read and merge lengths
    # ====================================================================
    file_path = os.path.join(
        wp_dump_path,
        "lengths",
        f"kwnlp-{wiki}-{wp_yyyymmdd}-lengths.csv",
    )
    logger.info(f"reading {file_path}")
    df_len = pd.read_csv(file_path)

    df = pd.merge(df, df_len, on="page_id", how="left")
    df[["len_article_chars", "len_intro_chars"]] = (
        df[["len_article_chars", "len_intro_chars"]].fillna(0).astype("int")
    )

    # read and merge template data
    # ====================================================================
    file_path = os.path.join(
        wp_dump_path,
        "templates",
        f"kwnlp-{wiki}-{wp_yyyymmdd}-templates.csv",
    )
    logger.info(f"reading {file_path}")
    df_tmp = pd.read_csv(file_path)

    TEMPLATE_RENAMES = {
        "good_article": "tmpl_good_article",
        "featured_article": "tmpl_featured_article",
        "pseudoscience": "tmpl_pseudoscience",
        "conspiracy_theories": "tmpl_conspiracy_theories",
    }
    tmpl_col_names = list(TEMPLATE_RENAMES.values())
    df = pd.merge(df, df_tmp, on="page_id", how="left")
    df = df.rename(columns=TEMPLATE_RENAMES)
    df[tmpl_col_names] = df[tmpl_col_names].fillna(0).astype("int")

    # sort and write output
    # ====================================================================
    df = df[
        [
            "page_id",
            "item_id",
            "page_title",
            "views",
            "len_article_chars",
            "len_intro_chars",
            "in_link_count",
            "out_link_count",
        ]
        + tmpl_col_names
        + [f"isa_Q{nqid}" for nqid in ROOT_NQIDS]
    ]
    df = df.sort_values("page_id")
    file_path = os.path.join(
        wp_dump_path,
        "kwnlp-sql",
        f"kwnlp-{wiki}-{wp_yyyymmdd}-article.csv",
    )
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    logger.info(f"writing {file_path}")
    df.to_csv(file_path, index=False)


if __name__ == "__main__":

    description = "create kwnlp-article"
    arg_names = ["wp_yyyymmdd", "data_path", "wiki", "loglevel"]
    parser = argconfig.get_argparser(description, arg_names)

    args = parser.parse_args()
    logging.basicConfig(level=args.loglevel)
    logger.info(f"args={args}")

    main(
        args.wp_yyyymmdd,
        data_path=args.data_path,
        wiki=args.wiki,
    )

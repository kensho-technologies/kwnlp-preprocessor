# Copyright 2021-present Kensho Technologies, LLC.
"""Calculate ultimate redirects.

Here we find resolve chains of redirects to ultimate redirects,

A->B and B->C
becomes
A->C and B->C

In the process we remove cycles (self redirects, circular pairs, ...)
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
    ).set_index("page_id")

    # read redirect-it2 CSV
    # ====================================================================
    file_path = os.path.join(
        wp_dump_path,
        "kwnlp-sql",
        f"{wiki}-{wp_yyyymmdd}-redirect-it2.csv",
    )
    logger.info(f"reading {file_path}")
    df_redirect = pd.read_csv(file_path, keep_default_na=False).set_index("source_id")

    # mask to identify multi-hop redirects
    # ====================================================================
    mask = df_redirect["target_id"].isin(df_redirect.index)
    logger.info("resolving {} multi-hop redirects".format(mask.sum()))

    # calculate ultimate redirects
    # e.g. if A->B and B->C then we update such that
    # A->C and B->C
    # ====================================================================
    new_target_ids = []
    new_target_titles = []
    for irow, target_id in enumerate(df_redirect.loc[mask, "target_id"].values):
        target_chain = [target_id]

        # while the target id exists in the source id index, follow the chain
        while target_id in df_redirect.index:

            target_id = df_redirect.loc[target_id, "target_id"]
            target_title = df_page.loc[target_id, "page_title"]
            if target_id in target_chain:
                logger.info("cycle detected: {}".format(target_chain))
                target_id = -1
                target_title = ""
                break
            else:
                target_chain.append(target_id)

        new_target_ids.append(target_id)
        new_target_titles.append(target_title)

    df_redirect.loc[mask, "target_id"] = new_target_ids
    df_redirect.loc[mask, "target_title"] = new_target_titles
    df_redirect = df_redirect[df_redirect["target_id"] != -1]

    # sort and write output
    # ====================================================================
    df_redirect = df_redirect.sort_values("source_id")
    file_path = os.path.join(
        wp_dump_path,
        "kwnlp-sql",
        f"kwnlp-{wiki}-{wp_yyyymmdd}-ultimate-redirect.csv",
    )
    logger.info(f"writing {file_path}")
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    df_redirect.to_csv(file_path)


if __name__ == "__main__":

    description = "create ultimate redirect data"
    arg_names = ["wp_yyyymmdd", "data_path", "wiki", "loglevel"]
    parser = argconfig.get_argparser(description, arg_names)

    args = parser.parse_args()
    logging.basicConfig(level=args.loglevel)
    logger.info(f"args={args}")

    main(args.wp_yyyymmdd, data_path=args.data_path, wiki=args.wiki)

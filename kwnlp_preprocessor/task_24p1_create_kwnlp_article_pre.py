# Copyright 2021-present Kensho Technologies, LLC.
"""Create a CSV that contains pre-wikitext parsing article metadata."""
import logging
import os

import networkx as nx
import pandas as pd

from kwnlp_preprocessor import argconfig

logger = logging.getLogger(__name__)


# TODO: centralize this in argconfig
# have to change in task 39p1 create article too
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
    wd_yyyymmdd: str,
    data_path: str = argconfig.DEFAULT_KWNLP_DATA_PATH,
    wiki: str = argconfig.DEFAULT_KWNLP_WIKI,
) -> None:

    wp_dump_path = os.path.join(data_path, f"wikipedia-derived-{wp_yyyymmdd}")
    wd_dump_path = os.path.join(data_path, f"wikidata-derived-{wd_yyyymmdd}")

    # read title mapper CSV as base
    # ====================================================================
    file_path = os.path.join(
        wp_dump_path,
        "kwnlp-sql",
        f"kwnlp-{wiki}-{wp_yyyymmdd}-title-mapper.csv",
    )
    logger.info(f"reading {file_path}")
    df = pd.read_csv(
        file_path,
        keep_default_na=False,
        usecols=["source_id", "source_title", "is_redirect"],
    )

    # get base information from title mapper
    # ====================================================================
    df = df[~df["is_redirect"]]
    df = df[["source_id", "source_title"]]
    df = df.rename(columns={"source_id": "page_id", "source_title": "page_title"})

    # read item id from page props CSV
    # ====================================================================
    file_path = os.path.join(
        wp_dump_path,
        "kwnlp-sql",
        f"kwnlp-{wiki}-{wp_yyyymmdd}-page-props.csv",
    )
    logger.info(f"reading {file_path}")
    df_pp = pd.read_csv(file_path, usecols=["page_id", "wikibase_item"])

    # add wikidata item id
    # ====================================================================
    df = pd.merge(df, df_pp, on="page_id", how="left")

    df["wikibase_item"] = df["wikibase_item"].fillna("Q-1").apply(lambda x: int(x[1:]))

    df = df.rename(columns={"wikibase_item": "item_id"})

    # read subclass of info
    # ====================================================================
    file_path = os.path.join(
        wd_dump_path,
        "p279-claim",
        f"kwnlp-wikidata-{wd_yyyymmdd}-p279-claim.csv",
    )
    logger.info(f"reading {file_path}")
    df_p279 = pd.read_csv(file_path, usecols=["source_id", "target_id"])
    logger.info("building p279 graph")
    g_p279 = nx.DiGraph()
    g_p279.add_edges_from(df_p279.values)

    # read instance of info
    # ====================================================================
    file_path = os.path.join(
        wd_dump_path,
        "p31-claim",
        f"kwnlp-wikidata-{wd_yyyymmdd}-p31-claim.csv",
    )
    logger.info(f"reading {file_path}")
    df_p31 = pd.read_csv(file_path, usecols=["source_id", "target_id"])

    # add root qid tags
    # ====================================================================
    for root_nqid in ROOT_NQIDS:

        logger.info(f"tagging items in subclass tree of {root_nqid}")
        col_name = f"isa_Q{root_nqid}"
        subclass_qids = set(nx.ancestors(g_p279, root_nqid)).union(set([root_nqid]))
        mask = df_p31["target_id"].isin(subclass_qids)
        tmp_df = df_p31[mask].copy()
        tmp_df = tmp_df.sort_values(by=["source_id", "target_id"])
        tmp_df = tmp_df.drop_duplicates(subset=["source_id"], keep="first")
        tmp_df = tmp_df.rename(columns={"source_id": "item_id", "target_id": col_name})

        df = pd.merge(df, tmp_df, on="item_id", how="left")
        df[col_name] = df[col_name].fillna(False).astype(int)

    # read views CSV
    # ====================================================================
    file_path = os.path.join(
        wp_dump_path,
        "kwnlp-sql",
        f"kwnlp-{wiki}-{wp_yyyymmdd}-prior-month-pageviews-complete.csv",
    )
    logger.info(f"reading {file_path}")
    df_views = pd.read_csv(file_path, keep_default_na=False)

    # add views
    # ====================================================================
    df = pd.merge(df, df_views, on="page_title", how="left")
    df["views"] = df["views"].fillna(0).astype("int")

    # sort and write output
    # ====================================================================
    df = df[["page_id", "item_id", "page_title", "views"] + [f"isa_Q{nqid}" for nqid in ROOT_NQIDS]]
    df = df.sort_values("page_id")
    file_path = os.path.join(
        wp_dump_path,
        "kwnlp-sql",
        f"kwnlp-{wiki}-{wp_yyyymmdd}-article-pre.csv",
    )
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    logger.info(f"writing {file_path}")
    df.to_csv(file_path, index=False)
    return df


if __name__ == "__main__":

    description = "create kwnlp article pre"
    arg_names = ["wp_yyyymmdd", "wd_yyyymmdd", "data_path", "wiki", "loglevel"]
    parser = argconfig.get_argparser(description, arg_names)

    args = parser.parse_args()
    logging.basicConfig(level=args.loglevel)
    logger.info(f"args={args}")

    main(
        args.wp_yyyymmdd,
        args.wd_yyyymmdd,
        data_path=args.data_path,
        wiki=args.wiki,
    )

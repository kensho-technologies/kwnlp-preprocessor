# Copyright 2021-present Kensho Technologies, LLC.
from collections import Counter
import logging
import os
import re
import typing

import pandas as pd

from kwnlp_preprocessor import argconfig
from kwnlp_preprocessor import utils


logger = logging.getLogger(__name__)


def gather_link_edge_list(wp_yyyymmdd: str, data_path: str, wiki: str) -> None:

    in_dump_path = os.path.join(data_path, f"wikipedia-derived-{wp_yyyymmdd}", "links-chunks")
    logger.info(f"in dump path: {in_dump_path}")

    pattern = re.compile("kwnlp-" + wiki + r"-\d{8}-links(\d{1,2})-p(\d+)p(\d+)\.csv")
    all_file_names = [
        match.string for match in utils._get_ordered_files_from_path(in_dump_path, pattern)
    ]

    df = pd.DataFrame()
    for file_name in all_file_names:
        file_path = os.path.join(in_dump_path, file_name)
        logger.info(f"collecting from {file_path}")

        df1 = pd.read_csv(file_path)
        df = pd.concat([df, df1])

    out_dump_path = os.path.join(data_path, f"wikipedia-derived-{wp_yyyymmdd}", "links")
    os.makedirs(out_dump_path, exist_ok=True)
    logger.info(f"out dump path: {out_dump_path}")
    out_file_path = os.path.join(out_dump_path, f"kwnlp-{wiki}-{wp_yyyymmdd}-links.csv")
    df.to_csv(out_file_path, index=False)

    out_dump_path = os.path.join(data_path, f"wikipedia-derived-{wp_yyyymmdd}", "links-edges-plus")
    os.makedirs(out_dump_path, exist_ok=True)
    logger.info(f"out dump path: {out_dump_path}")
    out_file_path = os.path.join(out_dump_path, f"kwnlp-{wiki}-{wp_yyyymmdd}-links-edges-plus.csv")
    df[["source_page_id", "section_idx", "paragraph_idx", "target_page_id"]].to_csv(
        out_file_path, index=False
    )

    out_dump_path = os.path.join(data_path, f"wikipedia-derived-{wp_yyyymmdd}", "links-edges")
    os.makedirs(out_dump_path, exist_ok=True)
    logger.info(f"out dump path: {out_dump_path}")
    out_file_path = os.path.join(out_dump_path, f"kwnlp-{wiki}-{wp_yyyymmdd}-links-edges.csv")
    df[["source_page_id", "target_page_id"]].to_csv(out_file_path, index=False)


def gather_anchor_counts(wp_yyyymmdd: str, data_path: str, wiki: str) -> None:

    in_dump_path = os.path.join(
        data_path, f"wikipedia-derived-{wp_yyyymmdd}", "anchor-target-counts-chunks"
    )

    out_dump_path = os.path.join(
        data_path, f"wikipedia-derived-{wp_yyyymmdd}", "anchor-target-counts"
    )

    logger.info(f"in dump path: {in_dump_path}")
    os.makedirs(out_dump_path, exist_ok=True)
    logger.info(f"out dump path: {out_dump_path}")

    pattern = re.compile(
        "kwnlp-" + wiki + r"-\d{8}-anchor-target-counts(\d{1,2})-p(\d+)p(\d+)\.csv"
    )
    all_atc_file_names = [
        match.string for match in utils._get_ordered_files_from_path(in_dump_path, pattern)
    ]

    atc: typing.Counter[typing.Tuple[str, int]] = Counter()
    for atc_file_name in all_atc_file_names:
        atc_file_path = os.path.join(in_dump_path, atc_file_name)
        logger.info(f"collecting from {atc_file_path}")

        df = pd.read_csv(atc_file_path)
        atc1 = Counter(
            {(a, t): c for a, t, c in zip(df["anchor_text"], df["target_page_id"], df["count"])}
        )
        del df
        atc = atc + atc1

    df_atc = pd.DataFrame(
        [(el[0][0], el[0][1], el[1]) for el in atc.most_common()],
        columns=["anchor_text", "target_page_id", "count"],
    )

    out_file_path = os.path.join(
        out_dump_path, f"kwnlp-{wiki}-{wp_yyyymmdd}-anchor-target-counts.csv"
    )
    df_atc.to_csv(out_file_path, index=False)


def gather_inout_counts(wp_yyyymmdd: str, data_path: str, wiki: str) -> None:

    in_dump_path = os.path.join(
        data_path, f"wikipedia-derived-{wp_yyyymmdd}", "in-out-counts-chunks"
    )

    out_dump_path = os.path.join(data_path, f"wikipedia-derived-{wp_yyyymmdd}", "in-out-counts")

    logger.info(f"in dump path: {in_dump_path}")
    os.makedirs(out_dump_path, exist_ok=True)
    logger.info(f"out dump path: {out_dump_path}")

    pattern = re.compile("kwnlp-" + wiki + r"-\d{8}-in-out-counts(\d{1,2})-p(\d+)p(\d+)\.csv")
    all_ioc_file_names = [
        match.string for match in utils._get_ordered_files_from_path(in_dump_path, pattern)
    ]

    in_c: typing.Counter[int] = Counter()
    out_c: typing.Counter[int] = Counter()
    for ioc_file_name in all_ioc_file_names:
        ioc_file_path = os.path.join(in_dump_path, ioc_file_name)
        logger.info(f"collecting from {ioc_file_path}")

        df = pd.read_csv(ioc_file_path)
        in_c1 = Counter({p: c for p, c in zip(df["page_id"], df["in_count"])})
        out_c1 = Counter({p: c for p, c in zip(df["page_id"], df["out_count"])})
        del df
        in_c = in_c + in_c1
        out_c = out_c + out_c1

    df_in = pd.DataFrame(in_c.most_common(), columns=["page_id", "in_count"])
    df_out = pd.DataFrame(out_c.most_common(), columns=["page_id", "out_count"])
    df_inout = pd.merge(df_in, df_out, on="page_id", how="outer").fillna(0).astype(int)
    df_inout = df_inout.sort_values("page_id")

    out_file_path = os.path.join(out_dump_path, f"kwnlp-{wiki}-{wp_yyyymmdd}-in-out-counts.csv")
    df_inout.to_csv(out_file_path, index=False)


def main(
    wp_yyyymmdd: str,
    data_path: str = argconfig.DEFAULT_KWNLP_DATA_PATH,
    wiki: str = argconfig.DEFAULT_KWNLP_WIKI,
) -> None:

    gather_link_edge_list(wp_yyyymmdd, data_path, wiki)
    gather_inout_counts(wp_yyyymmdd, data_path, wiki)
    gather_anchor_counts(wp_yyyymmdd, data_path, wiki)


if __name__ == "__main__":

    description = "collect post processed link data"
    arg_names = ["wp_yyyymmdd", "data_path", "wiki", "loglevel"]
    parser = argconfig.get_argparser(description, arg_names)

    args = parser.parse_args()
    logging.basicConfig(level=args.loglevel)
    logger.info(f"args={args}")

    main(args.wp_yyyymmdd, data_path=args.data_path, wiki=args.wiki)

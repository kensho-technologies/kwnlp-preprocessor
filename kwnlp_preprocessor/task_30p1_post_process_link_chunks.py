# Copyright 2021-present Kensho Technologies, LLC.
from collections import Counter
import logging
from multiprocessing import Pool
import os
import re
import typing

import pandas as pd

from kwnlp_preprocessor import argconfig, utils

logger = logging.getLogger(__name__)


def parse_file(args: dict) -> None:

    # read links
    logger.info("parsing {}".format(args["link_file_path"]))
    df_links = pd.read_csv(
        args["link_file_path"],
        usecols=["anchor_text", "source_page_id", "target_page_id"],
    )

    # calculate anchor target counts
    atc: typing.Counter[typing.Tuple[str, int]] = Counter(
        (zip(df_links["anchor_text"], df_links["target_page_id"]))
    )
    df_atc = pd.DataFrame(
        [(el[0][0], el[0][1], el[1]) for el in atc.most_common()],
        columns=["anchor_text", "target_page_id", "count"],
    )
    df_atc.to_csv(args["atc_file_path"], index=False)

    # calculate in/out link counts
    df_in = pd.DataFrame(
        Counter(df_links["target_page_id"]).most_common(),
        columns=["page_id", "in_count"],
    )

    df_out = pd.DataFrame(
        Counter(df_links["source_page_id"]).most_common(),
        columns=["page_id", "out_count"],
    )

    df_inout = pd.merge(df_in, df_out, on="page_id", how="outer").fillna(0).astype(int)
    df_inout = df_inout.sort_values("page_id")
    df_inout.to_csv(args["ioc_file_path"], index=False)


def main(
    wp_yyyymmdd: str,
    data_path: str = argconfig.DEFAULT_KWNLP_DATA_PATH,
    wiki: str = argconfig.DEFAULT_KWNLP_WIKI,
    workers: int = argconfig.DEFAULT_KWNLP_WORKERS,
) -> None:

    in_dump_path = os.path.join(data_path, f"wikipedia-derived-{wp_yyyymmdd}", "links-chunks")

    out_dump_paths = {
        "atc": os.path.join(
            data_path, f"wikipedia-derived-{wp_yyyymmdd}", "anchor-target-counts-chunks"
        ),
        "ioc": os.path.join(data_path, f"wikipedia-derived-{wp_yyyymmdd}", "in-out-counts-chunks"),
    }

    logger.info(f"in dump path: {in_dump_path}")

    for path in out_dump_paths.values():
        logger.info(f"out dump path: {path}")
        os.makedirs(path, exist_ok=True)

    pattern = re.compile("kwnlp-" + wiki + r"-\d{8}-links(\d{1,2})-p(\d+)p(\d+)\.csv")
    all_links_file_names = [
        match.string for match in utils._get_ordered_files_from_path(in_dump_path, pattern)
    ]

    mp_args = []
    for link_file_name in all_links_file_names:
        link_file_path = os.path.join(in_dump_path, link_file_name)

        atc_file_name = link_file_name.replace("links", "anchor-target-counts")
        atc_file_path = os.path.join(out_dump_paths["atc"], atc_file_name)

        ioc_file_name = link_file_name.replace("links", "in-out-counts")
        ioc_file_path = os.path.join(out_dump_paths["ioc"], ioc_file_name)

        mp_args.append(
            {
                "link_file_path": link_file_path,
                "atc_file_path": atc_file_path,
                "ioc_file_path": ioc_file_path,
            }
        )

    with Pool(workers) as p:
        p.map(parse_file, mp_args)


if __name__ == "__main__":

    description = "post process link chunks"
    arg_names = ["wp_yyyymmdd", "data_path", "wiki", "workers", "loglevel"]
    parser = argconfig.get_argparser(description, arg_names)

    args = parser.parse_args()
    logging.basicConfig(level=args.loglevel)
    logger.info(f"args={args}")

    main(
        args.wp_yyyymmdd,
        data_path=args.data_path,
        wiki=args.wiki,
        workers=args.workers,
    )

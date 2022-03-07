# Copyright 2021-present Kensho Technologies, LLC.
import logging
import os
import re

import pandas as pd

from kwnlp_preprocessor import argconfig, utils

logger = logging.getLogger(__name__)


def main(
    wp_yyyymmdd: str,
    data_path: str = argconfig.DEFAULT_KWNLP_DATA_PATH,
    wiki: str = argconfig.DEFAULT_KWNLP_WIKI,
) -> None:

    in_dump_path = os.path.join(
        data_path, f"wikipedia-derived-{wp_yyyymmdd}", "section-names-chunks"
    )

    out_dump_path = os.path.join(data_path, f"wikipedia-derived-{wp_yyyymmdd}", "section-names")

    logger.info(f"in dump path: {in_dump_path}")

    os.makedirs(out_dump_path, exist_ok=True)
    logger.info(f"out dump path: {out_dump_path}")

    pattern = re.compile("kwnlp-" + wiki + r"-\d{8}-section-names(\d{1,2})-p(\d+)p(\d+)\.csv")
    all_file_names = [
        match.string for match in utils._get_ordered_files_from_path(in_dump_path, pattern)
    ]

    df = pd.DataFrame()
    for file_name in all_file_names:
        file_path = os.path.join(in_dump_path, file_name)
        logger.info(f"collecting from {file_path}")

        df1 = pd.read_csv(file_path)
        df = pd.concat([df, df1])

    out_file_path = os.path.join(out_dump_path, f"kwnlp-{wiki}-{wp_yyyymmdd}-section-names.csv")
    df.to_csv(out_file_path, index=False)


if __name__ == "__main__":

    description = "collect section names"
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

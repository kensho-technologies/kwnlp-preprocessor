# Copyright 2021-present Kensho Technologies, LLC.
import logging
import os
import re

import pandas as pd

from kwnlp_preprocessor import argconfig
from kwnlp_preprocessor import utils


logger = logging.getLogger(__name__)


def main(wd_yyyymmdd: str, data_path: str = argconfig.DEFAULT_KWNLP_DATA_PATH) -> None:

    for sample in [
        "p31-claim",
        "p279-claim",
        "qpq-claim",
        "item",
        "item-alias",
        "item-statements",
        "property",
        "property-alias",
        "skipped-entity",
    ]:

        in_dump_path = os.path.join(
            data_path,
            f"wikidata-derived-{wd_yyyymmdd}",
            f"{sample}-chunks",
        )
        logger.info(f"in_dump_path: {in_dump_path}")

        out_dump_path = os.path.join(
            data_path,
            f"wikidata-derived-{wd_yyyymmdd}",
            f"{sample}",
        )
        out_dump_file = os.path.join(
            out_dump_path,
            f"kwnlp-wikidata-{wd_yyyymmdd}-{sample}.csv",
        )
        logger.info(f"out_dump_path: {out_dump_path}")
        os.makedirs(out_dump_path, exist_ok=True)

        pattern = re.compile(r"kwnlp-wikidata-\d{8}-chunk-(\d{4})-" + sample + ".csv")
        all_file_names = [
            match.string for match in utils._get_ordered_files_from_path(in_dump_path, pattern)
        ]

        df = pd.DataFrame()
        for file_name in all_file_names:
            file_path = os.path.join(in_dump_path, file_name)
            df1 = pd.read_csv(file_path)
            df = pd.concat([df, df1])
        df.to_csv(out_dump_file, index=False)


if __name__ == "__main__":

    description = "gather wikidata chunks"
    arg_names = ["wd_yyyymmdd", "data_path", "loglevel"]
    parser = argconfig.get_argparser(description, arg_names)

    args = parser.parse_args()
    logging.basicConfig(level=args.loglevel)
    logger.info(f"args={args}")

    main(args.wd_yyyymmdd, data_path=args.data_path)

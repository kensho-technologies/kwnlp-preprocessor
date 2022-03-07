# Copyright 2021-present Kensho Technologies, LLC.
"""Update page_props format."""
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

    wp_derived_path = os.path.join(data_path, f"wikipedia-derived-{wp_yyyymmdd}")

    # read page_props CSV
    # ====================================================================
    file_path = os.path.join(
        wp_derived_path,
        "kwnlp-sql",
        f"{wiki}-{wp_yyyymmdd}-page-props.csv",
    )
    logger.info(f"reading {file_path}")
    df_pp = pd.read_csv(
        file_path,
        keep_default_na=False,
    )

    # reform
    # ====================================================================
    page_ids = df_pp["pp_page"].unique()
    df = pd.DataFrame({"page_id": page_ids})

    for propname in df_pp["pp_propname"].unique():

        df = (
            pd.merge(
                df,
                df_pp.loc[df_pp["pp_propname"] == propname, ["pp_page", "pp_value"]],
                left_on="page_id",
                right_on="pp_page",
                how="left",
            )
            .rename(columns={"pp_value": propname})
            .drop(columns=["pp_page"])
        )

    # write output
    # ====================================================================
    file_path = os.path.join(
        wp_derived_path,
        "kwnlp-sql",
        f"kwnlp-{wiki}-{wp_yyyymmdd}-page-props.csv",
    )
    logger.info(f"writing {file_path}")
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    df.to_csv(file_path, index=False)


if __name__ == "__main__":

    description = "convert KWNLP page_props CSV"
    arg_names = ["wp_yyyymmdd", "data_path", "wiki", "loglevel"]
    parser = argconfig.get_argparser(description, arg_names)

    args = parser.parse_args()
    logging.basicConfig(level=args.loglevel)
    logger.info(f"args={args}")

    main(args.wp_yyyymmdd, data_path=args.data_path, wiki=args.wiki)

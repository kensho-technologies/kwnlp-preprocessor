# Copyright 2021-present Kensho Technologies, LLC.
"""Convert raw SQL dumps into CSVs."""
import logging
import os

from kwnlp_sql_parser.wp_sql_dump import WikipediaSqlDump

from kwnlp_preprocessor import argconfig

logger = logging.getLogger(__name__)
ARTICLE_NAMESPACE = ("0",)


def _get_in_table_path(
    prefix: str,
    table: str,
    wiki: str,
    wp_yyyymmdd: str,
) -> str:
    return os.path.join(
        prefix,
        "{}table".format(table.replace("_", "")),
        f"{wiki}-{wp_yyyymmdd}-{table}.sql.gz",
    )


def _get_out_table_path(
    prefix: str,
    table: str,
    wiki: str,
    wp_yyyymmdd: str,
) -> str:
    name = table.replace("_", "-")
    return os.path.join(
        prefix,
        f"{wiki}-{wp_yyyymmdd}-{name}.csv",
    )


def main(
    wp_yyyymmdd: str,
    data_path: str = argconfig.DEFAULT_KWNLP_DATA_PATH,
    wiki: str = argconfig.DEFAULT_KWNLP_WIKI,
) -> None:

    wp_in_path = os.path.join(data_path, f"wikipedia-raw-{wp_yyyymmdd}")
    wp_out_path = os.path.join(data_path, f"wikipedia-derived-{wp_yyyymmdd}", "kwnlp-sql")

    table = "page_props"
    in_file_path = _get_in_table_path(wp_in_path, table, wiki, wp_yyyymmdd)
    out_file_path = _get_out_table_path(wp_out_path, table, wiki, wp_yyyymmdd)
    os.makedirs(os.path.dirname(out_file_path), exist_ok=True)
    page_props_sql_dump = WikipediaSqlDump(
        in_file_path,
        allowlists={"pp_propname": ("wikibase_item", "wikibase-shortdesc")},
        keep_column_names=("pp_page", "pp_propname", "pp_value"),
    )
    page_props_sql_dump.to_csv(outfile=out_file_path)

    table = "redirect"
    in_file_path = _get_in_table_path(wp_in_path, table, wiki, wp_yyyymmdd)
    out_file_path = _get_out_table_path(wp_out_path, table, wiki, wp_yyyymmdd)
    os.makedirs(os.path.dirname(out_file_path), exist_ok=True)
    redirect_sql_dump = WikipediaSqlDump(
        in_file_path,
        allowlists={"rd_namespace": ARTICLE_NAMESPACE},
        keep_column_names=("rd_from", "rd_title"),
    )
    redirect_sql_dump.to_csv(outfile=out_file_path)

    table = "page"
    in_file_path = _get_in_table_path(wp_in_path, table, wiki, wp_yyyymmdd)
    out_file_path = _get_out_table_path(wp_out_path, table, wiki, wp_yyyymmdd)
    os.makedirs(os.path.dirname(out_file_path), exist_ok=True)
    page_sql_dump = WikipediaSqlDump(
        in_file_path,
        allowlists={"page_namespace": ARTICLE_NAMESPACE},
        keep_column_names=(
            "page_id",
            "page_namespace",
            "page_title",
            "page_is_redirect",
            "page_is_new",
            "page_touched",
            "page_links_updated",
            "page_latest",
            "page_len",
        ),
    )
    page_sql_dump.to_csv(outfile=out_file_path)


if __name__ == "__main__":

    description = "convert SQL dumps to CSVs"
    arg_names = ["wp_yyyymmdd", "data_path", "wiki", "loglevel"]
    parser = argconfig.get_argparser(description, arg_names)

    args = parser.parse_args()
    logging.basicConfig(level=args.loglevel)
    logger.info(f"args={args}")

    main(args.wp_yyyymmdd, data_path=args.data_path, wiki=args.wiki)

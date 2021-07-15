# Copyright 2021-present Kensho Technologies, LLC.
import logging
from typing import List

from kwnlp_preprocessor import argconfig
from kwnlp_preprocessor import task_00_download_raw_dumps
from kwnlp_preprocessor import task_03p1_create_kwnlp_pagecounts
from kwnlp_preprocessor import task_03p2_convert_sql_to_csv
from kwnlp_preprocessor import task_06p1_create_kwnlp_page_props
from kwnlp_preprocessor import task_06p2_create_kwnlp_redirect_it2
from kwnlp_preprocessor import task_09p1_create_kwnlp_ultimate_redirect
from kwnlp_preprocessor import task_12p1_create_kwnlp_title_mapper
from kwnlp_preprocessor import task_15p1_split_and_compress_wikidata
from kwnlp_preprocessor import task_18p1_filter_wikidata_dump
from kwnlp_preprocessor import task_21p1_gather_wikidata_chunks
from kwnlp_preprocessor import task_24p1_create_kwnlp_article_pre
from kwnlp_preprocessor import task_27p1_parse_wikitext
from kwnlp_preprocessor import task_30p1_post_process_link_chunks
from kwnlp_preprocessor import task_33p1_collect_post_processed_link_data
from kwnlp_preprocessor import task_36p1_collect_template_data
from kwnlp_preprocessor import task_36p2_collect_length_data
from kwnlp_preprocessor import task_39p1_create_kwnlp_article
from kwnlp_preprocessor import task_42p1_collect_section_names


logger = logging.getLogger(__name__)


def main(
    wp_yyyymmdd: str,
    wd_yyyymmdd: str,
    data_path: str = argconfig.DEFAULT_KWNLP_DATA_PATH,
    wiki: str = argconfig.DEFAULT_KWNLP_WIKI,
    mirror_url: str = argconfig.DEFAULT_KWNLP_WIKI_MIRROR_URL,
    jobs_to_download: List[str] = argconfig.DEFAULT_KWNLP_DOWNLOAD_JOBS.split(","),
    max_entities: int = argconfig.DEFAULT_KWNLP_MAX_ENTITIES,
    workers: int = argconfig.DEFAULT_KWNLP_WORKERS,
) -> None:

    task_00_download_raw_dumps.main(
        wp_yyyymmdd,
        wd_yyyymmdd,
        data_path=data_path,
        mirror_url=mirror_url,
        wiki=wiki,
        jobs_to_download=jobs_to_download,
    )
    task_03p1_create_kwnlp_pagecounts.main(wp_yyyymmdd, data_path=data_path, wiki=wiki)
    task_03p2_convert_sql_to_csv.main(wp_yyyymmdd, data_path=data_path, wiki=wiki)
    task_06p1_create_kwnlp_page_props.main(wp_yyyymmdd, data_path=data_path, wiki=wiki)
    task_06p2_create_kwnlp_redirect_it2.main(wp_yyyymmdd, data_path=data_path, wiki=wiki)
    task_09p1_create_kwnlp_ultimate_redirect.main(wp_yyyymmdd, data_path=data_path, wiki=wiki)
    task_12p1_create_kwnlp_title_mapper.main(wp_yyyymmdd, data_path=data_path, wiki=wiki)
    task_15p1_split_and_compress_wikidata.main(
        wd_yyyymmdd, data_path=data_path, max_entities=max_entities
    )
    task_18p1_filter_wikidata_dump.main(
        wd_yyyymmdd,
        data_path=data_path,
        wiki=wiki,
        workers=workers,
        max_entities=max_entities,
    )
    task_21p1_gather_wikidata_chunks.main(wd_yyyymmdd, data_path=data_path)
    task_24p1_create_kwnlp_article_pre.main(
        wp_yyyymmdd, wd_yyyymmdd, data_path=data_path, wiki=wiki
    )
    task_27p1_parse_wikitext.main(
        wp_yyyymmdd,
        data_path=data_path,
        wiki=wiki,
        workers=workers,
        max_entities=max_entities,
    )
    task_30p1_post_process_link_chunks.main(
        wp_yyyymmdd, data_path=data_path, wiki=wiki, workers=workers
    )
    task_33p1_collect_post_processed_link_data.main(wp_yyyymmdd, data_path=data_path, wiki=wiki)
    task_36p1_collect_template_data.main(wp_yyyymmdd, data_path=data_path, wiki=wiki)
    task_36p2_collect_length_data.main(wp_yyyymmdd, data_path=data_path, wiki=wiki)
    task_39p1_create_kwnlp_article.main(wp_yyyymmdd, data_path=data_path, wiki=wiki)
    task_42p1_collect_section_names.main(wp_yyyymmdd, data_path=data_path, wiki=wiki)


if __name__ == "__main__":

    description = "run all wikimedia ingestion tasks"
    arg_names = [
        "wp_yyyymmdd",
        "wd_yyyymmdd",
        "data_path",
        "mirror_url",
        "wiki",
        "jobs",
        "max_entities",
        "workers",
        "loglevel",
    ]
    parser = argconfig.get_argparser(description, arg_names)

    args = parser.parse_args()
    logging.basicConfig(level=args.loglevel)
    logger.info(f"args={args}")
    jobs_to_download = argconfig.list_from_comma_delimited_string(args.jobs)

    main(
        args.wp_yyyymmdd,
        args.wd_yyyymmdd,
        data_path=args.data_path,
        mirror_url=args.mirror_url,
        wiki=args.wiki,
        jobs_to_download=jobs_to_download,
        max_entities=args.max_entities,
        workers=args.workers,
    )

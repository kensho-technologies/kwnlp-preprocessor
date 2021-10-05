# Copyright 2021-present Kensho Technologies, LLC.
"""Download raw wikimedia data."""
import logging
from typing import List

from kwnlp_preprocessor.argconfig import (
    DEFAULT_KWNLP_DATA_PATH,
    DEFAULT_KWNLP_WIKI_MIRROR_URL,
    DEFAULT_KWNLP_WIKI,
    DEFAULT_KWNLP_DOWNLOAD_JOBS,
    get_argparser,
    list_from_comma_delimited_string,
)
from kwnlp_dump_downloader.downloader import download_jobs


logger = logging.getLogger(__name__)


def main(
    wp_yyyymmdd: str,
    wd_yyyymmdd: str,
    data_path: str = DEFAULT_KWNLP_DATA_PATH,
    mirror_url: str = DEFAULT_KWNLP_WIKI_MIRROR_URL,
    wiki: str = DEFAULT_KWNLP_WIKI,
    jobs_to_download: List[str] = DEFAULT_KWNLP_DOWNLOAD_JOBS.split(","),
) -> None:

    download_jobs(
        wp_yyyymmdd,
        wd_yyyymmdd,
        data_path=data_path,
        mirror_url=mirror_url,
        wiki=wiki,
        jobs_to_download=jobs_to_download,
    )


if __name__ == "__main__":

    description = "download wikimedia dumps"
    arg_names = [
        "wp_yyyymmdd",
        "wd_yyyymmdd",
        "data_path",
        "mirror_url",
        "wiki",
        "jobs",
        "loglevel",
    ]
    parser = get_argparser(description, arg_names)

    args = parser.parse_args()
    logging.basicConfig(level=args.loglevel)
    logger.info(f"args={args}")
    jobs_to_download = list_from_comma_delimited_string(args.jobs)
    logger.info(f"jobs_to_download={jobs_to_download}")

    main(
        args.wp_yyyymmdd,
        args.wd_yyyymmdd,
        data_path=args.data_path,
        mirror_url=args.mirror_url,
        wiki=args.wiki,
        jobs_to_download=jobs_to_download,
    )

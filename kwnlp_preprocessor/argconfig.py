# Copyright 2021-present Kensho Technologies, LLC.
import argparse
import logging
import multiprocessing
import sys
from typing import Dict, List

DEFAULT_KWNLP_DATA_PATH: str = ""
DEFAULT_KWNLP_WIKI_MIRROR_URL: str = "https://dumps.wikimedia.org"
DEFAULT_KWNLP_WIKI: str = "enwiki"
DEFAULT_KWNLP_DOWNLOAD_JOBS: str = (
    "pageviewcomplete,pagetable,pagepropstable,redirecttable,articlesdump,wikidata"
)
DEFAULT_KWNLP_LOGGING_LEVEL: int = logging.INFO
DEFAULT_KWNLP_MAX_ENTITIES: int = sys.maxsize
DEFAULT_KWNLP_WORKERS = multiprocessing.cpu_count() - 1


ap_wp_yyyymmdd = argparse.ArgumentParser(add_help=False)
ap_wp_yyyymmdd.add_argument("wp_yyyymmdd", help="date string for Wikipedia dump (e.g. 20200920)")

ap_wd_yyyymmdd = argparse.ArgumentParser(add_help=False)
ap_wd_yyyymmdd.add_argument("wd_yyyymmdd", help="date string for Wikidata dump (e.g. 20200921)")

ap_data_path = argparse.ArgumentParser(add_help=False)
ap_data_path.add_argument(
    "--data_path",
    default=DEFAULT_KWNLP_DATA_PATH,
    help="path to top level data directory (e.g. /data/wikimedia-ingestion)",
)

ap_mirror_url = argparse.ArgumentParser(add_help=False)
ap_mirror_url.add_argument(
    "--mirror_url",
    default=DEFAULT_KWNLP_WIKI_MIRROR_URL,
    help="base URL for Wikimedia dumps (e.g. https://dumps.wikimedia.org)",
)

ap_wiki = argparse.ArgumentParser(add_help=False)
ap_wiki.add_argument(
    "--wiki",
    default=DEFAULT_KWNLP_WIKI,
    help="selects which language wikipedia to use (e.g. enwiki)",
)

ap_jobs = argparse.ArgumentParser(add_help=False)
ap_jobs.add_argument(
    "--jobs",
    default=DEFAULT_KWNLP_DOWNLOAD_JOBS,
    help="comma separated list of job names to download (e.g. pagecounts,pagetable)",
)

ap_max_entities = argparse.ArgumentParser(add_help=False)
ap_max_entities.add_argument(
    "--max-entities",
    default=DEFAULT_KWNLP_MAX_ENTITIES,
    help="maximum number of entities to parse from each file (for debugging)",
    type=int,
)

ap_workers = argparse.ArgumentParser(add_help=False)
ap_workers.add_argument(
    "--workers",
    default=DEFAULT_KWNLP_WORKERS,
    help="number of parallel workers to use",
    type=int,
)

ap_loglevel = argparse.ArgumentParser(add_help=False)
ap_loglevel.add_argument(
    "--loglevel",
    default=DEFAULT_KWNLP_LOGGING_LEVEL,
    help="python logging level integer (e.g. 20)",
    type=int,
)

ap_include_item_statements = argparse.ArgumentParser(add_help=False)
ap_include_item_statements.add_argument(
    "--include_item_statements",
    default=False,
    help="whether to produce the wikidata item statements output",
    type=bool,
)


ARGS: Dict[str, argparse.ArgumentParser] = {
    "wp_yyyymmdd": ap_wp_yyyymmdd,
    "wd_yyyymmdd": ap_wd_yyyymmdd,
    "data_path": ap_data_path,
    "mirror_url": ap_mirror_url,
    "wiki": ap_wiki,
    "jobs": ap_jobs,
    "max_entities": ap_max_entities,
    "workers": ap_workers,
    "loglevel": ap_loglevel,
    "include_item_statements": ap_include_item_statements,
}


def list_from_comma_delimited_string(comma_delimited_string: str) -> List[str]:
    return [el.strip() for el in comma_delimited_string.strip().split(",")]


def get_argparser(description: str, arg_names: List[str]) -> argparse.ArgumentParser:
    parents = [ARGS[arg_name] for arg_name in arg_names]
    parser = argparse.ArgumentParser(description=description, parents=parents)
    return parser

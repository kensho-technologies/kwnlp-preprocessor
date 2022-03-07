# Copyright 2021-present Kensho Technologies, LLC.
"""Split Wikidata dump into many chunks.

Split single compressed wikidata dump file into many compressed chunks.
Chunks contain a fixed number of lines (i.e. wikidata entities).
Splitting is handled in the main thread and re-compression is handled by
multiple subprocesses.
"""
import logging
import os
import subprocess
from typing import Iterable

import funcy
from qwikidata.json_dump import WikidataJsonDump

from kwnlp_preprocessor import argconfig

logger = logging.getLogger(__name__)


CHUNK_SIZE = 500_000


def _count_unfinished_bzip_jobs(bzip_jobs: Iterable[subprocess.Popen]) -> int:
    return sum([1 if bzip_job.poll() is None else 0 for bzip_job in bzip_jobs])


def main(
    wd_yyyymmdd: str,
    data_path: str = argconfig.DEFAULT_KWNLP_DATA_PATH,
    max_entities: int = argconfig.DEFAULT_KWNLP_MAX_ENTITIES,
) -> None:

    in_dump_path = os.path.join(
        data_path,
        f"wikidata-raw-{wd_yyyymmdd}",
        f"wikidata-{wd_yyyymmdd}-all.json.bz2",
    )

    out_dump_dir = os.path.join(
        data_path,
        f"wikidata-raw-chunks-{wd_yyyymmdd}",
    )

    logger.info(f"in_dump_path: {in_dump_path}")
    os.makedirs(out_dump_dir, exist_ok=True)
    logger.info(f"out_dump_dir: {out_dump_dir}")

    bzip_jobs = []
    wjd = WikidataJsonDump(in_dump_path)
    num_lines_written = 0
    for ii_chunk, line_buffer in enumerate(funcy.chunks(CHUNK_SIZE, wjd.iter_lines())):

        line_buffer = [line.rstrip(",\n") for line in line_buffer]
        if line_buffer[0] == "[":
            line_buffer = line_buffer[1:]
        if line_buffer[-1] == "]":
            line_buffer = line_buffer[:-1]

        out_file_path = os.path.join(
            out_dump_dir, f"wikidata-{wd_yyyymmdd}-chunk-{ii_chunk:0>4d}.jsonl"
        )
        logger.info(f"writing chunk {ii_chunk} to {out_file_path}")
        with open(out_file_path, "w") as fp:
            fp.write("".join(["{}\n".format(ll) for ll in line_buffer]))
        bzip_jobs.append(subprocess.Popen(["bzip2", out_file_path]))
        logger.info(
            "currently running bzip2 on {} files".format(_count_unfinished_bzip_jobs(bzip_jobs))
        )

        num_lines_written += len(line_buffer)
        if num_lines_written >= max_entities:
            logger.info(f"wrote {num_lines_written}. stopping b/c max_entities={max_entities}")
            break

    logger.info(
        "waiting for {} bzip2 jobs to finish".format(_count_unfinished_bzip_jobs(bzip_jobs))
    )
    for bzip_job in bzip_jobs:
        bzip_job.wait()


if __name__ == "__main__":

    description = "split JSON wikidata dump"
    arg_names = ["wd_yyyymmdd", "data_path", "max_entities", "loglevel"]
    parser = argconfig.get_argparser(description, arg_names)

    args = parser.parse_args()
    logging.basicConfig(level=args.loglevel)
    logger.info(f"args={args}")

    main(args.wd_yyyymmdd, data_path=args.data_path, max_entities=args.max_entities)

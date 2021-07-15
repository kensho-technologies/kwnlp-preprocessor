# Copyright 2021-present Kensho Technologies, LLC.
"""Parse chunked XML article dump into plaintext and links.

Can do all chunks at once with a machine with 64 cores and 256G RAM
"""
import bz2
from contextlib import ExitStack
import copy
import json
import logging
from multiprocessing import get_context
import os
import pandas as pd
import re
from typing import Dict, Pattern

import mwtext
import mwxml

from kwnlp_preprocessor import argconfig
from kwnlp_preprocessor import utils


logger = logging.getLogger(__name__)


FORBIDDEN_WIKILINK_PREFIXES = frozenset(
    [
        "category",
        "file",
        "image",
    ]
)

TEMPLATE_PATTERNS = {
    "good_article": re.compile(r"{{good article}}", flags=re.IGNORECASE),
    "featured_article": re.compile(r"{{featured article}}", flags=re.IGNORECASE),
    "pseudoscience": re.compile(r"{{pseudoscience(\|.*?)?}}", flags=re.IGNORECASE),
    "conspiracy_theories": re.compile(r"{{conspiracy(_| )theories(\|.*?)?}}", flags=re.IGNORECASE),
}


def _get_link_annotated_text_from_page(
    page: mwxml.Page, revision: mwxml.Revision, transformer: mwtext.Wikitext2Structured
) -> Dict:

    structured = transformer.transform(revision.text)
    link_annotated_text = {
        "page_id": page.id,
        "revision_id": revision.id,
        "page_title": page.title[0].upper() + page.title[1:].replace(" ", "_"),
        "paragraphs": structured["paragraphs"],
        "categories": structured["categories"],
    }
    return link_annotated_text


def _get_links_from_link_annotated_text(compressed_link_annotated_text: Dict) -> pd.DataFrame:
    dfd: Dict = {
        "source_page_id": [],
        "section_idx": [],
        "paragraph_idx": [],
        "anchor_text": [],
        "anchor_start": [],
        "target_page_id": [],
    }
    for paragraph_idx, paragraph in enumerate(compressed_link_annotated_text["paragraphs"]):
        for target_page_id, anchor_span in zip(
            paragraph["target_page_ids"], paragraph["anchor_spans"]
        ):
            anchor_text = paragraph["plaintext"][anchor_span[0] : anchor_span[1]]
            dfd["source_page_id"].append(compressed_link_annotated_text["page_id"])
            dfd["section_idx"].append(paragraph["section_idx"])
            dfd["paragraph_idx"].append(paragraph_idx)
            dfd["anchor_text"].append(anchor_text)
            dfd["anchor_start"].append(anchor_span[0])
            dfd["target_page_id"].append(target_page_id)
    df = pd.DataFrame(dfd)
    return df


def _get_paragraphs_from_link_annotated_text(link_annotated_text: Dict) -> pd.DataFrame:
    dfd: Dict = {
        "page_id": [],
        "section_idx": [],
        "paragraph_idx": [],
        "plaintext": [],
    }
    for paragraph_idx, paragraph in enumerate(link_annotated_text["paragraphs"]):
        dfd["page_id"].append(link_annotated_text["page_id"])
        dfd["section_idx"].append(paragraph["section_idx"])
        dfd["paragraph_idx"].append(paragraph_idx)
        dfd["plaintext"].append(paragraph["plaintext"])
    df = pd.DataFrame(dfd)
    return df


def _get_section_names_from_link_annotated_text(link_annotated_text: Dict) -> pd.DataFrame:
    dfd: Dict = {
        "page_id": [],
        "section_idx": [],
        "section_name": [],
    }
    for paragraph_idx, paragraph in enumerate(link_annotated_text["paragraphs"]):
        for wikilink in paragraph["wikilinks"]:
            dfd["page_id"].append(link_annotated_text["page_id"])
            dfd["section_idx"].append(paragraph["section_idx"])
            dfd["section_name"].append(paragraph["section_name"])
    df = pd.DataFrame(dfd)
    return df.drop_duplicates()


def _create_compressed_link_annotated_text(link_annotated_text: Dict, title_id_map: Dict) -> Dict:
    compressed_link_annotated_text = copy.deepcopy(link_annotated_text)
    for paragraph in compressed_link_annotated_text["paragraphs"]:
        target_page_ids = []
        anchor_spans = []
        for wikilink in paragraph["wikilinks"]:
            target_page_title = wikilink[0]
            wikilink[1]
            anchor_offset_start = wikilink[2]
            anchor_offset_end = wikilink[3]
            target_page_id = int(title_id_map.get(target_page_title, -1))
            if target_page_id == -1:
                pass  # no match in title map
            else:
                target_page_ids.append(target_page_id)
                anchor_spans.append([anchor_offset_start, anchor_offset_end])
        paragraph["target_page_ids"] = target_page_ids
        paragraph["anchor_spans"] = anchor_spans
        del paragraph["wikilinks"]
    return compressed_link_annotated_text


def _get_templates_from_page(
    page: mwxml.Page, revision: mwxml.Revision, template_patterns: Dict[str, Pattern]
) -> pd.DataFrame:

    res = pd.DataFrame(
        {
            name: [int(re.search(pattern, revision.text) is not None)]
            for name, pattern in template_patterns.items()
        }
    )
    res["page_id"] = page.id
    res = res[["page_id"] + list(template_patterns.keys())]
    return res


def _get_lengths_from_link_annotated_text(
    page: mwxml.Page, link_annotated_text: Dict
) -> pd.DataFrame:

    paragraph_lengths = [
        len(paragraph["plaintext"]) for paragraph in link_annotated_text["paragraphs"]
    ]
    len_article = sum(paragraph_lengths)
    len_intro = paragraph_lengths[0] if paragraph_lengths else 0
    res = pd.DataFrame(
        {
            "page_id": [page.id],
            "len_article_chars": [len_article],
            "len_intro_chars": [len_intro],
        }
    )
    return res


def parse_file(args: Dict) -> None:

    logger.info("parsing {}".format(args["wikitext_file_path"]))
    df_title_mapper = pd.read_csv(
        args["title_mapper_file_path"], usecols=["source_title", "target_id"]
    )
    title_id_map = {
        title: tid
        for title, tid in zip(
            df_title_mapper["source_title"].values,
            df_title_mapper["target_id"].values,
        )
    }
    del df_title_mapper

    transformer = mwtext.Wikitext2Structured(
        forbidden_wikilink_prefixes=FORBIDDEN_WIKILINK_PREFIXES,
    )
    dump = mwxml.Dump.from_file(bz2.open(args["wikitext_file_path"]))
    pages_written = 0
    with ExitStack() as exit_stack:
        lat_fp = exit_stack.enter_context(open(args["lat_file_path"], "w"))
        lnk_fp = exit_stack.enter_context(open(args["lnk_file_path"], "w"))
        par_fp = exit_stack.enter_context(open(args["par_file_path"], "w"))
        sct_fp = exit_stack.enter_context(open(args["sct_file_path"], "w"))
        tmp_fp = exit_stack.enter_context(open(args["tmp_file_path"], "w"))
        len_fp = exit_stack.enter_context(open(args["len_file_path"], "w"))

        write_header = True
        for page_idx, page in enumerate(dump):

            if page.namespace != 0 or page.redirect:
                continue

            revisions = list(page)
            assert len(revisions) == 1
            revision = revisions[0]
            if not isinstance(revision.text, str):
                continue

            templates = _get_templates_from_page(page, revision, TEMPLATE_PATTERNS)
            link_annotated_text = _get_link_annotated_text_from_page(page, revision, transformer)
            compressed_link_annotated_text = _create_compressed_link_annotated_text(
                link_annotated_text, title_id_map
            )
            links = _get_links_from_link_annotated_text(compressed_link_annotated_text)
            paragraphs = _get_paragraphs_from_link_annotated_text(link_annotated_text)
            section_names = _get_section_names_from_link_annotated_text(link_annotated_text)
            lengths = _get_lengths_from_link_annotated_text(page, link_annotated_text)

            lat_fp.write("{}\n".format(json.dumps(compressed_link_annotated_text)))
            links.to_csv(lnk_fp, mode="a", header=write_header, index=False)
            paragraphs.to_csv(par_fp, mode="a", header=write_header, index=False)
            section_names.to_csv(sct_fp, mode="a", header=write_header, index=False)
            templates.to_csv(tmp_fp, mode="a", header=write_header, index=False)
            lengths.to_csv(len_fp, mode="a", header=write_header, index=False)
            if write_header:
                write_header = False

            pages_written += 1
            if pages_written >= args["max_entities"]:
                return

    logger.info("finished {}".format(args["wikitext_file_path"]))


def main(
    wp_yyyymmdd: str,
    data_path: str = argconfig.DEFAULT_KWNLP_DATA_PATH,
    wiki: str = argconfig.DEFAULT_KWNLP_WIKI,
    workers: int = argconfig.DEFAULT_KWNLP_WORKERS,
    max_entities: int = argconfig.DEFAULT_KWNLP_MAX_ENTITIES,
) -> None:

    in_dump_paths: Dict[str, str] = {
        "wikitext": os.path.join(data_path, f"wikipedia-raw-{wp_yyyymmdd}", "articlesdump"),
        "title-mapper": os.path.join(
            data_path,
            f"wikipedia-derived-{wp_yyyymmdd}",
            "kwnlp-sql",
            f"kwnlp-{wiki}-{wp_yyyymmdd}-title-mapper.csv",
        ),
    }

    out_dump_paths: Dict[str, str] = {
        "lat": os.path.join(
            data_path, f"wikipedia-derived-{wp_yyyymmdd}", "link-annotated-text-chunks"
        ),
        "lnk": os.path.join(data_path, f"wikipedia-derived-{wp_yyyymmdd}", "links-chunks"),
        "par": os.path.join(data_path, f"wikipedia-derived-{wp_yyyymmdd}", "paragraphs-chunks"),
        "sct": os.path.join(
            data_path,
            f"wikipedia-derived-{wp_yyyymmdd}",
            "section-names-chunks",
        ),
        "tmp": os.path.join(
            data_path,
            f"wikipedia-derived-{wp_yyyymmdd}",
            "templates-chunks",
        ),
        "len": os.path.join(
            data_path,
            f"wikipedia-derived-{wp_yyyymmdd}",
            "lengths-chunks",
        ),
    }

    for name, path in in_dump_paths.items():
        logger.info(f"{name} path: {path}")

    for name, path in out_dump_paths.items():
        os.makedirs(path, exist_ok=True)
        logger.info(f"{name} path: {path}")

    pattern = re.compile(wiki + r"-\d{8}-pages-articles(\d{1,2}).xml-p(\d+)p(\d+)\.bz2")
    wikitext_file_names = [
        match.string
        for match in utils._get_ordered_files_from_path(in_dump_paths["wikitext"], pattern)
    ]

    mp_args = []
    for wikitext_file_name in wikitext_file_names:
        wikitext_file_path = os.path.join(in_dump_paths["wikitext"], wikitext_file_name)
        out_file_base = "kwnlp-" + wikitext_file_name.replace(".xml", "").replace(".bz2", "")

        lat_file_name = out_file_base.replace("pages-articles", "link-annotated-text") + ".jsonl"
        lat_file_path = os.path.join(out_dump_paths["lat"], lat_file_name)

        lnk_file_name = out_file_base.replace("pages-articles", "links") + ".csv"
        lnk_file_path = os.path.join(out_dump_paths["lnk"], lnk_file_name)

        par_file_name = out_file_base.replace("pages-articles", "paragraphs") + ".csv"
        par_file_path = os.path.join(out_dump_paths["par"], par_file_name)

        sct_file_name = out_file_base.replace("pages-articles", "section-names") + ".csv"
        sct_file_path = os.path.join(out_dump_paths["sct"], sct_file_name)

        tmp_file_name = out_file_base.replace("pages-articles", "templates") + ".csv"
        tmp_file_path = os.path.join(out_dump_paths["tmp"], tmp_file_name)

        len_file_name = out_file_base.replace("pages-articles", "lengths") + ".csv"
        len_file_path = os.path.join(out_dump_paths["len"], len_file_name)

        mp_args.append(
            {
                "wikitext_file_path": wikitext_file_path,
                "title_mapper_file_path": in_dump_paths["title-mapper"],
                "lat_file_path": lat_file_path,
                "lnk_file_path": lnk_file_path,
                "par_file_path": par_file_path,
                "sct_file_path": sct_file_path,
                "tmp_file_path": tmp_file_path,
                "len_file_path": len_file_path,
                "max_entities": max_entities,
            }
        )

    with get_context("spawn").Pool(workers) as p:
        p.map(parse_file, mp_args)


if __name__ == "__main__":

    description = "parse wikitext"
    arg_names = [
        "wp_yyyymmdd",
        "data_path",
        "wiki",
        "workers",
        "max_entities",
        "loglevel",
    ]
    parser = argconfig.get_argparser(description, arg_names)

    args = parser.parse_args()
    logging.basicConfig(level=args.loglevel)
    logger.info(f"args={args}")

    main(
        args.wp_yyyymmdd,
        data_path=args.data_path,
        wiki=args.wiki,
        workers=args.workers,
        max_entities=args.max_entities,
    )

# Copyright 2021-present Kensho Technologies, LLC.
import bz2
from contextlib import ExitStack
import csv
import json
import logging
from multiprocessing import Pool
import os
import re
from typing import Dict

from qwikidata.entity import WikidataItem, WikidataProperty

from kwnlp_preprocessor import argconfig, utils

logger = logging.getLogger(__name__)


SKIP_INSTANCES_OF_NQID = frozenset(
    [
        13442814,  # scholarly article
    ]
)

RANK_TO_INT = {"deprecated": 2, "normal": 1, "preferred": 0}


def parse_file(args: Dict) -> None:

    logger.info("input: {}".format(args["wikidata_file_path"]))

    # get file pointers, csv writers, and write headers
    # ============================================================
    with ExitStack() as exit_stack:
        wkd_fp = exit_stack.enter_context(bz2.open(args["wikidata_file_path"], "r"))

        article_file_path = os.path.join(
            args["data_path"],
            "wikidata-derived-{}".format(args["wd_yyyymmdd"]),
            "{}-article-chunks".format(args["wiki"]),
            "kwnlp-{}-{}-article.jsonl".format(args["out_file_base"], args["wiki"]),
        )
        os.makedirs(os.path.dirname(article_file_path), exist_ok=True)
        article_fp = exit_stack.enter_context(open(article_file_path, "w"))

        p279_file_path = os.path.join(
            args["data_path"],
            "wikidata-derived-{}".format(args["wd_yyyymmdd"]),
            "p279-claim-chunks",
            "kwnlp-{}-p279-claim.csv".format(args["out_file_base"]),
        )
        os.makedirs(os.path.dirname(p279_file_path), exist_ok=True)
        p279_fp = exit_stack.enter_context(open(p279_file_path, "w"))
        fieldnames = ["source_id", "target_id", "rnk"]
        p279_writer = csv.DictWriter(p279_fp, fieldnames=fieldnames)
        p279_writer.writeheader()

        p31_file_path = os.path.join(
            args["data_path"],
            "wikidata-derived-{}".format(args["wd_yyyymmdd"]),
            "p31-claim-chunks",
            "kwnlp-{}-p31-claim.csv".format(args["out_file_base"]),
        )
        os.makedirs(os.path.dirname(p31_file_path), exist_ok=True)
        p31_fp = exit_stack.enter_context(open(p31_file_path, "w"))
        fieldnames = ["source_id", "target_id", "rnk"]
        p31_writer = csv.DictWriter(p31_fp, fieldnames=fieldnames)
        p31_writer.writeheader()

        qpq_file_path = os.path.join(
            args["data_path"],
            "wikidata-derived-{}".format(args["wd_yyyymmdd"]),
            "qpq-claim-chunks",
            "kwnlp-{}-qpq-claim.csv".format(args["out_file_base"]),
        )
        os.makedirs(os.path.dirname(qpq_file_path), exist_ok=True)
        qpq_fp = exit_stack.enter_context(open(qpq_file_path, "w"))
        fieldnames = ["source_id", "property_id", "target_id", "rnk"]
        qpq_writer = csv.DictWriter(qpq_fp, fieldnames=fieldnames)
        qpq_writer.writeheader()

        item_file_path = os.path.join(
            args["data_path"],
            "wikidata-derived-{}".format(args["wd_yyyymmdd"]),
            "item-chunks",
            "kwnlp-{}-item.csv".format(args["out_file_base"]),
        )
        os.makedirs(os.path.dirname(item_file_path), exist_ok=True)
        item_fp = exit_stack.enter_context(open(item_file_path, "w"))
        fieldnames = ["item_id", "en_label", "en_description"]
        item_writer = csv.DictWriter(item_fp, fieldnames=fieldnames)
        item_writer.writeheader()

        item_alias_file_path = os.path.join(
            args["data_path"],
            "wikidata-derived-{}".format(args["wd_yyyymmdd"]),
            "item-alias-chunks",
            "kwnlp-{}-item-alias.csv".format(args["out_file_base"]),
        )
        os.makedirs(os.path.dirname(item_alias_file_path), exist_ok=True)
        item_alias_fp = exit_stack.enter_context(open(item_alias_file_path, "w"))
        fieldnames = ["item_id", "en_alias"]
        item_alias_writer = csv.DictWriter(item_alias_fp, fieldnames=fieldnames)
        item_alias_writer.writeheader()

        if args["include_item_statements"]:
            item_statements_file_path = os.path.join(
                args["data_path"],
                "wikidata-derived-{}".format(args["wd_yyyymmdd"]),
                "item-statements-chunks",
                "kwnlp-{}-item-statements.csv".format(args["out_file_base"]),
            )
            os.makedirs(os.path.dirname(item_statements_file_path), exist_ok=True)
            item_statements_fp = exit_stack.enter_context(open(item_statements_file_path, "w"))
            fieldnames = [
                "statement_id",
                "mainsnak_datatype",
                "datavalue_datatype",
                "source_item_id",
                "edge_property_id",
                "target_datavalue",
            ]
            item_statements_writer = csv.DictWriter(item_statements_fp, fieldnames=fieldnames)
            item_statements_writer.writeheader()

        property_file_path = os.path.join(
            args["data_path"],
            "wikidata-derived-{}".format(args["wd_yyyymmdd"]),
            "property-chunks",
            "kwnlp-{}-property.csv".format(args["out_file_base"]),
        )
        os.makedirs(os.path.dirname(property_file_path), exist_ok=True)
        property_fp = exit_stack.enter_context(open(property_file_path, "w"))
        fieldnames = ["property_id", "en_label", "en_description"]
        property_writer = csv.DictWriter(property_fp, fieldnames=fieldnames)
        property_writer.writeheader()

        property_alias_file_path = os.path.join(
            args["data_path"],
            "wikidata-derived-{}".format(args["wd_yyyymmdd"]),
            "property-alias-chunks",
            "kwnlp-{}-property-alias.csv".format(args["out_file_base"]),
        )
        os.makedirs(os.path.dirname(property_alias_file_path), exist_ok=True)
        property_alias_fp = exit_stack.enter_context(open(property_alias_file_path, "w"))
        fieldnames = ["property_id", "en_alias"]
        property_alias_writer = csv.DictWriter(property_alias_fp, fieldnames=fieldnames)
        property_alias_writer.writeheader()

        skipped_file_path = os.path.join(
            args["data_path"],
            "wikidata-derived-{}".format(args["wd_yyyymmdd"]),
            "skipped-entity-chunks",
            "kwnlp-{}-skipped-entity.csv".format(args["out_file_base"]),
        )
        os.makedirs(os.path.dirname(skipped_file_path), exist_ok=True)
        skipped_fp = exit_stack.enter_context(open(skipped_file_path, "w"))
        fieldnames = ["qid", "instances_of"]
        skipped_writer = csv.DictWriter(skipped_fp, fieldnames=fieldnames)
        skipped_writer.writeheader()

        # parse file
        # ============================================================
        entities_parsed = 0

        for line in wkd_fp:
            entities_parsed += 1
            entity_dict = json.loads(line)

            if entity_dict["type"] == "property":
                wd_entity = WikidataProperty(entity_dict)
                source_id = wd_entity.entity_id[1:]

                # write label and description
                # ---------------------------------------------------------
                property_writer.writerow(
                    {
                        "property_id": source_id,
                        "en_label": wd_entity.get_label(lang="en"),
                        "en_description": wd_entity.get_description(lang="en"),
                    }
                )

                # write aliases
                # ---------------------------------------------------------
                property_alias_writer.writerows(
                    [
                        {
                            "property_id": source_id,
                            "en_alias": alias,
                        }
                        for alias in wd_entity.get_aliases(lang="en")
                    ]
                )

            elif entity_dict["type"] == "item":

                wd_entity = WikidataItem(entity_dict)
                source_id = wd_entity.entity_id[1:]

                # get P31 (instance of) claims
                # ---------------------------------------------------------
                p31_rows = [
                    {
                        "source_id": source_id,
                        "target_id": claim.mainsnak.datavalue.value["numeric-id"],
                        "rnk": RANK_TO_INT[claim.rank],
                    }
                    for claim in wd_entity.get_claim_group("P31")
                    if claim.mainsnak.snaktype == "value" and claim.rank != "deprecated"
                ]

                # check if we want to skip this item
                # ---------------------------------------------------------
                p31_nqids = set([row["target_id"] for row in p31_rows])
                skip_intersection = p31_nqids & SKIP_INSTANCES_OF_NQID  # intersection
                if len(skip_intersection) > 0:
                    row = {
                        "qid": source_id,
                        "instances_of": "|".join([str(el) for el in skip_intersection]),
                    }
                    skipped_writer.writerow(row)
                    continue

                # start writing if we're keeping
                # ---------------------------------------------------------
                for row in p31_rows:
                    p31_writer.writerow(row)

                # get P279 (subclass of) claims
                # ---------------------------------------------------------
                p279_rows = [
                    {
                        "source_id": source_id,
                        "target_id": claim.mainsnak.datavalue.value["numeric-id"],
                        "rnk": RANK_TO_INT[claim.rank],
                    }
                    for claim in wd_entity.get_claim_group("P279")
                    if claim.mainsnak.snaktype == "value" and claim.rank != "deprecated"
                ]
                for row in p279_rows:
                    p279_writer.writerow(row)

                # qpq operations
                # ---------------------------------------------------------
                for (
                    claim_id_str,
                    claim_group,
                ) in wd_entity.get_truthy_claim_groups().items():
                    qpq_rows = [
                        {
                            "source_id": source_id,
                            "property_id": claim_id_str[1:],
                            "target_id": claim.mainsnak.datavalue.value["numeric-id"],
                            "rnk": RANK_TO_INT[claim.rank],
                        }
                        for claim in claim_group
                        if (
                            claim.mainsnak.snaktype == "value"
                            and claim.rank != "deprecated"
                            and claim.mainsnak.snak_datatype == "wikibase-item"
                        )
                    ]
                    for row in qpq_rows:
                        qpq_writer.writerow(row)

                # write label and description
                # ---------------------------------------------------------
                item_writer.writerow(
                    {
                        "item_id": source_id,
                        "en_label": wd_entity.get_label(lang="en"),
                        "en_description": wd_entity.get_description(lang="en"),
                    }
                )

                # write aliases
                # ---------------------------------------------------------
                item_alias_writer.writerows(
                    [
                        {
                            "item_id": source_id,
                            "en_alias": alias,
                        }
                        for alias in wd_entity.get_aliases(lang="en")
                    ]
                )

                # write statements
                # ---------------------------------------------------------
                if args["include_item_statements"]:
                    for (
                        claim_id_str,
                        claim_group,
                    ) in wd_entity.get_truthy_claim_groups().items():
                        statement_rows = [
                            {
                                "statement_id": f"{wd_entity.entity_id}-{claim_id_str}-{i}",
                                "mainsnak_datatype": claim.mainsnak.snak_datatype,
                                "datavalue_datatype": claim.mainsnak.value_datatype,
                                "source_item_id": wd_entity.entity_id[1:],
                                "edge_property_id": claim_id_str[1:],
                                # we must access private variable to faithfully replicate this field
                                "target_datavalue": json.dumps(
                                    claim.mainsnak.datavalue._datavalue_dict
                                ),
                            }
                            for i, claim in enumerate(claim_group)
                            if (claim.mainsnak.snaktype == "value" and claim.rank != "deprecated")
                        ]
                        for row in statement_rows:
                            item_statements_writer.writerow(row)

                # filter articles from chosen wiki
                # ---------------------------------------------------------
                if args["wiki"] in wd_entity.get_sitelinks():
                    article_fp.write("{}\n".format(json.dumps(entity_dict)))

            if entities_parsed >= args["max_entities"]:
                return


def main(
    wd_yyyymmdd: str,
    data_path: str = argconfig.DEFAULT_KWNLP_DATA_PATH,
    wiki: str = argconfig.DEFAULT_KWNLP_WIKI,
    workers: int = argconfig.DEFAULT_KWNLP_WORKERS,
    max_entities: int = argconfig.DEFAULT_KWNLP_MAX_ENTITIES,
    include_item_statements: bool = False,
) -> None:

    in_dump_paths = {
        "wikidata": os.path.join(
            data_path,
            f"wikidata-raw-chunks-{wd_yyyymmdd}",
        ),
    }

    for name, path in in_dump_paths.items():
        logger.info(f"{name} path: {path}")

    pattern = re.compile(r"wikidata-\d{8}-chunk-(\d{4}).json")
    all_wikidata_file_names = [
        match.string
        for match in utils._get_ordered_files_from_path(in_dump_paths["wikidata"], pattern)
    ]

    mp_args = []
    for wikidata_file_name in all_wikidata_file_names:
        wikidata_file_path = os.path.join(in_dump_paths["wikidata"], wikidata_file_name)
        out_file_base = wikidata_file_name.replace(".jsonl.bz2", "")
        mp_args.append(
            {
                "wiki": wiki,
                "data_path": data_path,
                "wd_yyyymmdd": wd_yyyymmdd,
                "wikidata_file_path": wikidata_file_path,
                "out_file_base": out_file_base,
                "max_entities": max_entities,
                "include_item_statements": include_item_statements,
            }
        )

    with Pool(workers) as p:
        p.map(parse_file, mp_args)


if __name__ == "__main__":

    description = "filter wikidata chunks"
    arg_names = [
        "wd_yyyymmdd",
        "data_path",
        "wiki",
        "workers",
        "max_entities",
        "loglevel",
        "include_item_statements",
    ]
    parser = argconfig.get_argparser(description, arg_names)

    args = parser.parse_args()
    logging.basicConfig(level=args.loglevel)
    logger.info(f"args={args}")

    main(
        args.wd_yyyymmdd,
        data_path=args.data_path,
        wiki=args.wiki,
        workers=args.workers,
        max_entities=args.max_entities,
        include_item_statements=args.include_item_statements,
    )

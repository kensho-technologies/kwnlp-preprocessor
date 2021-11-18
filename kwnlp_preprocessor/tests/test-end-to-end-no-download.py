# Copyright 2021-present Kensho Technologies, LLC.
import os.path
import filecmp
import logging

from kwnlp_preprocessor import argconfig
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
import unittest
import shutil
from tempfile import TemporaryDirectory
import os

logger = logging.getLogger(__name__)


def are_dir_trees_equal(output: str, truth: str) -> bool:
    """Compare two directories recursively.

    Files in each directory are assumed to be equal if their names and contents are equal.
    This is a modified version found from https://stackoverflow.com/a/6681395, it only cares that all the files in the truth are in the output, but not the other way around.
    The output will have extra files because there is intermediate data we dont
    care about comparing. The intermediate data are implementation details.

    @param output: Outputs from processing
    @param truth: Ground truth outputs

    @return: True if the directory trees are the same and
        there were no errors while accessing the directories or files,
        False otherwise.
    """
    dirs_cmp = filecmp.dircmp(output, truth)
    # If the truth data has files the output doesnt, we fail
    # funny_files: Files which are in both a and b, but could not be compared.
    if len(dirs_cmp.right_only) > 0 or len(dirs_cmp.funny_files) > 0:
        return False
    (_, mismatch, errors) = filecmp.cmpfiles(output, truth, dirs_cmp.common_files, shallow=False)
    if len(mismatch) > 0 or len(errors) > 0:
        return False
    for common_dir in dirs_cmp.common_dirs:
        new_output = os.path.join(output, common_dir)
        new_truth = os.path.join(truth, common_dir)
        if not are_dir_trees_equal(new_output, new_truth):
            return False
    return True


class TestEachStep(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = TemporaryDirectory()
        self.data_path = os.path.join(self.tmpdir.name, "data")
        input_data_path = os.path.join(os.path.dirname(__file__), "data/inputs")
        shutil.copytree(input_data_path, self.data_path)

    def test_all_no_download(self) -> None:
        wp_yyyymmdd = "20210701"
        wd_yyyymmdd = "20210705"
        wiki = argconfig.DEFAULT_KWNLP_WIKI
        max_entities = argconfig.DEFAULT_KWNLP_MAX_ENTITIES
        data_path = self.data_path
        workers = 1
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

        self.assertTrue(
            are_dir_trees_equal(self.data_path, "kwnlp_preprocessor/tests/data/outputs")
        )

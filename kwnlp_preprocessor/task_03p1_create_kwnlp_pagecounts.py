# Copyright 2021-present Kensho Technologies, LLC.
"""Filter monthly pageviews file to a single wiki and put in CSV format.

in: pageviews-YYYYMMDD-user.bz2
out: kwnlp-WIKI-YYYYMMDD-prior-month-pageviews-complete.csv
"""
import bz2
from calendar import monthrange
import collections
import csv
import datetime
import logging
import os
import time
import typing
from kwnlp_preprocessor import argconfig


logger = logging.getLogger(__name__)


def _get_date_obj(yyyymmdd: str) -> datetime.date:
    wp_date = datetime.date(
        year=int(yyyymmdd[0:4]),
        month=int(yyyymmdd[4:6]),
        day=int(yyyymmdd[6:8]),
    )
    return wp_date


def _subtract_one_month(date_obj: datetime.date) -> datetime.date:
    date_obj_minus_one_month = date_obj.replace(day=1) - datetime.timedelta(days=1)
    return date_obj_minus_one_month


def main(
    wp_yyyymmdd: str,
    data_path: str = argconfig.DEFAULT_KWNLP_DATA_PATH,
    wiki: str = argconfig.DEFAULT_KWNLP_WIKI,
) -> None:

    wp_raw_path = os.path.join(data_path, f"wikipedia-raw-{wp_yyyymmdd}")
    wp_raw_pageview_path = os.path.join(wp_raw_path, "pageviewcomplete")
    wp_derived_path = os.path.join(data_path, f"wikipedia-derived-{wp_yyyymmdd}")
    filter_code = "{}.wikipedia".format(wiki.replace("wiki", ""))

    wp_date = _get_date_obj(wp_yyyymmdd)
    wp_date_minus_one_month = _subtract_one_month(wp_date)
    year = wp_date_minus_one_month.year
    month = wp_date_minus_one_month.month
    _, days_in_month = monthrange(wp_date_minus_one_month.year, wp_date_minus_one_month.month)

    # read in and filter pagecounts
    # ====================================================================
    t_start = time.time()
    pageviews: typing.Counter[str] = collections.Counter()

    # loop over days and accumulate into pageviews counter
    for day in range(1, days_in_month + 1):

        in_file_name = "pageviews-{}{:0>2d}{:0>2d}-user.bz2".format(year, month, day)
        in_file_path = os.path.join(wp_raw_pageview_path, in_file_name)
        logger.info(f"reading and filtering {in_file_path}")

        with bz2.open(in_file_path, "rb") as fp:
            for line in fp:
                pieces = line.decode("utf-8", errors="ignore").split()
                if len(pieces) != 6:
                    continue
                if not pieces[0].startswith(filter_code):
                    continue
                project_name, page_title, page_id, platform, daily_views, hourly_views = pieces

                # to capture views for the same page from different sources
                # e.g.
                # ['en.wikipedia', 'Anarchism', '12', 'desktop', '1614', 'A69B77C73D72E59F46G52H67I37J44K50L44M58N56O90P103Q78R79S94T60U68V80W88X70']
                # ['en.wikipedia', 'Anarchism', '12', 'mobile-web', '1972', 'A94B100C120D86E124F91G65H83I57J63K60L64M58N68O81P86Q84R80S99T67U80V90W79X93']
                pageviews[page_title] += int(daily_views)
                if page_title == "Anarchism":
                    print(pieces)

    rows = [(page_title, views) for page_title, views in pageviews.items()]
    t_end = time.time()
    logger.info("time elapsed: {:.2f}s".format(t_end - t_start))

    # write out filtered pagecounts
    # ====================================================================
    out_file_name = f"kwnlp-{wiki}-{wp_yyyymmdd}-prior-month-pageviews-complete.csv"
    out_file_path = os.path.join(
        wp_derived_path,
        "kwnlp-sql",
        out_file_name,
    )
    os.makedirs(os.path.dirname(out_file_path), exist_ok=True)
    logger.info(f"writing {out_file_path}")
    t_start = time.time()
    with open(out_file_path, "w") as ofp:
        csv_writer = csv.writer(ofp)
        csv_writer.writerow(["page_title", "views"])
        csv_writer.writerows(rows)
    t_end = time.time()


if __name__ == "__main__":

    description = "create kwnlp pageviews-complete"
    arg_names = ["wp_yyyymmdd", "data_path", "wiki", "loglevel"]
    parser = argconfig.get_argparser(description, arg_names)

    args = parser.parse_args()
    logging.basicConfig(level=args.loglevel)
    logger.info(f"args={args}")

    main(args.wp_yyyymmdd, data_path=args.data_path, wiki=args.wiki)

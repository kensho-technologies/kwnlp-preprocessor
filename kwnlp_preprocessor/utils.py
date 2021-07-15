# Copyright 2021-present Kensho Technologies, LLC.
import os
import re
from typing import List, Pattern, Union


def _get_ordered_files_from_path(path: str, pattern: Pattern) -> List[re.Match]:
    """Return ordered list of regex matches from input path and regex pattern.

    Assumes that the pattern groups are convertible to integers.
    """
    all_matches: List[Union[re.Match, None]] = [
        re.match(pattern, filename) for filename in os.listdir(path)
    ]
    matches: List[re.Match] = [match for match in all_matches if match is not None]
    matches = sorted(matches, key=lambda x: tuple(int(grp) for grp in x.groups()))
    return matches

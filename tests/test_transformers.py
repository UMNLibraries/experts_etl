from datetime import datetime, timezone

import pytest

from experts_etl import transformers
from experts_etl.exceptions import ExpertsEtlUnkownDatetimeFormatError

def test_transform_datetime_string():
    expected_datetime = datetime(2018, 7, 13, 6, 0, 4, 110000, tzinfo=timezone.utc)
    assert transformers.iso_8601_string_to_datetime("2018-07-13T06:00:04.110+0000") == expected_datetime

def test_iso_8601_string_to_datetime():
    expected_datetime = datetime(2018, 7, 13, 6, 0, 4, 110000, tzinfo=timezone.utc)
    assert transformers.string_to_datetime("2018-07-13T06:00:04.110+0000") == expected_datetime

def test_ymd_string_to_datetime():
    expected_datetime = datetime(2018, 7, 13)
    assert transformers.string_to_datetime("2018-07-13") == expected_datetime

def test_ym_string_to_datetime():
    expected_datetime = datetime(2018, 7, 1)
    assert transformers.string_to_datetime("2018-07") == expected_datetime

def test_y_string_to_datetime():
    expected_datetime = datetime(2018, 1, 1)
    assert transformers.string_to_datetime("2018") == expected_datetime

def test_unsupported_datetime_string_format():
    with pytest.raises(ExpertsEtlUnkownDatetimeFormatError):
        transformers.string_to_datetime("bogus")

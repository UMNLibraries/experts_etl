from datetime import datetime, timezone
from experts_etl import transformers

def test_transform_datetime_string():
  expected_datetime = datetime(2018, 7, 13, 6, 0, 4, 110000, tzinfo=timezone.utc)
  assert transformers.iso_8601_string_to_datetime("2018-07-13T06:00:04.110+0000") == expected_datetime

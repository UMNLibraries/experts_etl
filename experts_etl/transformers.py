from datetime import datetime
import re

from experts_etl.exceptions import ExpertsEtlUnkownDatetimeFormatError

iso_8601_format = '%Y-%m-%dT%H:%M:%S.%f%z'

datetime_format_pattern_map = {
    '%Y-%m-%dT%H:%M:%S.%f%z': re.compile(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}\+\d{4}$'), # ISO 8601
    '%Y-%m-%d': re.compile(r'^\d{4}-\d{2}-\d{2}$'),
    '%Y-%m': re.compile(r'^\d{4}-\d{2}$'),
    '%Y': re.compile(r'^\d{4}$'),
}

def iso_8601_string_to_datetime(iso_8601_string):
    return datetime.strptime(
        iso_8601_string,
        iso_8601_format,
    )

def string_to_datetime(string):
    _datetime = None
    for _format, pattern in datetime_format_pattern_map.items():
        if pattern.match(string):
            _datetime = datetime.strptime(string, _format)
            break
    if _datetime:
        return _datetime
    raise ExpertsEtlUnkownDatetimeFormatError(string)

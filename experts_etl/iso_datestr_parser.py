from datetime import datetime

# ISO 8601 datetime string format, where f = milliseconds and z = timezone: '%Y-%m-%dT%H:%M:%S.%f%z'
iso_8601_format_sans_ms_tz = '%Y-%m-%dT%H:%M:%S'

def datetime_sans_ms_tz(iso_8601_datetime_str):
    # We exclude the milliseconds and timezone for comparison with datetimes
    # retrieved from Oracle which, at least in some cases, do not have this info.
    # If we do not exclude them, we get errors like:
    # TypeError: can't compare offset-naive and offset-aware datetimes
    return datetime.strptime(
        iso_8601_datetime_str[:iso_8601_datetime_str.rfind('.')],
        iso_8601_format_sans_ms_tz
    )

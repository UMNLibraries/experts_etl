from datetime import datetime

# defaults:

iso_8601_format = '%Y-%m-%dT%H:%M:%S.%f%z'

def iso_8601_string_to_datetime(iso_8601_string):
  return datetime.strptime(
    iso_8601_string,
    iso_8601_format,
  )

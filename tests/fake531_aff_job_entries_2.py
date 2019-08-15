import datetime

entries = [
  {
    'emplid': 'fake531',
    'deptid': '11219',
    'um_campus': 'TXXX',
    'title': 'Adjunct Professor',
    'um_affiliate_id': '03',
    'um_affil_relation': '9401A',
    'effdt': datetime.datetime(2017,1,18,0,0),
    'status': 'A',
    'status_flg': 'C',
  },
]

stints = [entries]

entry_groups = [
  {
    'deptid': '11219',
    'um_affil_relation': '9401A',
    'start_date': datetime.datetime(2017,1,18,0,0),
    'entries': entries,
  },
]

jobs = [
  {
   'affiliation_id': '9401A',
   'deptid': '11219',
   'um_campus': 'TXXX',
   'org_id': 'LBZUCPBF',
   'job_title': 'Adjunct Professor',
   'job_description': 'Adjunct Professor',
   'employment_type': 'adjunct_faculty',
   'staff_type': 'nonacademic',
   'start_date': datetime.datetime(2017,1,18,0,0),
   'end_date': None,
   'visibility': 'Restricted',
   'profiled': False,
  },
]

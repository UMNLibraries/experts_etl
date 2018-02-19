import datetime

entries = [
  {
    'emplid': 'fake357',
    'deptid': '11735',
    'title': 'Adjunct Assistant Professor',
    'um_affiliate_id': '01',
    'um_affil_relation': '9403A',
    'effdt': datetime.datetime(2015,4,6,0,0),
    'status': 'A',
    'status_flg': 'H',
  },
  {
    'emplid': 'fake357',
    'deptid': '11735',
    'title': 'Adjunct Assistant Professor',
    'um_affiliate_id': '01',
    'um_affil_relation': '9403A',
    'effdt': datetime.datetime(2015,4,7,0,0),
    'status': 'I',
    'status_flg': 'C',
  },
]

stints = [entries]

jobs = [
  {
   'deptid': '11735',
   'org_id': 'WSSKOZQ',
   'job_title': 'Adjunct Assistant Professor',
   'employment_type': 'adjunct_faculty',
   'staff_type': 'nonacademic',
   'start_date': datetime.datetime(2015,4,6,0,0),
   'end_date': datetime.datetime(2015,4,7,0,0),
   'visibility': 'Restricted',
   'profiled': False,
  },
]

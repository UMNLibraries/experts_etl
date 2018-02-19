import datetime

entries = [
  {
    'emplid': 'fake531',
    'deptid': '11219',
    'title': 'Adjunct Associate Professor',
    'um_affiliate_id': '03',
    'um_affil_relation': '9402A',
    'effdt': datetime.datetime(2015,4,6,0,0),
    'status': 'A',
    'status_flg': 'H',
  },
  {
    'emplid': 'fake531',
    'deptid': '11219',
    'title': 'Adjunct Associate Professor',
    'um_affiliate_id': '03',
    'um_affil_relation': '9402A',
    'effdt': datetime.datetime(2017,1,12,0,0),
    'status': 'A',
    'status_flg': 'H',
  },
]

stints = [entries]

jobs = [
  {
   'deptid': '11219',
   'org_id': 'LBZUCPBF',
   'job_title': 'Adjunct Associate Professor',
   'employment_type': 'adjunct_faculty',
   'staff_type': 'nonacademic',
   'start_date': datetime.datetime(2015,4,6,0,0),
   'end_date': datetime.datetime(2017,1,12,0,0),
   'visibility': 'Restricted',
   'profiled': False,
  },
]

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
    'effdt': datetime.datetime(2016,5,7,0,0),
    'status': 'I',
    'status_flg': 'H',
  },
  {
    'emplid': 'fake531',
    'deptid': '11219',
    'title': 'Adjunct Associate Professor',
    'um_affiliate_id': '03',
    'um_affil_relation': '9402A',
    'effdt': datetime.datetime(2016,6,8,0,0),
    'status': 'I',
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
  {
    'emplid': 'fake531',
    'deptid': '11219',
    'title': 'Adjunct Associate Professor',
    'um_affiliate_id': '03',
    'um_affil_relation': '9402A',
    'effdt': datetime.datetime(2017,2,13,0,0),
    'status': 'I',
    'status_flg': 'H',
  },
  {
    'emplid': 'fake531',
    'deptid': '11219',
    'title': 'Adjunct Associate Professor',
    'um_affiliate_id': '03',
    'um_affil_relation': '9402A',
    'effdt': datetime.datetime(2018,3,14,0,0),
    'status': 'A',
    'status_flg': 'C',
  },
]

stints = [
  entries[0:3],
  entries[3:5],
  entries[5:],
]

entry_groups = [
  {
    'deptid': '11219',
    'um_affil_relation': '9402A',
    'start_date': datetime.datetime(2015,4,6,0,0),
    'entries': entries[0:3],
  },
  {
    'deptid': '11219',
    'um_affil_relation': '9402A',
    'start_date': datetime.datetime(2017,1,12,0,0),
    'entries': entries[3:5],
  },
  {
    'deptid': '11219',
    'um_affil_relation': '9402A',
    'start_date': datetime.datetime(2018,3,14,0,0),
    'entries': entries[5:],
  },
]

jobs = [
  {
   'deptid': '11219',
   'org_id': 'LBZUCPBF',
   'job_title': 'Adjunct Associate Professor',
   'employment_type': 'adjunct_faculty',
   'staff_type': 'nonacademic',
   'start_date': datetime.datetime(2015,4,6,0,0),
   'end_date': datetime.datetime(2016,6,8,0,0),
   'visibility': 'Restricted',
   'profiled': False,
  },
  {
   'deptid': '11219',
   'org_id': 'LBZUCPBF',
   'job_title': 'Adjunct Associate Professor',
   'employment_type': 'adjunct_faculty',
   'staff_type': 'nonacademic',
   'start_date': datetime.datetime(2017,1,12,0,0),
   'end_date': datetime.datetime(2017,2,13,0,0),
   'visibility': 'Restricted',
   'profiled': False,
  },
  {
   'deptid': '11219',
   'org_id': 'LBZUCPBF',
   'job_title': 'Adjunct Associate Professor',
   'employment_type': 'adjunct_faculty',
   'staff_type': 'nonacademic',
   'start_date': datetime.datetime(2018,3,14,0,0),
   'end_date': None,
   'visibility': 'Restricted',
   'profiled': False,
  },
]
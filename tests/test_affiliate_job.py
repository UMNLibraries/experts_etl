from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())
import datetime, re
from experts_etl import affiliate_job

def test_extract():
  #emplid = '3367339', '1173706'
  emplid = '2585238'
  jobs = affiliate_job.extract(emplid)

  assert isinstance(jobs, list)
  for job in jobs:
    assert isinstance(job, dict)
    assert job['emplid'] == emplid
    assert re.match(r'^\d+$', job['um_affiliate_id']) 
    assert re.match(r'^\d+$', job['deptid']) 
    assert isinstance(job['effdt'], datetime.date)

def test_group_jobs_by_deptid_um_affiliate_id_um_affil_relation():
  jobs = [
    {'um_affiliate_id': '01', 'um_affil_relation': '9402A', 'deptid': '10986', 'a': 2, 'b': 3},
    {'um_affiliate_id': '01', 'um_affil_relation': '9402A', 'deptid': '10986', 'a': 3, 'b': 4},
    {'um_affiliate_id': '02', 'um_affil_relation': '9403A', 'deptid': '11083', 'a': 5, 'b': 6},
    {'um_affiliate_id': '03', 'um_affil_relation': '9402A', 'deptid': '11219', 'a': 7, 'b': 8},
    {'um_affiliate_id': '02', 'um_affil_relation': '9403A', 'deptid': '11083', 'a': 9, 'b': 10},
    {'um_affiliate_id': '03', 'um_affil_relation': '9402A', 'deptid': '11219', 'a': 11, 'b': 12},
    {'um_affiliate_id': '03', 'um_affil_relation': '9401A', 'deptid': '11219', 'a': 13, 'b': 14},
    {'um_affiliate_id': '04', 'um_affil_relation': '9403A', 'deptid': '10986', 'a': 15, 'b': 16},
  ]

  expected_jobs_by_deptid_um_affiliate_id_um_affil_relation = {
    '10986-01-9402A': [
      {'um_affiliate_id': '01', 'um_affil_relation': '9402A', 'deptid': '10986', 'a': 2, 'b': 3},
      {'um_affiliate_id': '01', 'um_affil_relation': '9402A', 'deptid': '10986', 'a': 3, 'b': 4},
    ],
    '11083-02-9403A': [
      {'um_affiliate_id': '02', 'um_affil_relation': '9403A', 'deptid': '11083', 'a': 5, 'b': 6},
      {'um_affiliate_id': '02', 'um_affil_relation': '9403A', 'deptid': '11083', 'a': 9, 'b': 10},
    ],
    '11219-03-9402A': [
      {'um_affiliate_id': '03', 'um_affil_relation': '9402A', 'deptid': '11219', 'a': 7, 'b': 8},
      {'um_affiliate_id': '03', 'um_affil_relation': '9402A', 'deptid': '11219', 'a': 11, 'b': 12},
    ],
    '11219-03-9401A': [
      {'um_affiliate_id': '03', 'um_affil_relation': '9401A', 'deptid': '11219', 'a': 13, 'b': 14},
    ],
    '10986-04-9403A': [
      {'um_affiliate_id': '04', 'um_affil_relation': '9403A', 'deptid': '10986', 'a': 15, 'b': 16},
    ],
  }

  jobs_by_deptid_um_affiliate_id_um_affil_relation = affiliate_job.group_by_deptid_um_affiliate_id_um_affil_relation(jobs)
  assert jobs_by_deptid_um_affiliate_id_um_affil_relation == expected_jobs_by_deptid_um_affiliate_id_um_affil_relation

def test_transform_job_entries():
  entries = [
    {
      'emplid': '1173706',
      'deptid': '11735',
      'um_affiliate_id': '01',
      'um_affil_relation': '9403A',
      'effdt': datetime.date(2015,4,6),
      'status': 'A',
      'status_flg': 'H',
    },
    {
      'emplid': '1173706',
      'deptid': '11735',
      'um_affiliate_id': '01',
      'um_affil_relation': '9403A',
      'effdt': datetime.date(2015,4,7),
      'status': 'I',
      'status_flg': 'C',
    },
  ]

  job_stints = affiliate_job.transform_job_entries(entries)

  expected_job_stints = [
    [
      {
        'emplid': '1173706',
        'deptid': '11735',
        'um_affiliate_id': '01',
        'um_affil_relation': '9403A',
        'effdt': datetime.date(2015,4,6),
        'status': 'A',
        'status_flg': 'H',
      },
      {
        'emplid': '1173706',
        'deptid': '11735',
        'um_affiliate_id': '01',
        'um_affil_relation': '9403A',
        'effdt': datetime.date(2015,4,7),
        'status': 'I',
        'status_flg': 'C',
      },
    ],
  ]

  assert job_stints == expected_job_stints

  entries_2 = [
    {
      'emplid': '2585238',
      'deptid': '11219',
      'um_affiliate_id': '03',
      'um_affil_relation': '9402A',
      'effdt': datetime.date(2015,4,6),
      'status': 'A',
      'status_flg': 'H',
    },
    {
      'emplid': '2585238',
      'deptid': '11219',
      'um_affiliate_id': '03',
      'um_affil_relation': '9402A',
      'effdt': datetime.date(2017,1,12),
      'status': 'A',
      'status_flg': 'H',
    },
  ]

  job_stints_2 = affiliate_job.transform_job_entries(entries_2)

  expected_job_stints_2 = [
    [
      {
        'emplid': '2585238',
        'deptid': '11219',
        'um_affiliate_id': '03',
        'um_affil_relation': '9402A',
        'effdt': datetime.date(2015,4,6),
        'status': 'A',
        'status_flg': 'H',
      },
      {
        'emplid': '2585238',
        'deptid': '11219',
        'um_affiliate_id': '03',
        'um_affil_relation': '9402A',
        'effdt': datetime.date(2017,1,12),
        'status': 'A',
        'status_flg': 'H',
      },
    ],
  ]

  assert job_stints_2 == expected_job_stints_2

  entries_3 = [
    {
      'emplid': '2585238',
      'deptid': '11219',
      'um_affiliate_id': '03',
      'um_affil_relation': '9401A',
      'effdt': datetime.date(2017,1,18),
      'status': 'A',
      'status_flg': 'C',
    },
  ]

  job_stints_3 = affiliate_job.transform_job_entries(entries_3)

  expected_job_stints_3 = [
    [
      {
        'emplid': '2585238',
        'deptid': '11219',
        'um_affiliate_id': '03',
        'um_affil_relation': '9401A',
        'effdt': datetime.date(2017,1,18),
        'status': 'A',
        'status_flg': 'C',
      },
    ],
  ]

  assert job_stints_3 == expected_job_stints_3

def test_transform_job_stint():
  job_stint_1 = [
    {
      'emplid': '1173706',
      'deptid': '11735',
      'um_affiliate_id': '01',
      'um_affil_relation': '9403A',
      'title': 'Adjunct Assistant Professor',
      'effdt': datetime.date(2015,4,6),
      'status': 'A',
      'status_flg': 'H',
    },
    {
      'emplid': '1173706',
      'deptid': '11735',
      'um_affiliate_id': '01',
      'um_affil_relation': '9403A',
      'title': 'Adjunct Assistant Professor',
      'effdt': datetime.date(2015,4,7),
      'status': 'I',
      'status_flg': 'C',
    },
  ]

  expected_transformed_job_1 = {
   'deptid': '11735',
   'org_id': 'WSSKOZQ',
   'job_title': 'Adjunct Assistant Professor',
   'employment_type': 'adjunct_faculty',
   'staff_type': 'nonacademic',
   'start_date': datetime.date(2015,4,6),
   'end_date': datetime.date(2015,4,7),
   'visibility': 'Restricted',
   'profiled': False,
  }

  transformed_job_1 = affiliate_job.transform_job_stint(job_stint_1)
  assert transformed_job_1 == expected_transformed_job_1

  job_stint_2 = [
    {
      'emplid': '2585238',
      'deptid': '11219',
      'um_affiliate_id': '03',
      'um_affil_relation': '9402A',
      'title': 'Adjunct Associate Professor',
      'effdt': datetime.date(2015,4,6),
      'status': 'A',
      'status_flg': 'H',
    },
    {
      'emplid': '2585238',
      'deptid': '11219',
      'um_affiliate_id': '03',
      'um_affil_relation': '9402A',
      'title': 'Adjunct Associate Professor',
      'effdt': datetime.date(2017,1,12),
      'status': 'A',
      'status_flg': 'H',
    },
  ]

  expected_transformed_job_2 = {
   'deptid': '11219',
   'org_id': 'LBZUCPBF',
   'job_title': 'Adjunct Associate Professor',
   'employment_type': 'adjunct_faculty',
   'staff_type': 'nonacademic',
   'start_date': datetime.date(2015,4,6),
   'end_date': datetime.date(2017,1,12),
   'visibility': 'Restricted',
   'profiled': False,
  }

  transformed_job_2 = affiliate_job.transform_job_stint(job_stint_2)
  assert transformed_job_2 == expected_transformed_job_2

  job_stint_3 = [
    {
      'emplid': '2585238',
      'deptid': '11219',
      'um_affiliate_id': '03',
      'um_affil_relation': '9401A',
      'title': 'Adjunct Professor',
      'effdt': datetime.date(2017,1,18),
      'status': 'A',
      'status_flg': 'C',
    },
  ]

  expected_transformed_job_3 = {
   'deptid': '11219',
   'org_id': 'LBZUCPBF',
   'job_title': 'Adjunct Professor',
   'employment_type': 'adjunct_faculty',
   'staff_type': 'nonacademic',
   'start_date': datetime.date(2017,1,18),
   'end_date': None,
   'visibility': 'Restricted',
   'profiled': False,
  }

  transformed_job_3 = affiliate_job.transform_job_stint(job_stint_3)
  assert transformed_job_3 == expected_transformed_job_3

def test_transform():
  jobs = [
    {
      'emplid': '1173706',
      'deptid': '11735',
      'um_affiliate_id': '01',
      'um_affil_relation': '9403A',
      'title': 'Adjunct Assistant Professor',
      'effdt': datetime.date(2015,4,6),
      'status': 'A',
      'status_flg': 'H',
    },
    {
      'emplid': '1173706',
      'deptid': '11735',
      'um_affiliate_id': '01',
      'um_affil_relation': '9403A',
      'title': 'Adjunct Assistant Professor',
      'effdt': datetime.date(2015,4,7),
      'status': 'I',
      'status_flg': 'C',
    },
  ]

  transformed_jobs = affiliate_job.transform(jobs)

  expected_transformed_jobs = [
    {
      'deptid': '11735',
      'org_id': 'WSSKOZQ',
      'job_title': 'Adjunct Assistant Professor',
      'employment_type': 'adjunct_faculty',
      'staff_type': 'nonacademic',
      'start_date': datetime.date(2015,4,6),
      'end_date': datetime.date(2015,4,7),
      'visibility': 'Restricted',
      'profiled': False,
    }
  ]

  assert transformed_jobs == expected_transformed_jobs

#def test_extract_transform():
#  transformed_jobs = employee_job.extract_transform('1082441')
#
#  expected_transformed_jobs = [
#    {
#     'deptid': '11945',
#     'org_id': 'QWTDIYIJ',
#     'empl_rcdno': '0',
#     'job_title': 'Assistant Professor',
#     'employment_type': 'faculty',
#     'staff_type': 'academic',
#     'start_date': datetime.datetime(2007,1,30,0,0),
#     'end_date': datetime.datetime(2015,8,24,0,0),
#     'visibility': 'Restricted',
#     'profiled': False,
#    },
#    {
#     'deptid': '11945',
#     'org_id': 'QWTDIYIJ',
#     'empl_rcdno': '0',
#     'job_title': 'Assistant Professor',
#     'employment_type': 'faculty',
#     'staff_type': 'academic',
#     'start_date': datetime.datetime(2015,8,31,0,0),
#     'end_date': None,
#     'visibility': 'Public',
#     'profiled': True,
#    },
#  ]
#
#  assert transformed_jobs == expected_transformed_jobs

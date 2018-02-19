from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())
import datetime, re
import pytest
from experts_etl import affiliate_job

def test_extract():
  #emplid = '3367339' # Maybe use this later...
  emplid = '2585238'
  jobs = affiliate_job.extract(emplid)

  assert isinstance(jobs, list)
  for job in jobs:
    assert isinstance(job, dict)
    assert job['emplid'] == emplid
    assert re.match(r'^\d+$', job['um_affiliate_id']) 
    assert re.match(r'^\d+$', job['deptid']) 
    assert isinstance(job['effdt'], datetime.datetime)

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

@pytest.fixture(params=['fake357','fake531','fake531_2'])
def job_entries(request):
  from . import fake357_aff_job_entries
  from . import fake531_aff_job_entries
  from . import fake531_aff_job_entries_2
  entries_sets = {
    'fake357': fake357_aff_job_entries,
    'fake531': fake531_aff_job_entries,
    'fake531_2': fake531_aff_job_entries_2,
  }
  entries_set = entries_sets[request.param]
  yield entries_set

def test_transform_job_entries(job_entries):
  assert affiliate_job.transform_job_entries(job_entries.entries) == job_entries.stints

def test_transform_job_stint(job_entries):
  for index, stint in enumerate(job_entries.stints):
    assert affiliate_job.transform_job_stint(stint) == job_entries.jobs[index]

def test_transform(job_entries):
  assert affiliate_job.transform(job_entries.entries) == job_entries.jobs

def test_extract_transform():
  transformed_jobs = affiliate_job.extract_transform('1173706')

  expected_transformed_jobs = [
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

  assert transformed_jobs == expected_transformed_jobs

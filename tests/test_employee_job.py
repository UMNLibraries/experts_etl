from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())
import datetime, re
import pytest
from experts_etl import employee_job

def test_extract():
  emplid = '5150075'
  jobs = employee_job.extract(emplid)

  assert isinstance(jobs, list)
  for job in jobs:
    assert isinstance(job, dict)
    assert job['emplid'] == emplid
    assert re.match(r'^\d+$', job['empl_rcdno']) 
    assert re.match(r'^\d+$', job['position_nbr']) 
    assert isinstance(job['effdt'], datetime.date)
    assert isinstance(job['effseq'], int)

@pytest.fixture
def fake123():
  from . import fake123_employee_jobs
  return fake123_employee_jobs

def test_group_by_position_nbr(fake123):
  assert employee_job.group_by_position_nbr(fake123.entries) == fake123.entries_by_position_nbr

@pytest.fixture(params=['fake321','fake765'])
def job_entries(request):
  from . import fake321_emp_job_entries
  from . import fake765_emp_job_entries
  entries_sets = {
    'fake321': fake321_emp_job_entries,
    'fake765': fake765_emp_job_entries,
  }
  entries_set = entries_sets[request.param]
  yield entries_set

def test_transform_job_entries(job_entries):
  assert employee_job.transform_job_entries(job_entries.entries) == job_entries.stints

@pytest.fixture
def job_stints():
  from . import fake321_emp_job_entries
  return fake321_emp_job_entries

def test_transform_job_stint(job_stints):
    for index, stint in enumerate(job_stints.stints):
      assert employee_job.transform_job_stint(stint) == job_stints.jobs[index]

def test_transform(job_stints):
  assert job_stints.jobs == employee_job.transform(job_stints.entries)

def test_extract_transform():
  transformed_jobs = employee_job.extract_transform('1082441')

  expected_transformed_jobs = [
    {
     'deptid': '11945',
     'org_id': 'QWTDIYIJ',
     'empl_rcdno': '0',
     'job_title': 'Assistant Professor',
     'employment_type': 'faculty',
     'staff_type': 'academic',
     'start_date': datetime.datetime(2007,1,30,0,0),
     'end_date': datetime.datetime(2015,8,24,0,0),
     'visibility': 'Restricted',
     'profiled': False,
    },
    {
     'deptid': '11945',
     'org_id': 'QWTDIYIJ',
     'empl_rcdno': '0',
     'job_title': 'Assistant Professor',
     'employment_type': 'faculty',
     'staff_type': 'academic',
     'start_date': datetime.datetime(2015,8,31,0,0),
     'end_date': None,
     'visibility': 'Public',
     'profiled': True,
    },
  ]

  assert transformed_jobs == expected_transformed_jobs

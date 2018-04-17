from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())
import datetime, re
import pandas as pd
import pytest
from experts_etl import employee_job

def test_extract():
  emplid = '5150075'
  entries = employee_job.extract(emplid)

  assert isinstance(entries, list)
  for entry in entries:
    assert isinstance(entry, dict)
    assert entry['emplid'] == emplid
    assert re.match(r'^\d+$', entry['empl_rcdno']) 
    assert re.match(r'^\d+$', entry['position_nbr']) 
    assert isinstance(entry['effdt'], datetime.datetime)
    assert isinstance(entry['effseq'], int)

@pytest.fixture(params=['fake321','fake322','fake765','fake123','fake312','fake567'])
def entries_to_group(request):
  from . import fake321_emp_job_entries
  from . import fake322_emp_job_entries
  from . import fake765_emp_job_entries
  from . import fake123_employee_jobs
  from . import fake312_employee_jobs
  from . import fake567_employee_jobs
  entries_sets = {
    'fake321': fake321_emp_job_entries,
    'fake322': fake322_emp_job_entries,
    'fake765': fake765_emp_job_entries,
    'fake123': fake123_employee_jobs,
    'fake312': fake312_employee_jobs,
    'fake567': fake567_employee_jobs,
  }
  entries_set = entries_sets[request.param]
  yield entries_set

def test_group_entries(entries_to_group):
  assert employee_job.group_entries(entries_to_group.entries) == entries_to_group.entry_groups
  assert employee_job.group_entries([]) == []

@pytest.fixture(params=['fake321','fake322','fake765','fake312','fake567'])
def entry_groups(request):
  from . import fake321_emp_job_entries
  from . import fake322_emp_job_entries
  from . import fake765_emp_job_entries
  from . import fake312_employee_jobs
  from . import fake567_employee_jobs
  entries_sets = {
    'fake321': fake321_emp_job_entries,
    'fake322': fake322_emp_job_entries,
    'fake765': fake765_emp_job_entries,
    'fake312': fake312_employee_jobs,
    'fake567': fake567_employee_jobs,
  }
  entries_set = entries_sets[request.param]
  yield entries_set

def test_transform_entry_groups(entry_groups):
  assert employee_job.transform_entry_groups(entry_groups.entry_groups) == entry_groups.jobs
  assert employee_job.transform_entry_groups([]) == []

def test_transform(entry_groups):
  assert employee_job.transform(entry_groups.entries) == entry_groups.jobs
  assert employee_job.transform([]) == []

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

import datetime, re
import pandas as pd
import pytest
from experts_dw import db
from experts_etl import employee_job

@pytest.fixture
def session():
  with db.session('hotel') as session:
    yield session

def test_extract(session):
  emplid = '5150075'
  entries = employee_job.extract(session, emplid)

  assert isinstance(entries, list)
  for entry in entries:
    assert isinstance(entry, dict)
    assert entry['emplid'] == emplid
    assert re.match(r'^\d+$', entry['empl_rcdno']) 
    assert re.match(r'^\d+$', entry['position_nbr']) 
    assert isinstance(entry['effdt'], datetime.datetime)
    assert isinstance(entry['effseq'], int)

@pytest.fixture(params=['4604830','1082441','3262322','5150075','1717940','1217312','2110507','8000397'])
def entries_to_group(request):
  from . import emp_job_entries_4604830
  from . import emp_job_entries_1082441
  from . import emp_job_entries_3262322
  from . import emp_job_entries_5150075
  from . import employee_jobs_1717940
  from . import employee_jobs_1217312
  from . import employee_jobs_2110507
  from . import employee_jobs_8000397
  entries_sets = {
    '4604830': emp_job_entries_4604830,
    '1082441': emp_job_entries_1082441,
    '3262322': emp_job_entries_3262322,
    '5150075': emp_job_entries_5150075,
    '1717940': employee_jobs_1717940,
    '1217312': employee_jobs_1217312,
    '2110507': employee_jobs_2110507,
    '8000397': employee_jobs_8000397,
  }
  entries_set = entries_sets[request.param]
  yield entries_set

def test_group_entries(entries_to_group):
  assert employee_job.group_entries(entries_to_group.entries) == entries_to_group.entry_groups
  assert employee_job.group_entries([]) == []

@pytest.fixture(params=['4604830','1082441','3262322','5150075','1217312','2110507','8000397'])
def entry_groups(request):
  from . import emp_job_entries_4604830
  from . import emp_job_entries_1082441
  from . import emp_job_entries_3262322
  from . import emp_job_entries_5150075
  from . import employee_jobs_1217312
  from . import employee_jobs_2110507
  from . import employee_jobs_8000397
  entries_sets = {
    '4604830': emp_job_entries_4604830,
    '1082441': emp_job_entries_1082441,
    '3262322': emp_job_entries_3262322,
    '5150075': emp_job_entries_5150075,
    '1217312': employee_jobs_1217312,
    '2110507': employee_jobs_2110507,
    '8000397': employee_jobs_8000397,
  }
  entries_set = entries_sets[request.param]
  yield entries_set

def test_transform_entry_groups(session, entry_groups):
  assert employee_job.transform_entry_groups(session, entry_groups.entry_groups) == entry_groups.jobs
  assert employee_job.transform_entry_groups(session, []) == []

def test_transform(session, entry_groups):
  assert employee_job.transform(session, entry_groups.entries) == entry_groups.jobs
  assert employee_job.transform(session, []) == []

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

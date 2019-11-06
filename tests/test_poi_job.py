import datetime, re
import pandas as pd
import pytest
from experts_dw import db
from experts_etl.oit_to_edw import poi_job

@pytest.fixture
def session():
  with db.session('hotel') as session:
    yield session

def test_extract(session):
  emplid = '2898289'
  entries = poi_job.extract(session, emplid)

  assert isinstance(entries, list)
  for entry in entries:
    assert isinstance(entry, dict)
    assert entry['emplid'] == emplid
    assert re.match(r'^\d+$', entry['empl_rcdno'])
    assert re.match(r'^\d+$', entry['position_nbr'])
    assert isinstance(entry['effdt'], datetime.datetime)
    assert isinstance(entry['effseq'], int)

@pytest.fixture(params=['2898289','5575725','2927554','5231388','5491169'])
def entries_to_group(request):
  from . import poi_jobs_2898289
  from . import poi_jobs_5575725
  from . import poi_jobs_2927554
  from . import poi_jobs_5231388
  from . import poi_jobs_5491169
  entries_sets = {
    '2898289': poi_jobs_2898289,
    '5575725': poi_jobs_5575725,
    '2927554': poi_jobs_2927554,
    '5231388': poi_jobs_5231388,
    '5491169': poi_jobs_5491169,
  }
  entries_set = entries_sets[request.param]
  yield entries_set

def test_group_entries(entries_to_group):
  print(entries_to_group.entry_groups)
  assert poi_job.group_entries(entries_to_group.entries) == entries_to_group.entry_groups
  assert poi_job.group_entries([]) == []

@pytest.fixture(params=['2898289','5575725','2927554','5231388','5491169'])
def entry_groups(request):
  from . import poi_jobs_2898289
  from . import poi_jobs_5575725
  from . import poi_jobs_2927554
  from . import poi_jobs_5231388
  from . import poi_jobs_5491169
  entries_sets = {
    '2898289': poi_jobs_2898289,
    '5575725': poi_jobs_5575725,
    '2927554': poi_jobs_2927554,
    '5231388': poi_jobs_5231388,
    '5491169': poi_jobs_5491169,
  }
  entries_set = entries_sets[request.param]
  yield entries_set

def test_transform_entry_groups(session, entry_groups):
  assert poi_job.transform_entry_groups(session, entry_groups.entry_groups) == entry_groups.jobs
  assert poi_job.transform_entry_groups(session, []) == []

def test_transform(session, entry_groups):
  assert poi_job.transform(session, entry_groups.entries) == entry_groups.jobs
  assert poi_job.transform(session, []) == []

##def test_extract_transform():
##  transformed_jobs = employee_job.extract_transform('1082441')
##
##  expected_transformed_jobs = [
##    {
##     'deptid': '11945',
##     'org_id': 'QWTDIYIJ',
##     'empl_rcdno': '0',
##     'job_title': 'Assistant Professor',
##     'employment_type': 'faculty',
##     'staff_type': 'academic',
##     'start_date': datetime.datetime(2007,1,30,0,0),
##     'end_date': datetime.datetime(2015,8,24,0,0),
##     'visibility': 'Restricted',
##     'profiled': False,
##    },
##    {
##     'deptid': '11945',
##     'org_id': 'QWTDIYIJ',
##     'empl_rcdno': '0',
##     'job_title': 'Assistant Professor',
##     'employment_type': 'faculty',
##     'staff_type': 'academic',
##     'start_date': datetime.datetime(2015,8,31,0,0),
##     'end_date': None,
##     'visibility': 'Public',
##     'profiled': True,
##    },
##  ]
##
##  assert transformed_jobs == expected_transformed_jobs

from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())
import datetime, re
import pytest
from experts_etl import affiliate_job

def test_extract():
  #emplid = '3367339' # Maybe use this later...
  emplid = '2585238'
  entries = affiliate_job.extract(emplid)

  assert isinstance(entries, list)
  for entry in entries:
    assert isinstance(entry, dict)
    assert entry['emplid'] == emplid
    assert re.match(r'^\d+$', entry['um_affiliate_id']) 
    assert re.match(r'^\d+$', entry['deptid']) 
    assert isinstance(entry['effdt'], datetime.datetime)

@pytest.fixture(params=['fake357','fake531','fake531_2','fake531_3'])
def job_entries(request):
  from . import fake357_aff_job_entries
  from . import fake531_aff_job_entries
  from . import fake531_aff_job_entries_2
  from . import fake531_aff_job_entries_3
  entries_sets = {
    'fake357': fake357_aff_job_entries,
    'fake531': fake531_aff_job_entries,
    'fake531_2': fake531_aff_job_entries_2,
    'fake531_3': fake531_aff_job_entries_3,
  }
  entries_set = entries_sets[request.param]
  yield entries_set

def test_group_entries(job_entries):
  assert affiliate_job.group_entries(job_entries.entries) == job_entries.entry_groups

def test_split_entries_into_stints(job_entries):
  # Danger! This only works right now because all entries for each person have the same
  # deptid, um_affiliate_id, and um_affil_relation.
  assert affiliate_job.split_entries_into_stints(job_entries.entries) == job_entries.stints

def test_transform(job_entries):
  assert affiliate_job.transform(job_entries.entries) == job_entries.jobs

def test_extract_transform():
  jobs = affiliate_job.extract_transform('1173706')

  expected_jobs = [
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

  assert jobs == expected_jobs

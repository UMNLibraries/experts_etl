from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())
import datetime, re
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

def test_group_jobs_by_position_nbr():
  jobs = [
    {'position_nbr': '1', 'a': 2, 'b': 3},
    {'position_nbr': '1', 'a': 3, 'b': 4},
    {'position_nbr': '2', 'a': 5, 'b': 6},
    {'position_nbr': '3', 'a': 7, 'b': 8},
    {'position_nbr': '2', 'a': 9, 'b': 10},
    {'position_nbr': '1', 'a': 11, 'b': 12},
  ]

  expected_jobs_by_position_nbr = {
    '1': [
      {'position_nbr': '1', 'a': 2, 'b': 3},
      {'position_nbr': '1', 'a': 3, 'b': 4},
      {'position_nbr': '1', 'a': 11, 'b': 12},
    ],
    '2': [
      {'position_nbr': '2', 'a': 5, 'b': 6},
      {'position_nbr': '2', 'a': 9, 'b': 10},
    ],
    '3': [
      {'position_nbr': '3', 'a': 7, 'b': 8},
    ],
  }

  jobs_by_position_nbr = employee_job.group_by_position_nbr(jobs)
  assert jobs_by_position_nbr == expected_jobs_by_position_nbr


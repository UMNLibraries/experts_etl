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

def test_transform_job_entries():
  entries = [
    {'emplid': '1082441', 'position_nbr': '208989', 'effdt': datetime.date(2015,4,6), 'empl_status': 'A'},
    {'emplid': '1082441', 'position_nbr': '208989', 'effdt': datetime.date(2015,5,25), 'empl_status': 'S'},
    {'emplid': '1082441', 'position_nbr': '208989', 'effdt': datetime.date(2015,5,25), 'empl_status': 'T'},
    {'emplid': '1082441', 'position_nbr': '208989', 'effdt': datetime.date(2015,8,24), 'empl_status': 'T'},
    {'emplid': '1082441', 'position_nbr': '208989', 'effdt': datetime.date(2015,8,31), 'empl_status': 'A'},
    {'emplid': '1082441', 'position_nbr': '208989', 'effdt': datetime.date(2016,5,23), 'empl_status': 'W'},
    {'emplid': '1082441', 'position_nbr': '208989', 'effdt': datetime.date(2016,8,29), 'empl_status': 'A'},
    {'emplid': '1082441', 'position_nbr': '208989', 'effdt': datetime.date(2016,11,14), 'empl_status': 'A'},
    {'emplid': '1082441', 'position_nbr': '208989', 'effdt': datetime.date(2017,1,9), 'empl_status': 'A'},
    {'emplid': '1082441', 'position_nbr': '208989', 'effdt': datetime.date(2017,5,17), 'empl_status': 'W'},
    {'emplid': '1082441', 'position_nbr': '208989', 'effdt': datetime.date(2017,8,21), 'empl_status': 'W'},
    {'emplid': '1082441', 'position_nbr': '208989', 'effdt': datetime.date(2017,8,28), 'empl_status': 'A'},
  ]

  job_stints = employee_job.transform_job_entries(entries)

  expected_job_stints = [
    [
      {'emplid': '1082441', 'position_nbr': '208989', 'effdt': datetime.date(2015,4,6), 'empl_status': 'A'},
      {'emplid': '1082441', 'position_nbr': '208989', 'effdt': datetime.date(2015,5,25), 'empl_status': 'S'},
      {'emplid': '1082441', 'position_nbr': '208989', 'effdt': datetime.date(2015,5,25), 'empl_status': 'T'},
      {'emplid': '1082441', 'position_nbr': '208989', 'effdt': datetime.date(2015,8,24), 'empl_status': 'T'},
    ],
    [
      {'emplid': '1082441', 'position_nbr': '208989', 'effdt': datetime.date(2015,8,31), 'empl_status': 'A'},
      {'emplid': '1082441', 'position_nbr': '208989', 'effdt': datetime.date(2016,5,23), 'empl_status': 'W'},
      {'emplid': '1082441', 'position_nbr': '208989', 'effdt': datetime.date(2016,8,29), 'empl_status': 'A'},
      {'emplid': '1082441', 'position_nbr': '208989', 'effdt': datetime.date(2016,11,14), 'empl_status': 'A'},
      {'emplid': '1082441', 'position_nbr': '208989', 'effdt': datetime.date(2017,1,9), 'empl_status': 'A'},
      {'emplid': '1082441', 'position_nbr': '208989', 'effdt': datetime.date(2017,5,17), 'empl_status': 'W'},
      {'emplid': '1082441', 'position_nbr': '208989', 'effdt': datetime.date(2017,8,21), 'empl_status': 'W'},
      {'emplid': '1082441', 'position_nbr': '208989', 'effdt': datetime.date(2017,8,28), 'empl_status': 'A'},
    ],
  ]

  assert job_stints == expected_job_stints

  entries_2 = [
    {'emplid': '5150075', 'position_nbr': '258641', 'effdt': datetime.date(2015,4,6), 'empl_status': 'A'},
    {'emplid': '5150075', 'position_nbr': '258641', 'effdt': datetime.date(2015,5,25), 'empl_status': 'W'},
    {'emplid': '5150075', 'position_nbr': '258641', 'effdt': datetime.date(2015,7,27), 'empl_status': 'W'},
    {'emplid': '5150075', 'position_nbr': '258641', 'effdt': datetime.date(2015,5,25), 'empl_status': 'A'},
    {'emplid': '5150075', 'position_nbr': '258641', 'effdt': datetime.date(2015,8,31), 'empl_status': 'A'},
    {'emplid': '5150075', 'position_nbr': '258641', 'effdt': datetime.date(2015,8,31), 'empl_status': 'A'},
    {'emplid': '5150075', 'position_nbr': '258641', 'effdt': datetime.date(2015,8,31), 'empl_status': 'A'},
    {'emplid': '5150075', 'position_nbr': '258641', 'effdt': datetime.date(2016,1,1), 'empl_status': 'W'},
    {'emplid': '5150075', 'position_nbr': '258641', 'effdt': datetime.date(2016,5,30), 'empl_status': 'W'},
    {'emplid': '5150075', 'position_nbr': '258641', 'effdt': datetime.date(2016,7,25), 'empl_status': 'A'},
    {'emplid': '5150075', 'position_nbr': '258641', 'effdt': datetime.date(2016,8,29), 'empl_status': 'A'},
    {'emplid': '5150075', 'position_nbr': '258641', 'effdt': datetime.date(2016,8,29), 'empl_status': 'A'},
    {'emplid': '5150075', 'position_nbr': '258641', 'effdt': datetime.date(2017,5,29), 'empl_status': 'W'},
    {'emplid': '5150075', 'position_nbr': '258641', 'effdt': datetime.date(2017,7,24), 'empl_status': 'W'},
    {'emplid': '5150075', 'position_nbr': '258641', 'effdt': datetime.date(2017,8,28), 'empl_status': 'A'},
    {'emplid': '5150075', 'position_nbr': '258641', 'effdt': datetime.date(2017,8,28), 'empl_status': 'A'},
    {'emplid': '5150075', 'position_nbr': '258641', 'effdt': datetime.date(2017,8,28), 'empl_status': 'A'},
  ]

  job_stints_2 = employee_job.transform_job_entries(entries_2)

  expected_job_stints_2 = [
    [
      {'emplid': '5150075', 'position_nbr': '258641', 'effdt': datetime.date(2015,4,6), 'empl_status': 'A'},
      {'emplid': '5150075', 'position_nbr': '258641', 'effdt': datetime.date(2015,5,25), 'empl_status': 'W'},
      {'emplid': '5150075', 'position_nbr': '258641', 'effdt': datetime.date(2015,7,27), 'empl_status': 'W'},
      {'emplid': '5150075', 'position_nbr': '258641', 'effdt': datetime.date(2015,5,25), 'empl_status': 'A'},
      {'emplid': '5150075', 'position_nbr': '258641', 'effdt': datetime.date(2015,8,31), 'empl_status': 'A'},
      {'emplid': '5150075', 'position_nbr': '258641', 'effdt': datetime.date(2015,8,31), 'empl_status': 'A'},
      {'emplid': '5150075', 'position_nbr': '258641', 'effdt': datetime.date(2015,8,31), 'empl_status': 'A'},
      {'emplid': '5150075', 'position_nbr': '258641', 'effdt': datetime.date(2016,1,1), 'empl_status': 'W'},
      {'emplid': '5150075', 'position_nbr': '258641', 'effdt': datetime.date(2016,5,30), 'empl_status': 'W'},
      {'emplid': '5150075', 'position_nbr': '258641', 'effdt': datetime.date(2016,7,25), 'empl_status': 'A'},
      {'emplid': '5150075', 'position_nbr': '258641', 'effdt': datetime.date(2016,8,29), 'empl_status': 'A'},
      {'emplid': '5150075', 'position_nbr': '258641', 'effdt': datetime.date(2016,8,29), 'empl_status': 'A'},
      {'emplid': '5150075', 'position_nbr': '258641', 'effdt': datetime.date(2017,5,29), 'empl_status': 'W'},
      {'emplid': '5150075', 'position_nbr': '258641', 'effdt': datetime.date(2017,7,24), 'empl_status': 'W'},
      {'emplid': '5150075', 'position_nbr': '258641', 'effdt': datetime.date(2017,8,28), 'empl_status': 'A'},
      {'emplid': '5150075', 'position_nbr': '258641', 'effdt': datetime.date(2017,8,28), 'empl_status': 'A'},
      {'emplid': '5150075', 'position_nbr': '258641', 'effdt': datetime.date(2017,8,28), 'empl_status': 'A'},
    ],
  ]

  assert job_stints_2 == expected_job_stints_2

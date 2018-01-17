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

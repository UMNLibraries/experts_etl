import datetime, re
from importlib import import_module

import pandas as pd
import pytest

from experts_dw import db
from experts_etl.oit_to_edw import employee_job

all_emplids = [
    '4604830',
    '1082441',
    '3262322',
    '5150075',
    '1717940',
    '1217312',
    '1732812',
    '0903070',
    '2110507',
    '8000397',
    '1905842',
    '2898289',
]
employee_job_data = {
    emplid: import_module(f'..data.emplid_{emplid}.employee_jobs', package=__name__)
    for emplid in all_emplids
}

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

emplids_with_employee_job_entries_and_groups = [
    '4604830',
    '1082441',
    '3262322',
    '5150075',
    '1717940',
    '1217312',
    '1732812',
    '0903070',
    '2110507',
    '8000397',
    '1905842',
    '2898289',
]
@pytest.fixture(params=emplids_with_employee_job_entries_and_groups)
def employee_jobs_with_entries_and_groups(request):
    yield employee_job_data[request.param]

def test_group_entries(employee_jobs_with_entries_and_groups):
    assert employee_job.group_entries(employee_jobs_with_entries_and_groups.entries) == employee_jobs_with_entries_and_groups.entry_groups
    assert employee_job.group_entries([]) == []

emplids_with_complete_employee_job_data = [
    '4604830',
    '1082441',
    '3262322',
    '5150075',
    '1217312',
    '1732812',
    '0903070',
    '2110507',
    '8000397',
    '1905842',
    '2898289',
]
@pytest.fixture(params=emplids_with_complete_employee_job_data)
def complete_employee_job_data(request):
    yield employee_job_data[request.param]

def test_transform_entry_groups(session, complete_employee_job_data):
    assert employee_job.transform_entry_groups(session, complete_employee_job_data.entry_groups) == complete_employee_job_data.jobs
    assert employee_job.transform_entry_groups(session, []) == []

def test_transform(session, complete_employee_job_data):
    assert employee_job.transform(session, complete_employee_job_data.entries) == complete_employee_job_data.jobs
    assert employee_job.transform(session, []) == []

def test_myself(session):
    pass
    # This fails because some entries have deptid's that do not exist in the legacy dw:
    #jobs = employee_job.transform(session, entries_sets['1717940'].entries)
    #print(jobs)

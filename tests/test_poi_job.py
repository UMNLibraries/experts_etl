import datetime, re
from importlib import import_module

import pandas as pd
import pytest

from experts_dw import db
from experts_etl.oit_to_edw import poi_job

all_emplids = ['2898289','5575725','2927554','5231388','5491169',]
poi_job_data = {
    emplid: import_module(f'..data.emplid_{emplid}.poi_jobs', package=__name__)
    for emplid in all_emplids
}

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

@pytest.fixture(params=all_emplids)
def complete_poi_job_data(request):
    yield poi_job_data[request.param]

def test_group_entries(complete_poi_job_data):
    assert poi_job.group_entries(complete_poi_job_data.entries) == complete_poi_job_data.entry_groups
    assert poi_job.group_entries([]) == []

def test_transform_entry_groups(session, complete_poi_job_data):
    assert poi_job.transform_entry_groups(session, complete_poi_job_data.entry_groups) == complete_poi_job_data.jobs
    assert poi_job.transform_entry_groups(session, []) == []

def test_transform(session, complete_poi_job_data):
    assert poi_job.transform(session, complete_poi_job_data.entries) == complete_poi_job_data.jobs
    assert poi_job.transform(session, []) == []

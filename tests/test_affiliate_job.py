import datetime, re
from importlib import import_module

import pytest

from experts_dw import db
from experts_etl.oit_to_edw import affiliate_job

@pytest.fixture
def session():
    with db.session('hotel') as session:
      yield session

def test_extract(session):
    #emplid = '3367339' # Maybe use this later...
    emplid = '2585238'
    entries = affiliate_job.extract(session, emplid)
  
    assert isinstance(entries, list)
    for entry in entries:
        assert isinstance(entry, dict)
        assert entry['emplid'] == emplid
        assert re.match(r'^\d+$', entry['um_affiliate_id'])
        assert re.match(r'^\d+$', entry['deptid'])
        assert isinstance(entry['effdt'], datetime.datetime)

@pytest.fixture(params=['fake357','fake531','fake532','fake533','1905842',])
def job_entries(request):
    entries_sets = {}
    for emplid in ['fake357','fake531','fake532','fake533','1905842',]:
        entries_sets[emplid] = import_module(f'..data.emplid_{emplid}.affiliate_jobs', package=__name__)

    entries_set = entries_sets[request.param]
    yield entries_set

def test_group_entries(job_entries):
    assert affiliate_job.group_entries(job_entries.entries) == job_entries.entry_groups

def test_group_entries_by_dept_affiliate_id_and_jobcode(job_entries):
    assert affiliate_job.group_entries_by_dept_affiliate_id_and_jobcode(job_entries.entries) == job_entries.entries_by_dept_affiliate_id_and_jobcode

def test_transform(session, job_entries):
    assert affiliate_job.transform(session, job_entries.entries) == job_entries.jobs

def test_extract_transform(session):
    jobs = affiliate_job.extract_transform(session, '1173706')
  
    expected_jobs = [
        {
            'affiliation_id': '9403A',
            'deptid': '11735',
            'um_campus': 'TXXX',
            'org_id': 'WSSKOZQ',
            'job_title': 'Adjunct Assistant Professor',
            'job_description': 'Adjunct Assistant Professor',
            'employment_type': 'adjunct_faculty',
            'staff_type': 'nonacademic',
            'start_date': datetime.datetime(2015,4,6,0,0),
            'end_date': datetime.datetime(2015,4,7,0,0),
            'visibility': 'Restricted',
            'profiled': False,
        },
        {
            'affiliation_id': '9401',
            'deptid': '11800',
            'um_campus': 'TXXX',
            'org_id': 'DBXNQ',
            'job_title': 'Professor',
            'job_description': 'Professor',
            'employment_type': 'medical_school_affiliate',
            'staff_type': 'nonacademic',
            'start_date': datetime.datetime(2015,4,6,0,0),
            'end_date': None,
            'visibility': 'Restricted',
            'profiled': False,
        },
    ]
  
    assert jobs == expected_jobs

from datetime import datetime

entries = [
    {
        'emplid': 'fake357',
        'deptid': '11735',
        'um_campus': 'TXXX',
        'title': 'Adjunct Assistant Professor',
        'um_affiliate_id': '01',
        'um_affil_relation': '9403A',
        'effdt': datetime(2015,4,6,0,0),
        'status': 'A',
        'status_flg': 'H',
    },
    {
        'emplid': 'fake357',
        'deptid': '11735',
        'um_campus': 'TXXX',
        'title': 'Adjunct Assistant Professor',
        'um_affiliate_id': '01',
        'um_affil_relation': '9403A',
        'effdt': datetime(2015,4,7,0,0),
        'status': 'I',
        'status_flg': 'C',
    },
]

entries_by_dept_affiliate_id_and_jobcode = [
    {
        'deptid': '11735',
        'um_affiliate_id': '01',
        'um_affil_relation': '9403A',
        'entries': entries,
    },
]

entry_groups = [
    {
        'deptid': '11735',
        'um_affil_relation': '9403A',
        'start_date': datetime(2015,4,6,0,0),
        'entries': entries,
    },
]

jobs = [
    {
       'affiliation_id': '9403A',
       'deptid': '11735',
       'um_campus': 'TXXX',
       'org_id': 'WSSKOZQ',
       'job_title': 'Adjunct Assistant Professor',
       'job_description': 'Adjunct Assistant Professor',
       'employment_type': 'adjunct_faculty',
       'staff_type': 'nonacademic',
       'start_date': datetime(2015,4,6,0,0),
       'end_date': datetime(2015,4,7,0,0),
       'visibility': 'Restricted',
       'profiled': False,
    },
]

from datetime import datetime

entries = [
    {
        'emplid': 'fake531',
        'deptid': '11219',
        'um_campus': 'TXXX',
        'title': 'Adjunct Associate Professor',
        'um_affiliate_id': '03',
        'um_affil_relation': '9402A',
        'effdt': datetime(2015,4,6,0,0),
        'status': 'A',
        'status_flg': 'H',
    },
    {
        'emplid': 'fake531',
        'deptid': '11219',
        'um_campus': 'TXXX',
        'title': 'Adjunct Associate Professor',
        'um_affiliate_id': '03',
        'um_affil_relation': '9402A',
        'effdt': datetime(2017,1,12,0,0),
        'status': 'A',
        'status_flg': 'H',
    },
]

entries_by_dept_affiliate_id_and_jobcode = [
    {
        'deptid': '11219',
        'um_affiliate_id': '03',
        'um_affil_relation': '9402A',
        'entries': entries,
    },
]

entry_groups = [
    {
        'deptid': '11219',
        'um_affil_relation': '9402A',
        'start_date': datetime(2015,4,6,0,0),
        'entries': entries,
    },
]

jobs = [
    {
        'affiliation_id': '9402A',
        'deptid': '11219',
        'um_campus': 'TXXX',
        'org_id': 'LBZUCPBF',
        'job_title': 'Adjunct Associate Professor',
        'job_description': 'Adjunct Associate Professor',
        'employment_type': 'adjunct_faculty',
        'staff_type': 'nonacademic',
        'start_date': datetime(2015,4,6,0,0),
        'end_date': datetime(2017,1,12,0,0),
        'visibility': 'Restricted',
        'profiled': False,
    },
]

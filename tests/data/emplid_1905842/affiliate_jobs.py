from datetime import datetime

entries = [
    {
        'emplid': '1905842',
        'deptid': '11828',
        'um_campus': 'TXXX',
        'title': 'Assistant Professor',
        'um_affiliate_id': '01',
        'um_affil_relation': '9403',
        'effdt': datetime(2015,10,6,0,0),
        'status': 'A',
        'status_flg': 'H'
    },
    {
        'emplid': '1905842',
        'deptid': '11828',
        'um_campus': 'TXXX',
        'title': 'Assistant Professor',
        'um_affiliate_id': '01',
        'um_affil_relation': '9403',
        'effdt': datetime(2016,6,13,0,0),
        'status': 'A',
        'status_flg': 'H'
    },
    {
        'emplid': '1905842',
        'deptid': '11828',
        'um_campus': 'TXXX',
        'title': 'Assistant Professor Joint Appt',
        'um_affiliate_id': '01',
        'um_affil_relation': '9403',
        'effdt': datetime(2016,6,14,0,0),
        'status': 'A',
        'status_flg': 'H'
    },
    {
        'emplid': '1905842',
        'deptid': '11828',
        'um_campus': 'TXXX',
        'title': 'Assistant Professor Joint Appt',
        'um_affiliate_id': '01',
        'um_affil_relation': '9403',
        'effdt': datetime(2016,7,27,0,0),
        'status': 'A',
        'status_flg': 'H'
    },
    {
        'emplid': '1905842',
        'deptid': '11828',
        'um_campus': 'TXXX',
        'title': 'Assistant Professor Dual Appt',
        'um_affiliate_id': '01',
        'um_affil_relation': '9403',
        'effdt': datetime(2019,6,18,0,0),
        'status': 'A',
        'status_flg': 'H'
    },
    {
        'emplid': '1905842',
        'deptid': '11770',
        'um_campus': 'TXXX',
        'title': 'Assistant Professor Dual Appt',
        'um_affiliate_id': '01',
        'um_affil_relation': '9403',
        'effdt': datetime(2021,9,21,0,0),
        'status': 'A',
        'status_flg': 'H'
    },
    {
        'emplid': '1905842',
        'deptid': '11771',
        'um_campus': 'TXXX',
        'title': 'Assistant Professor Dual Appt',
        'um_affiliate_id': '01',
        'um_affil_relation': '9403',
        'effdt': datetime(2022,6,2,0,0),
        'status': 'A',
        'status_flg': 'H'
    },
    {
        'emplid': '1905842',
        'deptid': '11771',
        'um_campus': 'TXXX',
        'title': 'Assistant Professor Dual Appt',
        'um_affiliate_id': '01',
        'um_affil_relation': '9403',
        'effdt': datetime(2023,4,30,0,0),
        'status': 'I',
        'status_flg': 'H'
    },
    {
        'emplid': '1905842',
        'deptid': '11771',
        'um_campus': 'TXXX',
        'title': 'Assistant Professor',
        'um_affiliate_id': '01',
        'um_affil_relation': '9403',
        'effdt': datetime(2023,5,1,0,0),
        'status': 'A',
        'status_flg': 'C'
    },
]

entries_by_dept_affiliate_id_and_jobcode = [
    {
        'deptid': '11770',
        'um_affiliate_id': '01',
        'um_affil_relation': '9403',
        'entries': entries[5:6],
    },
    {
        'deptid': '11771',
        'um_affiliate_id': '01',
        'um_affil_relation': '9403',
        'entries': entries[6:],
    },
    {
        'deptid': '11828',
        'um_affiliate_id': '01',
        'um_affil_relation': '9403',
        'entries': entries[:5],
    },
]

entry_groups = [
    {
        'deptid': '11770',
        'um_affil_relation': '9403',
        'start_date': datetime(2021,9,21,0,0),
        'entries': entries[5:6],
    },
    {
        'deptid': '11771',
        'um_affil_relation': '9403',
        'start_date': datetime(2022,6,2,0,0),
        'entries': entries[6:8],
    },
    {
        'deptid': '11771',
        'um_affil_relation': '9403',
        'start_date': datetime(2023,5,1,0,0),
        'entries': entries[8:],
    },
    {
        'deptid': '11828',
        'um_affil_relation': '9403',
        'start_date': datetime(2015,10,6,0,0),
        'entries': entries[:5],
    },
]

jobs = [
    {
        'affiliation_id': '9403',
        'deptid': '11770',
        'um_campus': 'TXXX',
        'org_id': 'WGLMQKYUA',
        'job_title': 'Assistant Professor Dual Appt',
        'job_description': 'Assistant Professor',
        'employment_type': 'medical_school_affiliate',
        'staff_type': 'nonacademic',
        'start_date': datetime(2021,9,21,0,0),
        'end_date': datetime(2021,9,21,0,0),
        'visibility': 'Restricted',
        'profiled': False,
    },
    {
        'affiliation_id': '9403',
        'deptid': '11771',
        'um_campus': 'TXXX',
        'org_id': 'TCHOSPMED',
        'job_title': 'Assistant Professor Dual Appt',
        'job_description': 'Assistant Professor',
        'employment_type': 'medical_school_affiliate',
        'staff_type': 'nonacademic',
        'start_date': datetime(2022,6,2,0,0),
        'end_date': datetime(2023,4,30,0,0),
        'visibility': 'Restricted',
        'profiled': False,
    },
    {
        'affiliation_id': '9403',
        'deptid': '11771',
        'um_campus': 'TXXX',
        'org_id': 'TCHOSPMED',
        'job_title': 'Assistant Professor',
        'job_description': 'Assistant Professor',
        'employment_type': 'medical_school_affiliate',
        'staff_type': 'nonacademic',
        'start_date': datetime(2023,5,1,0,0),
        'end_date': None,
        'visibility': 'Restricted',
        'profiled': False,
    },
    {
        'affiliation_id': '9403',
        'deptid': '11828',
        'um_campus': 'TXXX',
        'org_id': 'UGYBLAXO',
        'job_title': 'Assistant Professor Dual Appt',
        'job_description': 'Assistant Professor',
        'employment_type': 'medical_school_affiliate',
        'staff_type': 'nonacademic',
        'start_date': datetime(2015,10,6,0,0),
        'end_date': datetime(2019,6,18,0,0),
        'visibility': 'Restricted',
        'profiled': False,
    },
]

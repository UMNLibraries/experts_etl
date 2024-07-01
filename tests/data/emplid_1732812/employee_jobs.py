from datetime import datetime

entries = [
    {
        'emplid': '1732812',
        'empl_rcdno': '0',
        'effdt': datetime(2015,4,6,0,0),
        'effseq': '0',
        'position_nbr': ' ', # This is real data from ps_dwhr_job!
        'jobcode': '9401',
        'jobcode_descr': 'Professor',
        'empl_status': 'R',
        'deptid': '654A',
        'um_campus': None, # Also real data. um_college was also null, which caused exclusion from our views.
        'status_flg': 'C',
        'job_terminated': 'Y',
        'last_date_worked': datetime(2002,6,2,0,0),
        'job_entry_dt': datetime(1994,9,16,0,0),
        'position_entry_dt': None
    },
]

entry_groups = [
    {
        'position_nbr': ' ',
        'job_entry_dt': datetime(1994,9,16,0,0),
        'jobcode': '9401',
        'deptid': '654A',
        'entries': entries,
    },
]

jobs = [
    {
        'affiliation_id': '9401',
        'deptid': '654A',
        'um_campus': None,
        'org_id': 'MEYXRZM',
        'empl_rcdno': '0',
        'job_title': 'Professor',
        'job_description': 'Professor',
        'employment_type': 'faculty',
        'staff_type': 'nonacademic',
        'start_date': datetime(1994,9,16,0,0),
        'end_date': datetime(2002,6,2,0,0),
        'visibility': 'Restricted',
        'profiled': False,
    },
]

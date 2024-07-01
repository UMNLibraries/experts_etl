from datetime import datetime

entries = [
    {
        'emplid': '5575725',
        'empl_rcdno': '0',
        'effdt': datetime(2019,8,26,0,0),
        'effseq': '0',
        'position_nbr': '295034',
        'jobcode': '9561',
        'jobcode_descr': 'Graduate School Fellow',
        'empl_status': 'A',
        'deptid': '11130',
        'um_campus': 'TXXX',
        'status_flg': 'H',
        'job_entry_dt': datetime(2019,8,26,0,0),
        'position_entry_dt': datetime(2019,8,26,0,0),
    },
    {
        'emplid': '5575725',
        'empl_rcdno': '0',
        'effdt': datetime(2019,10,3,0,0),
        'effseq': '0',
        'position_nbr': '295034',
        'jobcode': '9561',
        'jobcode_descr': 'Graduate School Fellow',
        'empl_status': 'A',
        'deptid': '11130',
        'um_campus': 'TXXX',
        'status_flg': 'C',
        'job_entry_dt': datetime(2019,8,26,0,0),
        'position_entry_dt': datetime(2019,8,26,0,0),
    },
]

entry_groups = [
    {
        'position_nbr': '295034',
        'job_entry_dt': datetime(2019,8,26,0,0),
        'jobcode': '9561',
        'deptid': '11130',
        'entries': entries,
    },
]

jobs = [
    {
        'affiliation_id': '9561',
        'deptid': '11130',
        'um_campus': 'TXXX',
        'org_id': 'IHRBIHRB',
        'empl_rcdno': '0',
        'job_title': 'Graduate School Fellow',
        'job_description': 'Graduate School Fellow',
        'employment_type': 'student',
        'staff_type': 'nonacademic',
        'start_date': datetime(2019,8,26,0,0),
        'end_date': None,
        'visibility': 'Restricted',
        'profiled': False,
    },
]

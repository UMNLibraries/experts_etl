from datetime import datetime

entries = [
    {
        'emplid': '0903070',
        'empl_rcdno': '0',
        'effdt': datetime(2015,4,6,0,0),
        'effseq': '0',
        'position_nbr': '212651',
        'jobcode': '9337',
        'jobcode_descr': 'Departmental Director',
        'empl_status': 'L',
        'deptid': '10919',
        'um_campus': None, # Real data. um_college was also null, which caused exclusion from our views.
        'status_flg': 'H',
        'job_terminated': 'Y',
        'last_date_worked': datetime(2009,2,10,0,0),
        'job_entry_dt': datetime(2004,1,26,0,0),
        'position_entry_dt': datetime(2007,2,7,0,0),
    },
    {
        'emplid': '0903070',
        'empl_rcdno': '0',
        'effdt': datetime(2017,3,20,0,0),
        'effseq': '0',
        'position_nbr': '212651',
        'jobcode': '9337',
        'jobcode_descr': 'Departmental Director',
        'empl_status': 'R',
        'deptid': '10919',
        'um_campus': None,
        'status_flg': 'C',
        'job_terminated': 'Y',
        'last_date_worked': datetime(2009,2,10,0,0),
        'job_entry_dt': datetime(2004,1,26,0,0),
        'position_entry_dt': datetime(2007,2,7,0,0),
    },
]

entry_groups = [
    {
        'position_nbr': '212651',
        'job_entry_dt': datetime(2004,1,26,0,0),
        'jobcode': '9337',
        'deptid': '10919',
        'entries': entries,
    },
]

jobs = [
    {
        'affiliation_id': '9337',
        'deptid': '10919',
        'um_campus': None,
        'org_id': 'ZZGBZWTLKBRH',
        'empl_rcdno': '0',
        'job_title': 'Departmental Director',
        'job_description': 'Department Director',
        'employment_type': 'exec_admin_manage',
        'staff_type': 'nonacademic',
        'start_date': datetime(2004,1,26,0,0),
        'end_date': datetime(2009,2,10,0,0),
        'visibility': 'Restricted',
        'profiled': False,
    },
]

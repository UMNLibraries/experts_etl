from datetime import datetime

entries = [
    {
          'emplid': '4604830',
          'empl_rcdno': '0',
          'jobcode': '9742S2',
          'jobcode_descr': 'Research Project Specialist 2',
          'deptid': '11179',
          'um_campus': 'TXXX',
          'position_nbr': '233949',
          'effdt': datetime(2015,9,7,0,0),
          'effseq': '0',
          'job_entry_dt': datetime(2015,9,7,0,0),
          'position_entry_dt': datetime(2011,9,28,0,0),
          'last_date_worked': None,
          'empl_status': 'A',
          'job_terminated': 'N',
          'status_flg': 'H',
    },
    {
        'emplid': '4604830',
        'empl_rcdno': '0',
        'jobcode': '9341A1',
        'jobcode_descr': 'Admin Consultant/Analyst 1',
        'deptid': '11743',
        'um_campus': 'TXXX',
        'position_nbr': '284794',
        'effdt': datetime(2017,4,17,0,0),
        'effseq': '0',
        'job_entry_dt': datetime(2017,4,17,0,0),
        'position_entry_dt': datetime(2015,9,21,0,0),
        'last_date_worked': None,
        'empl_status': 'A',
        'job_terminated': 'N',
        'status_flg': 'H',
    },
    {
        'emplid': '4604830',
        'empl_rcdno': '0',
        'jobcode': '9341A1',
        'jobcode_descr': 'Admin Consultant/Analyst 1',
        'deptid': '11743',
        'um_campus': 'TXXX',
        'position_nbr': '284794',
        'effdt': datetime(2017,6,12,0,0),
        'effseq': '0',
        'job_entry_dt': datetime(2017,4,17,0,0),
        'position_entry_dt': datetime(2015,9,21,0,0),
        'last_date_worked': None,
        'empl_status': 'A',
        'job_terminated': 'N',
        'status_flg': 'H',
    },
    {
        'emplid': '4604830',
        'empl_rcdno': '0',
        'jobcode': '9341A1',
        'jobcode_descr': 'Admin Consultant/Analyst 1',
        'deptid': '11743',
        'um_campus': 'TXXX',
        'position_nbr': '284794',
        'effdt': datetime(2017,6,12,0,0),
        'effseq': '1',
        'job_entry_dt': datetime(2017,4,17,0,0),
        'position_entry_dt': datetime(2015,9,21,0,0),
        'last_date_worked': None,
        'empl_status': 'A',
        'job_terminated': 'N',
        'status_flg': 'H',
    },
    {
        'emplid': '4604830',
        'empl_rcdno': '0',
        'jobcode': '9341A1',
        'jobcode_descr': 'Admin Consultant/Analyst 1',
        'deptid': '11743',
        'um_campus': 'TXXX',
        'position_nbr': '284794',
        'effdt': datetime(2017,8,7,0,0),
        'effseq': '0',
        'job_entry_dt': datetime(2017,4,17,0,0),
        'position_entry_dt': datetime(2015,9,21,0,0),
        'last_date_worked': None,
        'empl_status': 'A',
        'job_terminated': 'N',
        'status_flg': 'H',
    },
    {
        'emplid': '4604830',
        'empl_rcdno': '0',
        'jobcode': '9341A1',
        'jobcode_descr': 'Admin Consultant/Analyst 1',
        'deptid': '11743',
        'um_campus': 'TXXX',
        'position_nbr': '284794',
        'effdt': datetime(2018,6,11,0,0),
        'effseq': '0',
        'job_entry_dt': datetime(2017,4,17,0,0),
        'position_entry_dt': datetime(2015,9,21,0,0),
        'last_date_worked': None,
        'empl_status': 'A',
        'job_terminated': 'N',
        'status_flg': 'H',
    },
    {
        'emplid': '4604830',
        'empl_rcdno': '0',
        'jobcode': '9341A1',
        'jobcode_descr': 'Admin Consultant/Analyst 1',
        'deptid': '11743',
        'um_campus': 'TXXX',
        'position_nbr': '284794',
        'effdt': datetime(2018,6,11,0,0),
        'effseq': '1',
        'job_entry_dt': datetime(2017,4,17,0,0),
        'position_entry_dt': datetime(2015,9,21,0,0),
        'last_date_worked': None,
        'empl_status': 'A',
        'job_terminated': 'N',
        'status_flg': 'C',
    },
]

entry_groups = [
    {
        'position_nbr': '233949',
        'job_entry_dt': datetime(2015,9,7,0,0),
        'jobcode': '9742S2',
        'deptid': '11179',
        'entries': entries[0:1],
    },
    {
        'position_nbr': '284794',
        'job_entry_dt': datetime(2017,4,17,0,0),
        'jobcode': '9341A1',
        'deptid': '11743',
        'entries': entries[1:],
    },
]

jobs = [
    {
        'affiliation_id': '9742S2',
        'deptid': '11179',
        'um_campus': 'TXXX',
        'org_id': 'PIXEZPPAPIRGQ',
        'empl_rcdno': '0',
        'job_title': 'Research Project Specialist 2',
        'job_description': 'Research Project Specialist',
        'employment_type': 'research_support',
        'staff_type': 'nonacademic',
        #'start_date': datetime(2011,9,28,0,0), # Maybe this should be the start_date?
        'start_date': datetime(2015,9,7,0,0),
        'end_date': datetime(2015,9,7,0,0),
        'visibility': 'Restricted',
        'profiled': False,
    },
    {
        'affiliation_id': '9341A1',
        'deptid': '11743',
        'um_campus': 'TXXX',
        'org_id': 'WPZZYUA',
        'empl_rcdno': '0',
        'job_title': 'Admin Consultant/Analyst 1',
        'job_description': 'Consultant/Analyst',
        'employment_type': 'academic_administrative',
        'staff_type': 'nonacademic',
        'start_date': datetime(2017,4,17,0,0),
        'end_date': None,
        'visibility': 'Restricted',
        'profiled': False,
    },
]

import datetime

entries = [
  {
    'emplid': '5575725',
    'empl_rcdno': '0',
    'effdt': datetime.datetime(2019,8,26,0,0),
    'effseq': '0',
    'position_nbr': '295034',
    'jobcode': '9561',
    'jobcode_descr': 'Graduate School Fellow',
    'empl_status': 'A',
    'deptid': '11130',
    'um_campus': 'TXXX',
    'status_flg': 'H',
    'job_entry_dt': datetime.datetime(2019,8,26,0,0),
    'position_entry_dt': datetime.datetime(2019,8,26,0,0),
  },
  {
    'emplid': '5575725',
    'empl_rcdno': '0',
    'effdt': datetime.datetime(2019,10,3,0,0),
    'effseq': '0',
    'position_nbr': '295034',
    'jobcode': '9561',
    'jobcode_descr': 'Graduate School Fellow',
    'empl_status': 'A',
    'deptid': '11130',
    'um_campus': 'TXXX',
    'status_flg': 'C',
    'job_entry_dt': datetime.datetime(2019,8,26,0,0),
    'position_entry_dt': datetime.datetime(2019,8,26,0,0),
  },
]

entry_groups = [
  {
    'position_nbr': '295034',
    'job_entry_dt': datetime.datetime(2019,8,26,0,0),
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
    'start_date': datetime.datetime(2019,8,26,0,0),
    'end_date': None,
    'visibility': 'Restricted',
    'profiled': False,
  },
]

# TODO: Not sure what to do with primary jobs in these cases yet:
#jobs_with_primary = []
#jobs_with_primary.append({**jobs[-1], **{'primary': True}})
#
## Set according to the primary job:
#transformed_profiled = False

#jobs_with_transformed_staff_type = []
#for job in jobs_with_primary:
#  jobs_with_transformed_staff_type.append({**job, **{'staff_type': 'nonacademic'}})
#
#jobs_with_staff_org_assoc_id = [
#  {**jobs_with_primary[3], **{'staff_org_assoc_id': 'autoid:6030-PIXEZPPAPIRGQ-Administrative Manager 2-exec_admin_manage-2017-07-10'}},
#  {**jobs_with_primary[1], **{'staff_org_assoc_id': 'autoid:6030-PIXEZPPAPIRGQ-Senior Research Fellow-researcher-2000-11-22'}},
#  {**jobs_with_primary[2], **{'staff_org_assoc_id': 'autoid:6030-PIXEZPPAPIRGQ-Post-Doctoral Fellow-researcher-2015-09-07',}}
#]

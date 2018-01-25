import re
from experts_dw import db
from experts_dw.models import PureEligibleEmpJobChngHst, PureNewStaffDeptDefaults, PureNewStaffPosDefaults, UmnDeptPureOrg
from sqlalchemy import and_

session = db.session('hotel')

def extract_transform(emplid):
  return transform(extract(emplid))

def extract(emplid):
  jobs = []
  for job in session.query(PureEligibleEmpJobChngHst).filter(PureEligibleEmpJobChngHst.emplid == emplid).order_by(PureEligibleEmpJobChngHst.effdt, PureEligibleEmpJobChngHst.effseq):
    jobs.append(
      {c.name: getattr(job, c.name) for c in job.__table__.columns}
    )

  return jobs

"""
status_flag values:
C Current 
F Future (We exclude these entries in the SQL views.)
H Historical

empl_status values:
A Active 
D Deceased 
L Leave of Absence 
P Leave With Pay 
Q Retired With Pay 
R Retired 
S Suspended 
T Terminated 
U Terminated With Pay 
V Terminated Pension Pay Out 
W Short Work Break 
X Retired-Pension Administration
"""
#active_states = ['A', 'L', 'S', 'W']
active_states = ['A', 'L', 'P', 'W']

def transform(jobs):
  jobs_by_position_nbr = group_by_position_nbr(jobs)
  transformed_jobs = []

  for position_nbr, entries in jobs_by_position_nbr.items():
    job_stints = transform_job_entries(entries)

    for job_stint in job_stints:
      transformed_job = transform_job_stint(job_stint)
      transformed_jobs.append(transformed_job)
      
  return transformed_jobs

def transform_job_stint(job_stint):
  transformed_job = {}
  first_entry, last_entry = job_stint[0], job_stint[-1]
  transformed_job['job_title'] = last_entry['jobcode_descr']
  transformed_job['deptid'] = last_entry['deptid']
  transformed_job['empl_rcdno'] = last_entry['empl_rcdno']

  potential_start_dates = [dt for dt in (first_entry['effdt'],first_entry['job_entry_dt'],first_entry['position_entry_dt']) if dt]
  transformed_job['start_date'] = min(potential_start_dates)

  transformed_job['staff_org_assoc_id'] = '{}-{}-{}-{}'.format(
    last_entry['emplid'],
    last_entry['deptid'],
    last_entry['jobcode'],
    transformed_job['start_date'].strftime('%Y-%m-%d')
  )

  if last_entry['empl_status'] not in active_states or last_entry['job_terminated'] == 'Y':
    potential_end_dates = [dt for dt in (last_entry['effdt'],last_entry['last_date_worked']) if dt]
    transformed_job['end_date'] = max(potential_end_dates)
    transformed_job['visibility'] = 'Restricted'
    transformed_job['profiled'] = False
  else:
    transformed_job['end_date'] = None
    pure_new_staff_dept_defaults = (
      session.query(PureNewStaffDeptDefaults)
      .filter(and_(
        PureNewStaffDeptDefaults.deptid == last_entry['deptid'],
        PureNewStaffDeptDefaults.jobcode == last_entry['jobcode'],
        PureNewStaffDeptDefaults.jobcode_descr == last_entry['jobcode_descr'],
      ))
      .one_or_none()
    )
    if pure_new_staff_dept_defaults:
      transformed_job['visibility'] = pure_new_staff_dept_defaults.default_visibility
      if pure_new_staff_dept_defaults.default_profiled == 'true':
        transformed_job['profiled'] = True
      else:
        transformed_job['profiled'] = False
    else:
      transformed_job['visibility'] = 'Restricted'
      transformed_job['profiled'] = False

  umn_dept_pure_org = (
    session.query(UmnDeptPureOrg)
    .filter(UmnDeptPureOrg.umn_dept_id == last_entry['deptid'])
    .one_or_none()
  )
  if umn_dept_pure_org:
    transformed_job['org_id'] = umn_dept_pure_org.pure_org_id
  else:
    transformed_job['org_id'] = None

  pure_new_staff_pos_defaults = (
    session.query(PureNewStaffPosDefaults)
    .filter(PureNewStaffPosDefaults.jobcode == last_entry['jobcode'])
    .one_or_none()
  )
  if pure_new_staff_pos_defaults:
    transformed_job['employment_type'] = pure_new_staff_pos_defaults.default_employed_as
    transformed_job['staff_type'] = pure_new_staff_pos_defaults.default_staff_type
  else:
    transformed_job['employment_type'] = None
    transformed_job['staff_type'] = None

  return transformed_job

def transform_job_entries(entries):
  job_stints = []
  current_stint = []
  current_stint_ending = False

  for entry in entries:
    if current_stint_ending:
      if entry['empl_status'] in active_states:
        # We've passed the end of the current stint, and this is a new stint in the same position.
        job_stints.append(current_stint)
        current_stint = []
        current_stint_ending = False
      current_stint.append(entry)
      continue

    if entry['empl_status'] not in active_states:
      # This is the first entry with an inactive state for this stint, so it's ending.
      # Other entries with inactive states may follow.
      current_stint_ending = True
    current_stint.append(entry)

  if len(current_stint) > 0:
    job_stints.append(current_stint)

  return job_stints

def group_by_position_nbr(jobs):
  jobs_by_position_nbr = {}
  for job in jobs:
    position_nbr = job['position_nbr']
    if position_nbr not in jobs_by_position_nbr:
      jobs_by_position_nbr[position_nbr] = [] 
    jobs_by_position_nbr[position_nbr].append(job)
  return jobs_by_position_nbr

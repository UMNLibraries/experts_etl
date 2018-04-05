import pandas as pd
import re
from experts_dw import db
from experts_dw.models import PureEligibleEmpJob, PureNewStaffDeptDefaults, PureNewStaffPosDefaults, UmnDeptPureOrg
from sqlalchemy import and_

session = db.session('hotel')

def extract_transform(emplid):
  return transform(extract(emplid))

def extract(emplid):
  jobs = []
  for job in session.query(PureEligibleEmpJob).filter(PureEligibleEmpJob.emplid == emplid).order_by(PureEligibleEmpJob.effdt, PureEligibleEmpJob.effseq):
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
  transformed_jobs = []

  if len(jobs) == 0:
    return transformed_jobs

  jobs_by_position_nbr = group_by_position_nbr(jobs)

  for position_nbr, entries in jobs_by_position_nbr.items():
    job_stints = transform_job_entries(entries)

    for job_stint in job_stints:
      transformed_job = transform_job_stint(job_stint)
      transformed_jobs.append(transformed_job)
      
  return transformed_jobs

def transform_job_stint(job_stint):
  transformed_job = {}

  if len(job_stint) == 0:
    return transformed_job

  first_entry = job_stint[0]

  # For the last entry, find the latest current (C) entry, if there is one:
  last_entry = None
  reversed_job_stint = job_stint.copy()
  reversed_job_stint.reverse()
  for entry in reversed_job_stint:
    if entry['status_flg'] == 'C':
      last_entry = entry
      break
  if not last_entry:
    last_entry = job_stint[-1]

  transformed_job['job_title'] = last_entry['jobcode_descr']
  transformed_job['deptid'] = last_entry['deptid']
  transformed_job['empl_rcdno'] = last_entry['empl_rcdno']

  potential_start_dates = [dt for dt in (first_entry['effdt'],first_entry['job_entry_dt'],first_entry['position_entry_dt']) if dt]
  transformed_job['start_date'] = min(potential_start_dates)

  if last_entry['empl_status'] not in active_states or last_entry['job_terminated'] == 'Y' or last_entry['status_flg'] == 'H':
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

  transformed_job['org_id'] = org_id(last_entry['deptid'])

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

def org_id(deptid):
  org_id = None
  # Some old deptids include letters, but umn_dept_pure_org.umn_dept_id is a number.
  # The old ones won't be in that table, anyway, so just skip those:
  if re.match('^\d+$', deptid):
    umn_dept_pure_org = (
      session.query(UmnDeptPureOrg)
      .filter(UmnDeptPureOrg.umn_dept_id == deptid)
      .one_or_none()
    )
    if umn_dept_pure_org:
      org_id = umn_dept_pure_org.pure_org_id
  return org_id

def transform_job_entries(entries):
  #df = pd.DataFrame(data=entries)
  #jobcodes = df.jobcode.unique()

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

  if len(jobs) == 0:
    # Avoid attempts to operate on an empty DataFrame below, which will generate errors:
    return jobs_by_position_nbr

  df = pd.DataFrame(data=jobs)
  for position_nbr in df.position_nbr.unique():
    position_nbr_selector = df['position_nbr'] == position_nbr
    jobs_by_position_nbr[position_nbr] = df_to_dicts(df[position_nbr_selector])

  return jobs_by_position_nbr

def neighborhood(iterable):
  iterator = iter(iterable)
  prev_item = None
  curr_item = next(iterator)  # throws StopIteration if empty.
  for next_item in iterator:
    yield (prev_item, curr_item, next_item)
    prev_item, curr_item = curr_item, next_item
  yield (prev_item, curr_item, None)

def transform_entry_groups(entry_groups):
  if len(entry_groups) == 0:
    return []

  jobs = []
  for prev_group, curr_group, next_group in neighborhood(entry_groups):
    job = {
      'start_date': curr_group['job_entry_dt'],
      'deptid': curr_group['deptid'],
    }
    curr_df = pd.DataFrame(data=curr_group['entries'])
    curr_df_with_current_status = curr_df[curr_df['status_flg'] == 'C']

def group_entries(entries):
  if len(entries) == 0:
    return []

  df = pd.DataFrame(data=entries)
  position_nbr_groups = df.groupby(['position_nbr'])
  
  entry_groups = []
  for position_nbr, position_entry_rows in position_nbr_groups:
    position_entry_rows.sort_values(['effdt','effseq'])
    position_entry_dicts = df_to_dicts(position_entry_rows)
    for entry in position_entry_dicts:
      if entry_matches_last_group(entry, entry_groups):
        entry_groups[-1]['entries'].append(entry)
        continue
      entry_group = {k:entry[k] for k in ('position_nbr','job_entry_dt','jobcode','deptid')}
      entry_group['entries'] = [entry]
      entry_groups.append(entry_group)

  return entry_groups

def entry_matches_last_group(entry_dict, entry_groups):
  if len(entry_groups) > 0:
    last_entry_group = entry_groups[-1]
    if all(entry_dict[k] == last_entry_group[k] for k in ('position_nbr','job_entry_dt','jobcode','deptid')):
      return True
  return False

def df_to_dicts(df):
  dicts = df.to_dict('records')
  for d in dicts:
    for k, v in d.items():
      if pd.isnull(v):
        # Convert all Pandas NaN's, NaT's etc to None:
        d[k] = None
      if k not in ['effdt','action_dt','job_entry_dt','dept_entry_dt','position_entry_dt','last_date_worked']:
        # Skip anything that's not a datetime:
        continue
      if d[k] is not None:
        d[k] = d[k].to_pydatetime()
  return dicts

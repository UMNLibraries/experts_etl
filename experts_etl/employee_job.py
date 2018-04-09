import pandas as pd
import re
from experts_dw import db
from experts_dw.models import PureEligibleEmpJob, PureNewStaffDeptDefaults, PureNewStaffPosDefaults, UmnDeptPureOrg
from sqlalchemy import and_

session = db.session('hotel')

def extract_transform(emplid):
  return transform(extract(emplid))

def extract(emplid):
  entries = []
  for entry in session.query(PureEligibleEmpJob).filter(PureEligibleEmpJob.emplid == emplid).order_by(PureEligibleEmpJob.effdt, PureEligibleEmpJob.effseq):
    entries.append(
      {c.name: getattr(entry, c.name) for c in entry.__table__.columns}
    )

  return entries

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

def transform(entries):
  jobs = []

  if len(entries) == 0:
    return jobs

  entry_groups = group_entries(entries)
  jobs = transform_entry_groups(entry_groups)
      
  return jobs

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
  else:
    transformed_job['end_date'] = None

  dept_defaults = new_staff_dept_defaults(
    end_date=transformed_job['end_date'],
    deptid=last_entry['deptid'],
    jobcode=last_entry['jobcode'],
    jobcode_descr=last_entry['jobcode_descr'],
  )
  transformed_job['visibility'] = dept_defaults['visibility']
  transformed_job['profiled'] = dept_defaults['profiled']

  transformed_job['org_id'] = org_id(last_entry['deptid'])

  position_defaults = new_staff_position_defaults(last_entry['jobcode'])
  transformed_job['employment_type'] = position_defaults['employment_type']
  transformed_job['staff_type'] = position_defaults['staff_type']

  return transformed_job

def new_staff_dept_defaults(**kwargs):
  defaults = {}
  if kwargs['end_date']:
    defaults['visibility'] = 'Restricted'
    defaults['profiled'] = False
  else:
    pure_new_staff_dept_defaults = (
      session.query(PureNewStaffDeptDefaults)
      .filter(and_(
        PureNewStaffDeptDefaults.deptid == kwargs['deptid'],
        PureNewStaffDeptDefaults.jobcode == kwargs['jobcode'],
        PureNewStaffDeptDefaults.jobcode_descr == kwargs['jobcode_descr'],
      ))
      .one_or_none()
    )
    if pure_new_staff_dept_defaults:
      defaults['visibility'] = pure_new_staff_dept_defaults.default_visibility
      if pure_new_staff_dept_defaults.default_profiled == 'true':
        defaults['profiled'] = True
      else:
        defaults['profiled'] = False
    else:
      defaults['visibility'] = 'Restricted'
      defaults['profiled'] = False
  return defaults

def new_staff_position_defaults(jobcode):
  defaults = {}
  pure_new_staff_pos_defaults = (
    session.query(PureNewStaffPosDefaults)
    .filter(PureNewStaffPosDefaults.jobcode == jobcode)
    .one_or_none()
  )
  if pure_new_staff_pos_defaults:
    defaults['employment_type'] = pure_new_staff_pos_defaults.default_employed_as
    defaults['staff_type'] = pure_new_staff_pos_defaults.default_staff_type
  else:
    defaults['employment_type'] = None
    defaults['staff_type'] = None
  return defaults

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
      'end_date': None, # Default for current/active job.
      'deptid': curr_group['deptid'],
    }
    reference_entry = None
    job_is_active = False
    curr_df = pd.DataFrame(data=curr_group['entries'])

    last_date_worked_df = curr_df[curr_df['last_date_worked'].notnull()]

    current_status_df = curr_df[curr_df['status_flg'] == 'C']
    if not current_status_df.empty:
      current_status_entry_index = current_status_df.index[0]
      reference_entry = curr_group['entries'][current_status_entry_index]
      if reference_entry['empl_status'] in active_states:
        job_is_active = True
    else:
      # If there is no 'C' row, this cannot be an active job, so just use the last entry,
      # which should have the latest effdt and highest effseq for that date:
      reference_entry = curr_group['entries'][-1]

      # Special case: If the next job has the same position_nbr, this person has had multiple
      # jobs in the same position. Here we can get a more accurate end_date by using the
      # job_entry_dt of the next job:
      if (
        next_group and
        next_group['position_nbr'] == curr_group['position_nbr'] and 
        last_date_worked_df.empty
      ):
        job['end_date'] = next_group['job_entry_dt']

    if not job_is_active and job['end_date'] is None:   
      if last_date_worked_df.empty:
        job['end_date'] = reference_entry['effdt']
      else:
        job['end_date'] = last_date_worked_df['last_date_worked'].max().to_pydatetime()

    job['job_title'] = reference_entry['jobcode_descr']
    job['empl_rcdno'] = reference_entry['empl_rcdno']

    dept_defaults = new_staff_dept_defaults(
      end_date=job['end_date'],
      deptid=reference_entry['deptid'],
      jobcode=reference_entry['jobcode'],
      jobcode_descr=reference_entry['jobcode_descr'],
    )
    job['visibility'] = dept_defaults['visibility']
    job['profiled'] = dept_defaults['profiled']
  
    job['org_id'] = org_id(reference_entry['deptid'])
  
    position_defaults = new_staff_position_defaults(reference_entry['jobcode'])
    job['employment_type'] = position_defaults['employment_type']
    job['staff_type'] = position_defaults['staff_type']

    jobs.append(job)

  return jobs

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

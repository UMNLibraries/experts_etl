import pandas as pd
import re
from datetime import datetime
from experts_dw.models import PureEligibleDemogChngHst, PureEligiblePOIJob, PureEligiblePOIJobcode, UmnDeptPureOrg
from experts_etl.demographics import latest_demographics_for_emplid # Commented out for now. See local implementation below.
from experts_etl.umn_data_error import record_unknown_dept_errors
from sqlalchemy import and_

def extract_transform(session, emplid):
  entries = extract(session, emplid)
  return transform(session, entries)

def extract(session, emplid):
  entries = []
  for entry in session.query(PureEligiblePOIJob).filter(PureEligiblePOIJob.emplid == emplid).order_by(PureEligiblePOIJob.effdt, PureEligiblePOIJob.effseq):
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
active_states = ['A', 'L', 'P', 'W']

def transform(session, entries):
  jobs = []

  if len(entries) == 0:
    return jobs

  entry_groups = group_entries(entries)
  jobs = transform_entry_groups(session, entry_groups)

  return jobs

def get_org_id(session, deptid):
  org_id = None
  umn_dept_pure_org = (
    session.query(UmnDeptPureOrg)
    .filter(UmnDeptPureOrg.deptid == deptid)
    .one_or_none()
  )
  if umn_dept_pure_org:
    org_id = umn_dept_pure_org.pure_org_id
  return org_id

def neighborhood(iterable):
  iterator = iter(iterable)
  prev_item = None
  curr_item = next(iterator)  # throws StopIteration if empty.
  for next_item in iterator:
    yield (prev_item, curr_item, next_item)
    prev_item, curr_item = curr_item, next_item
  yield (prev_item, curr_item, None)

def transform_entry_groups(session, entry_groups):
  if len(entry_groups) == 0:
    return []

  jobs = []
  for prev_group, curr_group, next_group in neighborhood(entry_groups):
    job = {
      'affiliation_id': curr_group['jobcode'],
      'start_date': curr_group['job_entry_dt'],
      'end_date': None, # Default for current/active job.
      'deptid': curr_group['deptid'],
    }
    reference_entry = None
    job_is_active = False
    curr_df = pd.DataFrame(data=curr_group['entries'])

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
        next_group['position_nbr'] == curr_group['position_nbr']
      ):
        job['end_date'] = next_group['job_entry_dt']

    org_id = get_org_id(session, reference_entry['deptid'])
    if org_id is None:
        # The ps_dwhr_job table, and therefore our views derived from it, don't provide
        # internet id, so we must make a separate query for it:
        demog = latest_demographics_for_emplid(session, reference_entry['emplid'])
        internet_id = demog.internet_id if demog else None

        record_unknown_dept_errors(
            session=session,
            emplid=reference_entry['emplid'],
            internet_id=internet_id,
            jobcode=reference_entry['jobcode'],
            jobcode_descr=reference_entry['jobcode_descr'],
            deptid=reference_entry['deptid'],
            deptid_descr=reference_entry['deptid_descr'],
            um_college=reference_entry['um_college'],
            um_college_descr=reference_entry['um_college_descr'],
            um_campus=reference_entry['um_campus'],
            um_campus_descr=reference_entry['um_campus_descr'],
        )
        continue
    job['org_id'] = org_id

    job['um_campus'] = reference_entry['um_campus']

    if not job_is_active and job['end_date'] is None:
        job['end_date'] = reference_entry['effdt']

    job['job_title'] = reference_entry['jobcode_descr']
    job['empl_rcdno'] = reference_entry['empl_rcdno']

    jobcode_defaults = session.query(PureEligiblePOIJobcode).filter(
        PureEligiblePOIJobcode.jobcode == reference_entry['jobcode']
    ).one()
    job['job_description'] = jobcode_defaults.pure_job_description
    job['employment_type'] = jobcode_defaults.default_employed_as
    if job['end_date'] is not None:
        job['staff_type'] = 'nonacademic'
    else:
        job['staff_type'] = jobcode_defaults.default_staff_type

    job['visibility'] = 'Restricted'
    if job['end_date'] is None and job['um_campus'] in ['TXXX','DXXX']:
        job['visibility'] = jobcode_defaults.default_visibility

    job['profiled'] = False
    if job['end_date'] is None:
        job['profiled'] = jobcode_defaults.default_profiled

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
      if k not in ['effdt','action_dt','job_entry_dt','dept_entry_dt','position_entry_dt']:
        # Skip anything that's not a datetime:
        continue
      if d[k] is not None:
        d[k] = d[k].to_pydatetime()
  return dicts

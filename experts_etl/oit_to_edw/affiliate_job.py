import pandas as pd
import re
from experts_dw.models import PureEligibleAffiliateJob, PureEligibleJobcode, UmnDeptPureOrg
from experts_etl.umn_data_error import record_unknown_dept_errors
from sqlalchemy import and_

def extract_transform(session, emplid):
  entries = extract(session, emplid)
  return transform(session, entries)

def extract(session, emplid):
  entries = []
  for entry in session.query(PureEligibleAffiliateJob).filter(PureEligibleAffiliateJob.emplid == emplid).order_by(PureEligibleAffiliateJob.effdt):
    entries.append(
      {c.name: getattr(entry, c.name) for c in entry.__table__.columns}
    )
  return entries

"""
status_flag values:
C Current 
F Future (We exclude these entries in the SQL views.)
H Historical

status values:
A Active 
I Inactive
"""
active_states = ['A']

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

def transform_entry_groups(session, entry_groups):
  if len(entry_groups) == 0:
    return []

  jobs = []
  for group in entry_groups:
    last_entry = group['entries'][-1]

    job = {
      'affiliation_id': group['um_affil_relation'], # jobcode
      'deptid': group['deptid'],
      'um_campus': last_entry['um_campus'],
      'start_date': group['start_date'],
      'job_title': last_entry['title'],
    }

    org_id = get_org_id(session, job['deptid'])
    if org_id is None:
        record_unknown_dept_errors(
            session=session,
            emplid=last_entry['emplid'],
            jobcode=group['um_affil_relation'],
            jobcode_descr=last_entry['title'],
            deptid=group['deptid'],
            deptid_descr=last_entry['deptid_descr'],
            um_college=last_entry['um_college'],
            um_college_descr=last_entry['um_college_descr'],
            um_campus=last_entry['um_campus'],
            um_campus_descr=last_entry['um_campus_descr'],
        )
        continue
    job['org_id'] = org_id

    if last_entry['status'] not in active_states or last_entry['status_flg'] == 'H':
      job['end_date'] = last_entry['effdt']
    else:
      job['end_date'] = None
  
    # We now set visibility = 'Restricted' and 'profiled' = False for all affiliate jobs:
    job['visibility'] = 'Restricted'
    job['profiled'] = False
  
    # We now set staff_type = 'nonacademic' for all affiliate jobs:
    job['staff_type'] = 'nonacademic'

    jobcode_defaults = session.query(PureEligibleJobcode).filter(
        PureEligibleJobcode.jobcode == group['um_affil_relation']
    ).one()
    job['job_description'] = jobcode_defaults.pure_job_description
    job['employment_type'] = jobcode_defaults.default_employed_as

    jobs.append(job)

  return jobs

def split_entries_into_stints(entries):
  if len(entries) == 0:
    return []

  stints = []
  current_stint = []
  current_stint_ending = False

  for entry in entries:
    if current_stint_ending:
      if entry['status'] in active_states:
        # We've passed the end of the current stint, and this is a new stint in the same position.
        stints.append(current_stint)
        current_stint = []
        current_stint_ending = False
      current_stint.append(entry)
      if entry['status_flg'] == 'C': # C is current.
        # Sometimes there are historical (H) entries with effdt's later than a C entry. Ignore them.
        break
      else:
        continue

    if entry['status'] not in active_states:
      # This is the first entry with an inactive state for this stint, so it's ending.
      # Other entries with inactive states may follow.
      current_stint_ending = True
    current_stint.append(entry)
    if entry['status_flg'] == 'C': # C is current.
      # Sometimes there are historical (H) entries with effdt's later than a C entry. Ignore them.
      break
    else:
      continue

  if len(current_stint) > 0:
    stints.append(current_stint)

  return stints

def group_entries(entries):
  if len(entries) == 0:
    return []

  df = pd.DataFrame(data=entries)
  df_groups = df.groupby(['deptid','um_affiliate_id','um_affil_relation'])
  
  entry_groups = []
  for key, rows in df_groups:
    rows.sort_values(['effdt'])
    dicts = df_to_dicts(rows)
    stints = split_entries_into_stints(dicts)
    for stint in stints:
      entry_group = {
        'deptid': key[0],
        'um_affil_relation': key[2],
        'start_date': stint[0]['effdt'],
        'entries': stint,
      }
      entry_groups.append(entry_group)

  return entry_groups

def df_to_dicts(df):
  dicts = df.to_dict('records')
  for d in dicts:
    for k, v in d.items():
      if pd.isnull(v):
        # Convert all Pandas NaN's, NaT's etc to None:
        d[k] = None
      if k not in ['effdt']:
        # Skip anything that's not a datetime:
        continue
      if d[k] is not None:
        d[k] = d[k].to_pydatetime()
  return dicts

import pandas as pd
import re
from experts_dw.models import PureEligibleAffJob, PureNewStaffDeptDefaults, PureNewStaffPosDefaults, UmnDeptPureOrg
from sqlalchemy import and_

def extract_transform(session, emplid):
  entries = extract(session, emplid)
  return transform(session, entries)

def extract(session, emplid):
  entries = []
  for entry in session.query(PureEligibleAffJob).filter(PureEligibleAffJob.emplid == emplid).order_by(PureEligibleAffJob.effdt):
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

# Currently unused:
def new_staff_dept_defaults(**kwargs):
  defaults = {
    'visibility': None,
    'profiled': None,
  }
  session = kwargs['session']
  pure_new_staff_dept_defaults = (
    session.query(PureNewStaffDeptDefaults)
    .filter(and_(
      PureNewStaffDeptDefaults.deptid == kwargs['deptid'],
      PureNewStaffDeptDefaults.jobcode == kwargs['jobcode'],
      #PureNewStaffDeptDefaults.jobcode_descr == kwargs['jobcode_descr'],
    ))
    #.one_or_none()
    .first()
  )
  if pure_new_staff_dept_defaults:
    defaults['visibility'] = pure_new_staff_dept_defaults.default_visibility
    if pure_new_staff_dept_defaults.default_profiled == 'true':
      defaults['profiled'] = True
    else:
      defaults['profiled'] = False
  return defaults

def new_staff_position_defaults(session, jobcode):
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

def org_id(session, deptid):
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
      'deptid': group['deptid'],
      'start_date': group['start_date'],
      'job_title': last_entry['title'],
    }

    if last_entry['status'] not in active_states or last_entry['status_flg'] == 'H':
      job['end_date'] = last_entry['effdt']
    else:
      job['end_date'] = None
  
# We now set visibility = 'Restricted' and 'profiled' = False for all affiliate jobs:
#    dept_defaults = new_staff_dept_defaults(
#      session=session,
#      end_date=job['end_date'],
#      deptid=job['deptid'],
#      jobcode=group['um_affil_relation'],
#      jobcode_descr=job['job_title'],
#    )
#    job['visibility'] = dept_defaults['visibility']
#    job['profiled'] = dept_defaults['profiled']
    job['visibility'] = 'Restricted'
    job['profiled'] = False
  
    job['org_id'] = org_id(session, job['deptid'])
  
    position_defaults = new_staff_position_defaults(session, group['um_affil_relation'])
    job['employment_type'] = position_defaults['employment_type']

    # We now set staff_type = 'nonacademic' for all affiliate jobs:
    #job['staff_type'] = position_defaults['staff_type']
    job['staff_type'] = 'nonacademic'

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

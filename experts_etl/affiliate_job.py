import re
from experts_dw import db
from experts_dw.models import PureEligibleAffJob, PureNewStaffDeptDefaults, PureNewStaffPosDefaults, UmnDeptPureOrg
from sqlalchemy import and_

session = db.session('hotel')

def extract_transform(emplid):
  return transform(extract(emplid))

def extract(emplid):
  jobs = []
  for job in session.query(PureEligibleAffJob).filter(PureEligibleAffJob.emplid == emplid).order_by(PureEligibleAffJob.effdt):
    jobs.append(
      {c.name: getattr(job, c.name) for c in job.__table__.columns}
    )

  return jobs

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

def transform(jobs):
  jobs_by_deptid_um_affiliate_id_um_affil_relation = group_by_deptid_um_affiliate_id_um_affil_relation(jobs)
  transformed_jobs = []

  for key, entries in jobs_by_deptid_um_affiliate_id_um_affil_relation.items():
    job_stints = transform_job_entries(entries)

    for job_stint in job_stints:
      transformed_job = transform_job_stint(job_stint)
      transformed_jobs.append(transformed_job)
      
  return transformed_jobs

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

def transform_job_stint(job_stint):
  transformed_job = {}

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

  transformed_job['job_title'] = last_entry['title']
  transformed_job['deptid'] = last_entry['deptid']
  transformed_job['start_date'] = first_entry['effdt']

  if last_entry['status'] not in active_states or last_entry['status_flg'] == 'H':
    transformed_job['end_date'] = last_entry['effdt']
  else:
    transformed_job['end_date'] = None

  dept_defaults = new_staff_dept_defaults(
    end_date=transformed_job['end_date'],
    deptid=last_entry['deptid'],
    jobcode=last_entry['um_affil_relation'],
    jobcode_descr=last_entry['title'],
  )
  transformed_job['visibility'] = dept_defaults['visibility']
  transformed_job['profiled'] = dept_defaults['profiled']

  transformed_job['org_id'] = org_id(last_entry['deptid'])

  position_defaults = new_staff_position_defaults(last_entry['um_affil_relation'])
  transformed_job['employment_type'] = position_defaults['employment_type']
  transformed_job['staff_type'] = position_defaults['staff_type']

  return transformed_job

def transform_job_entries(entries):
  job_stints = []
  current_stint = []
  current_stint_ending = False

  for entry in entries:
    if current_stint_ending:
      if entry['status'] in active_states:
        # We've passed the end of the current stint, and this is a new stint in the same position.
        job_stints.append(current_stint)
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
    job_stints.append(current_stint)

  return job_stints

def group_by_deptid_um_affiliate_id_um_affil_relation(jobs):
  jobs_by_deptid_um_affiliate_id_um_affil_relation = {}
  for job in jobs:
    key = job['deptid'] + '-'  + job['um_affiliate_id'] + '-' + job['um_affil_relation']
    if key not in jobs_by_deptid_um_affiliate_id_um_affil_relation:
      jobs_by_deptid_um_affiliate_id_um_affil_relation[key] = [] 
    jobs_by_deptid_um_affiliate_id_um_affil_relation[key].append(job)
  return jobs_by_deptid_um_affiliate_id_um_affil_relation

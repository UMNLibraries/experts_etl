import re
from sqlalchemy import func
from experts_dw import db
#from experts_dw.models import PureEligibleAffJob, PureEligibleAffJobNew, PureEligibleAffJobChngHst, PureEligibleEmpJob, PureEligibleEmpJobNew, PureEligibleEmpJobChngHst
from experts_dw.models import Person, PureEligibleDemogChngHst
from . import affiliate_job, employee_job

session = db.session('hotel')

from jinja2 import Environment, PackageLoader, Template, select_autoescape
env = Environment(
    loader=PackageLoader('experts_etl', 'templates'),
    autoescape=select_autoescape(['html', 'xml'])
)

def extract_transform_serialize(emplid):
  return serialize(transform(extract(emplid)))

def extract(emplid):
  subqry = session.query(func.max(PureEligibleDemogChngHst.timestamp)).filter(PureEligibleDemogChngHst.emplid == emplid)

  demog = (
    session.query(PureEligibleDemogChngHst)
    .filter(PureEligibleDemogChngHst.emplid == emplid, PureEligibleDemogChngHst.timestamp == subqry)
    .one_or_none()
  )
  person_dict = {c.name: getattr(demog, c.name) for c in demog.__table__.columns}

  person = (
    session.query(Person)
    .filter(Person.emplid == demog.emplid)
    .one_or_none()
  )
  if person:
    person_dict['scival_id'] = person.pure_id
  else:
    person_dict['scival_id'] = None

  return person_dict

def transform(person_dict):
  person_dict['person_id'] = transform_person_id(
    person_dict['emplid'],
    person_dict['scival_id']
  )

  person_dict['first_name'] = transform_first_name(
    person_dict['first_name'],
    person_dict['middle_initial']
  )

  employee_jobs = employee_job.extract_transform(person_dict['emplid'])
  affiliate_jobs = affiliate_job.extract_transform(person_dict['emplid'])
  jobs = transform_primary_job(affiliate_jobs, employee_jobs, person_dict['primary_empl_rcdno'])  
  person_dict['jobs'] = transform_staff_org_assoc_id(jobs, person_dict['person_id'])

  person_dict['visibility'] = 'Restricted'
  person_dict['profiled'] = False
  for job in jobs:
    if job['visibility'] == 'Public':
      person_dict['visibility'] = 'Public'
    if job['profiled']:
      person_dict['profiled'] = True

  return person_dict

def serialize(person_dict):
  template = env.get_template('person.xml.j2')
  return template.render(person_dict)

def transform_staff_org_assoc_id(jobs, person_id):
  transformed_jobs = []
  transformed_jobs_with_staff_org_assoc_id = {}
  for job in jobs:
    transformed_job = job.copy()
    if transformed_job['org_id'] and transformed_job['job_title'] and transformed_job['employment_type']:
      staff_org_assoc_id = 'autoid:{}-{}-{}-{}-{}'.format(
        person_id,
        transformed_job['org_id'],
        transformed_job['job_title'],
        transformed_job['employment_type'],
        transformed_job['start_date'].strftime('%Y-%m-%d')
      )
      transformed_job['staff_org_assoc_id'] = staff_org_assoc_id
      if staff_org_assoc_id not in transformed_jobs_with_staff_org_assoc_id:
        transformed_jobs_with_staff_org_assoc_id[staff_org_assoc_id] = []
      transformed_jobs_with_staff_org_assoc_id[staff_org_assoc_id].append(transformed_job)
    else:
     transformed_job['staff_org_assoc_id'] = None
     transformed_jobs.append(transformed_job)

  # Ensure we have only one job for each staff_org_assoc_id:
  for staff_org_assoc_id, possibly_multiple_jobs in transformed_jobs_with_staff_org_assoc_id.items():
    if len(possibly_multiple_jobs) == 1:
      transformed_jobs.append(possibly_multiple_jobs[0])
      continue

    jobs_include_primary = False
    jobs_with_no_end_date = []

    for transformed_job in possibly_multiple_jobs:
      if transformed_job['primary']:
        jobs_include_primary = True
      if not transformed_job['end_date']:
        jobs_with_no_end_date.append(transformed_job)
    
    job_to_keep = possibly_multiple_jobs[0]
    if len(jobs_with_no_end_date) > 0:
      job_to_keep = jobs_with_no_end_date[0]

    if jobs_include_primary:
      job_to_keep['primary'] = True

    transformed_jobs.append(job_to_keep)

  return transformed_jobs

def transform_primary_job(affiliate_jobs, employee_jobs, primary_empl_rcdno):
  transformed_aff_jobs = affiliate_jobs.copy()
  transformed_emp_jobs = employee_jobs.copy()

  # Affiliate jobs have no empl_rcdno, and thus no primary_empl_rcdno will ever match them,
  # so they default to false:
  for job in transformed_aff_jobs:
    job['primary'] = False

  # Any currently-active jobs are likely to be at or near the end of the list:
  transformed_emp_jobs.reverse()
  primary_job_set = False
  for job in transformed_emp_jobs:
    if not primary_job_set and job['empl_rcdno'] == str(primary_empl_rcdno) and job['end_date'] == None:
      job['primary'] = True
      primary_job_set = True
    else:
      job['primary'] = False
  transformed_emp_jobs.reverse()

  # From Jan Fransen on setting a primary position in email, 2018-01-23:
  # Rather than being completely arbitrary, make the position with the earliest
  # effective date the primary. If they have the same effective date, then
  # choose the lowest empl_rcdno. Hopefully it's not duplicated to that level,
  # but if it is then just pick one.
  if not primary_job_set:
    earliest_start_dates = []
    lowest_empl_rcdnos = []
    for job in transformed_emp_jobs:

      if job['end_date']:
        continue

      if len(earliest_start_dates) == 0 or job['start_date'] == min(earliest_start_dates):
        earliest_start_dates.append(job['start_date'])
      else:
        earliest_start_dates = [job['start_date']]

      if len(lowest_empl_rcdnos) == 0 or job['empl_rcdno'] == min(lowest_empl_rcdnos):
        lowest_empl_rcdnos.append(job['empl_rcdno'])
      else:
        lowest_empl_rcdnos = [job['empl_rcdno']]

    if len(earliest_start_dates) == 1:
      earliest_start_date = earliest_start_dates[0]
      for job in transformed_emp_jobs:
        if job['end_date']:
          continue
        if job['start_date'] == earliest_start_date:
          job['primary'] = True
          primary_job_set = True
    elif len(lowest_empl_rcdnos) == 1:
      lowest_empl_rcdno = lowest_empl_rcdnos[0]
      for job in transformed_emp_jobs:
        if job['end_date']:
          continue
        if job['empl_rcdno'] == lowest_empl_rcdno:
          job['primary'] = True
          primary_job_set = True

    if not primary_job_set:
      for job in transformed_emp_jobs:
        if job['end_date']:
          continue
        if job['start_date'] == min(earliest_start_dates):
          job['primary'] = True
          primary_job_set = True
          break

    if not primary_job_set:
      # In this case (maybe all emp jobs are ended, or there are none),
      # just pick any active aff job:
      for job in transformed_aff_jobs:
        if job['end_date']:
          continue
        else:
          job['primary'] = True
          primary_job_set = True
          break

  transformed_jobs = transformed_emp_jobs
  transformed_jobs.extend(transformed_aff_jobs)

  return transformed_jobs

def transform_person_id(emplid, scival_id):
  # The Pure person id from most persons will be the emplid, but for some
  # persons we use the id from ther predecessor system, SciVal.
  if scival_id:
    return scival_id
  else:
    return emplid

def transform_first_name(first_name, middle_initial):
  # If the person has a middle initial, include it in the first name.
  # The DW uses white space for non-existent middle initials, for some unfathomable reason:
  if (middle_initial and re.search(r'\S+', middle_initial)):
    first_name += ' ' + middle_initial
  return first_name

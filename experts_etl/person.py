import re
from experts_dw import db
#from experts_dw.models import PureEligibleAffJob, PureEligibleAffJobNew, PureEligibleAffJobChngHst, PureEligibleEmpJob, PureEligibleEmpJobNew, PureEligibleEmpJobChngHst
from experts_dw.models import Person, PureEligibleDemog
from . import employee_job

session = db.session('hotel')

from jinja2 import Environment, PackageLoader, Template, select_autoescape
env = Environment(
    loader=PackageLoader('experts_etl', 'templates'),
    autoescape=select_autoescape(['html', 'xml'])
)

def extract_transform_serialize(emplid):
  return serialize(transform(extract(emplid)))

def extract(emplid):
  demog = (
    session.query(PureEligibleDemog)
    .filter(PureEligibleDemog.emplid == emplid)
    .one_or_none()
  )
  person_dict = {c.name: getattr(demog, c.name) for c in demog.__table__.columns}

  person = (
    session.query(Person)
    .filter(Person.emplid == demog.emplid)
    .one_or_none()
  )
  person_dict['scival_id'] = person.pure_id

  return person_dict

def transform(person_dict):
  person_dict['contact_info_url'] = 'https://myaccount.umn.edu/lookup?type=Internet+ID&CN=' + person_dict['internet_id']

  person_dict['person_id'] = transform_person_id(
    person_dict['emplid'],
    person_dict['scival_id']
  )

  person_dict['first_name'] = transform_first_name(
    person_dict['first_name'],
    person_dict['middle_initial']
  )

  jobs = []
  jobs.extend(employee_job.extract_transform(person_dict['emplid']))

  jobs_with_primary = transform_primary_job(jobs, person_dict['primary_empl_rcdno'])  

  person_dict['jobs'] = jobs_with_primary

  return person_dict

def serialize(person_dict):
  template = env.get_template('person.xml.j2')
  return template.render(person_dict)

def transform_primary_job(jobs, primary_empl_rcdno):
  transformed_jobs = jobs.copy()
  # Any currently-active jobs are likely to be at or near the end of the list:
  transformed_jobs.reverse()
  primary_job_set = False
  for job in transformed_jobs:
    if not primary_job_set and job['empl_rcdno'] == str(primary_empl_rcdno) and job['end_date'] == None:
      job['primary'] = 'true'
      primary_job_set = True
    else:
      job['primary'] = 'false'
  transformed_jobs.reverse()
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

import re
from experts_dw import db
#from experts_dw.models import PureEligibleAffJob, PureEligibleAffJobNew, PureEligibleAffJobChngHst, PureEligibleEmpJob, PureEligibleEmpJobNew, PureEligibleEmpJobChngHst
from experts_dw.models import Person, PureEligibleDemog

session = db.session('hotel')

from jinja2 import Environment, PackageLoader, Template, select_autoescape
env = Environment(
    loader=PackageLoader('experts_etl', 'templates'),
    autoescape=select_autoescape(['html', 'xml'])
)

def extract(emplid):
  demog = (
    session.query(PureEligibleDemog)
    .filter(PureEligibleDemog.emplid == emplid)
    .one_or_none()
  )

  person = (
    session.query(Person)
    .filter(Person.emplid == demog.emplid)
    .one_or_none()
  )

  person_dict = {}
  for item in ['emplid','first_name','middle_initial','last_name','name_suffix','internet_id','instl_email_addr']:
    person_dict[item] = getattr(demog, item)

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

  template = env.get_template('person.xml.j2')
  return template.render(person_dict)

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

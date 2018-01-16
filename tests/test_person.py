import re
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())
from experts_dw import db
from experts_dw import db
#from experts_dw.models import PureEligibleAffJob, PureEligibleAffJobNew, PureEligibleAffJobChngHst, PureEligibleEmpJob, PureEligibleEmpJobNew, PureEligibleEmpJobChngHst
from experts_dw.models import Person, PureEligibleDemog

from jinja2 import Environment, PackageLoader, Template, select_autoescape
env = Environment(
    loader=PackageLoader('experts_etl', 'templates'),
    autoescape=select_autoescape(['html', 'xml'])
)

session = db.session('hotel')

def test_template():
  template = env.get_template('person.xml.j2')
  assert isinstance(template, Template)

  demog = (
    session.query(PureEligibleDemog)
    .filter(PureEligibleDemog.emplid == '5150075')
    .one_or_none()
  )
  assert isinstance(demog, PureEligibleDemog)

  person = (
    session.query(Person)
    .filter(Person.emplid == demog.emplid)
    .one_or_none()
  )
  assert isinstance(person, Person)

  expected_person_xml = """<person id="8185">
  <name>
    <v3:firstname>Maximiliano</v3:firstname>
    <v3:lastname>Bezada</v3:lastname>
  </name>
  <gender>unknown</gender>
  <links>
    <v3:link id="https://myaccount.umn.edu/lookup?type=Internet+ID&CN=mbezada">
      <v3:url>https://myaccount.umn.edu/lookup?type=Internet+ID&CN=mbezada</v3:url>
      <v3:type>contact_information</v3:type>
      <v3:description>
        <v3:text lang="en" country="US">Contact Information</v3:text>
      </v3:description>
    </v3:link>
  </links>
  <user>
    <userName>mbezada@umn.edu</userName>
    <email>mbezada@umn.edu</email>
  </user>
  <personIds>
    <v3:id type="employee">5150075</v3:id>
    <v3:id type="umn">mbezada</v3:id>
  </personIds>
</person>"""

  person_dict = {}
  if person.pure_id:
    person_dict['person_id'] = person.pure_id
  else:
    person_dict['person_id'] = demog.emplid
  first_name = demog.first_name
  if (demog.middle_initial and re.search(r'\S+', demog.middle_initial)):
    first_name += ' ' + demog.middle_initial
  person_dict['first_name'] = first_name
  for item in ['emplid','last_name','name_suffix','internet_id','instl_email_addr']:
    person_dict[item] = getattr(demog, item)

  person_xml = template.render(person_dict)
  assert person_xml == expected_person_xml

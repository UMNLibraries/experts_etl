from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())
from experts_dw import db
from experts_dw import db
#from experts_dw.models import PureEligibleAffJob, PureEligibleAffJobNew, PureEligibleAffJobChngHst, PureEligibleEmpJob, PureEligibleEmpJobNew, PureEligibleEmpJobChngHst
from experts_dw.models import PureEligibleDemog

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

  expected_person_xml = """<person id="5150075">
  <name>
    <v3:firstname>Maximiliano</v3:firstname>
    <v3:lastname>Bezada</v3:lastname>
  </name>
  <gender>unknown</gender>
  <user>
    <userName>mbezada@umn.edu</userName>
    <email>mbezada@umn.edu</email>
  </user>
  <personIds>
    <v3:id type="employee">5150075</v3:id>
    <v3:id type="umn">mbezada</v3:id>
  </personIds>
</person>"""

  person_xml = template.render({
    'emplid': demog.emplid, 
    'first_name': demog.first_name, 
    'last_name': demog.last_name, 
    'internet_id': demog.internet_id, 
    'instl_email_addr': demog.instl_email_addr, 
  })
  assert person_xml == expected_person_xml

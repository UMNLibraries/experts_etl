from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())
from experts_etl import person

def test_extract():
  person_dict = person.extract('5150075')

  expected_person_dict = {
    'scival_id': '8185',
    'emplid': '5150075',
    'internet_id': 'mbezada',
    'name': 'Bezada Vierma,Maximiliano J',
    'first_name': 'Maximiliano',
    'middle_initial': ' ',
    'last_name': 'Bezada',
    'name_suffix': None,
    'instl_email_addr': 'mbezada@umn.edu',
    'tenure_flag': 'N',
    'tenure_track_flag': 'Y',
    'primary_empl_rcdno': 0,
  }

  assert person_dict == expected_person_dict

def test_transform_person_id():
  scival_id = '8185'
  emplid = '5150075'

  assert person.transform_person_id(emplid, scival_id) == scival_id
  # Case where scival_id == emplid:
  assert person.transform_person_id(emplid, emplid) == emplid
  assert person.transform_person_id(emplid, None) == emplid

def test_transform_first_name():
  first_name = 'Alex'
  middle_initial = 'J'

  assert person.transform_first_name(first_name, middle_initial) == 'Alex J'
  assert person.transform_first_name(first_name, ' ') == 'Alex'
  assert person.transform_first_name(first_name, None) == 'Alex'

def test_transform():
  person_dict = {
    'scival_id': '8185',
    'emplid': '5150075',
    'internet_id': 'mbezada',
    'name': 'Bezada Vierma,Maximiliano J',
    'first_name': 'Maximiliano',
    'middle_initial': ' ',
    'last_name': 'Bezada',
    'name_suffix': None,
    'instl_email_addr': 'mbezada@umn.edu',
    'tenure_flag': 'N',
    'tenure_track_flag': 'Y',
    'primary_empl_rcdno': 0,
  }
  transformed_person_dict = person.transform(person_dict)
  expected_transformed_person_dict = {
    'contact_info_url': 'https://myaccount.umn.edu/lookup?type=Internet+ID&CN=mbezada',
    'emplid': '5150075',
    'first_name': 'Maximiliano',
    'instl_email_addr': 'mbezada@umn.edu',
    'internet_id': 'mbezada',
    'last_name': 'Bezada',
    'middle_initial': ' ',
    'name': 'Bezada Vierma,Maximiliano J',
    'name_suffix': None,
    'person_id': '8185',
    'primary_empl_rcdno': 0,
    'scival_id': '8185',
    'tenure_flag': 'N',
    'tenure_track_flag': 'Y',
  }
  assert transformed_person_dict == expected_transformed_person_dict

def test_serialize():
  transformed_person_dict = {
    'contact_info_url': 'https://myaccount.umn.edu/lookup?type=Internet+ID&CN=mbezada',
    'emplid': '5150075',
    'first_name': 'Maximiliano',
    'instl_email_addr': 'mbezada@umn.edu',
    'internet_id': 'mbezada',
    'last_name': 'Bezada',
    'middle_initial': ' ',
    'name': 'Bezada Vierma,Maximiliano J',
    'name_suffix': None,
    'person_id': '8185',
    'primary_empl_rcdno': 0,
    'scival_id': '8185',
    'tenure_flag': 'N',
    'tenure_track_flag': 'Y',
  }
  person_xml = person.serialize(transformed_person_dict)

  expected_person_xml = """<person id="8185">
  <name>
    <v3:firstname>Maximiliano</v3:firstname>
    <v3:lastname>Bezada</v3:lastname>
  </name>
  <gender>unknown</gender>
  <links>
    <v3:link id="contactInfoLink8185">
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

  assert person_xml == expected_person_xml

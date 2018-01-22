from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())
import datetime
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
    'visibility': 'Public',
    'profiled': True,
    'jobs': [
      {
        'deptid': '11130',
        'empl_rcdno': '0',
        'employment_type': 'faculty',
        'end_date': None,
        'job_title': 'Assistant Professor',
        'org_id': 'IHRBIHRB',
        'staff_type': 'academic',
        'start_date': datetime.datetime(2014, 8, 29, 0, 0),
	'primary': True,
        'visibility': 'Public',
        'profiled': True,
      },
    ],
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
    'visibility': 'Public',
    'profiled': True,
    'jobs': [
      {
        'deptid': '11130',
        'empl_rcdno': '0',
        'employment_type': 'faculty',
        'end_date': None,
        'job_title': 'Assistant Professor',
        'org_id': 'IHRBIHRB',
        'staff_type': 'academic',
        'start_date': datetime.datetime(2014, 8, 29, 0, 0),
	'primary': True,
        'visibility': 'Public',
        'profiled': True,
      },
    ],
  }
  person_xml = person.serialize(transformed_person_dict)

  expected_person_xml = """<person id="8185">
  <name>
    <v3:firstname>Maximiliano</v3:firstname>
    <v3:lastname>Bezada</v3:lastname>
  </name>
  <gender>unknown</gender>
  <organisationAssociations>
    <staffOrganisationAssociation id="staffAssoc-5150075-IHRBIHRB-1" managedInPure="false">
      <employmentType>faculty</employmentType>
      <primaryAssociation>true</primaryAssociation>
      <organisation>
        <v3:source_id>IHRBIHRB</v3:source_id>
      </organisation>
      <period>
        <v3:startDate>29-08-2014</v3:startDate>
      </period>
      <staffType>academic</staffType>
      <jobTitle>Assistant Professor</jobTitle>
    </staffOrganisationAssociation>
  </organisationAssociations>
  <links>
    <v3:link id="contactInfoLink-5150075">
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
  <visibility>Public</visibility>
  <profiled>true</profiled>
</person>"""

  assert person_xml == expected_person_xml

def test_extract_transform_serialize():
  person_xml = person.extract_transform_serialize('5150075')

  expected_person_xml = """<person id="8185">
  <name>
    <v3:firstname>Maximiliano</v3:firstname>
    <v3:lastname>Bezada</v3:lastname>
  </name>
  <gender>unknown</gender>
  <organisationAssociations>
    <staffOrganisationAssociation id="staffAssoc-5150075-IHRBIHRB-1" managedInPure="false">
      <employmentType>faculty</employmentType>
      <primaryAssociation>true</primaryAssociation>
      <organisation>
        <v3:source_id>IHRBIHRB</v3:source_id>
      </organisation>
      <period>
        <v3:startDate>29-08-2014</v3:startDate>
      </period>
      <staffType>academic</staffType>
      <jobTitle>Assistant Professor</jobTitle>
    </staffOrganisationAssociation>
  </organisationAssociations>
  <links>
    <v3:link id="contactInfoLink-5150075">
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
  <visibility>Public</visibility>
  <profiled>true</profiled>
</person>"""

  assert person_xml == expected_person_xml

  person_xml_2 = person.extract_transform_serialize('2585238')

  expected_person_xml_2 = """<person id="898">
  <name>
    <v3:firstname>Abigail</v3:firstname>
    <v3:lastname>Gewirtz</v3:lastname>
  </name>
  <gender>unknown</gender>
  <organisationAssociations>
    <staffOrganisationAssociation id="staffAssoc-2585238-RQHKJLUF-1" managedInPure="false">
      <employmentType>faculty</employmentType>
      <primaryAssociation>false</primaryAssociation>
      <organisation>
        <v3:source_id>RQHKJLUF</v3:source_id>
      </organisation>
      <period>
        <v3:startDate>27-08-2007</v3:startDate>
      </period>
      <staffType>academic</staffType>
      <jobTitle>Professor</jobTitle>
    </staffOrganisationAssociation>
    <staffOrganisationAssociation id="staffAssoc-2585238-LBZUCPBF-2" managedInPure="false">
      <employmentType>adjunct_faculty</employmentType>
      <primaryAssociation>false</primaryAssociation>
      <organisation>
        <v3:source_id>LBZUCPBF</v3:source_id>
      </organisation>
      <period>
        <v3:startDate>06-04-2015</v3:startDate>
        <v3:endDate>12-01-2017</v3:endDate>
      </period>
      <staffType>nonacademic</staffType>
      <jobTitle>Adjunct Associate Professor</jobTitle>
    </staffOrganisationAssociation>
    <staffOrganisationAssociation id="staffAssoc-2585238-CBIHJRCYWWAA-3" managedInPure="false">
      <employmentType>adjunct_faculty</employmentType>
      <primaryAssociation>false</primaryAssociation>
      <organisation>
        <v3:source_id>CBIHJRCYWWAA</v3:source_id>
      </organisation>
      <period>
        <v3:startDate>06-04-2015</v3:startDate>
      </period>
      <staffType>nonacademic</staffType>
      <jobTitle>Adjunct Associate Professor</jobTitle>
    </staffOrganisationAssociation>
    <staffOrganisationAssociation id="staffAssoc-2585238-SKDPLVKBRNWDZMQ-4" managedInPure="false">
      <employmentType>adjunct_faculty</employmentType>
      <primaryAssociation>false</primaryAssociation>
      <organisation>
        <v3:source_id>SKDPLVKBRNWDZMQ</v3:source_id>
      </organisation>
      <period>
        <v3:startDate>06-04-2015</v3:startDate>
      </period>
      <staffType>nonacademic</staffType>
      <jobTitle>Adjunct Assistant Professor</jobTitle>
    </staffOrganisationAssociation>
    <staffOrganisationAssociation id="staffAssoc-2585238-LBZUCPBF-5" managedInPure="false">
      <employmentType>adjunct_faculty</employmentType>
      <primaryAssociation>false</primaryAssociation>
      <organisation>
        <v3:source_id>LBZUCPBF</v3:source_id>
      </organisation>
      <period>
        <v3:startDate>18-01-2017</v3:startDate>
      </period>
      <staffType>nonacademic</staffType>
      <jobTitle>Adjunct Professor</jobTitle>
    </staffOrganisationAssociation>
    <staffOrganisationAssociation id="staffAssoc-2585238-CBIHJRCYWWAA-6" managedInPure="false">
      <employmentType>adjunct_faculty</employmentType>
      <primaryAssociation>false</primaryAssociation>
      <organisation>
        <v3:source_id>CBIHJRCYWWAA</v3:source_id>
      </organisation>
      <period>
        <v3:startDate>01-07-2017</v3:startDate>
      </period>
      <staffType>nonacademic</staffType>
      <jobTitle>Adjunct Assistant Professor</jobTitle>
    </staffOrganisationAssociation>
  </organisationAssociations>
  <links>
    <v3:link id="contactInfoLink-2585238">
      <v3:url>https://myaccount.umn.edu/lookup?type=Internet+ID&CN=agewirtz</v3:url>
      <v3:type>contact_information</v3:type>
      <v3:description>
        <v3:text lang="en" country="US">Contact Information</v3:text>
      </v3:description>
    </v3:link>
  </links>
  <user>
    <userName>agewirtz@umn.edu</userName>
    <email>agewirtz@umn.edu</email>
  </user>
  <personIds>
    <v3:id type="employee">2585238</v3:id>
    <v3:id type="umn">agewirtz</v3:id>
  </personIds>
  <visibility>Public</visibility>
  <profiled>true</profiled>
</person>"""

  assert person_xml_2 == expected_person_xml_2

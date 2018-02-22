from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())
import datetime, pytest
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
    'primary_empl_rcdno': '0',
    'timestamp': datetime.datetime(2018, 1, 22, 12, 6, 30),
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

@pytest.fixture
def jobs():
  from . import fake312_employee_jobs
  return fake312_employee_jobs

def test_transform_staff_org_assoc_id(jobs):
  # Trouble-shooting queries:
  # select * from pure_eligible_emp_job where emplid = '1217312' order by effdt, effseq;

  # select emplid, status_flg, count(*) from pure_eligible_emp_job where status_flg = 'C' group by emplid, status_flg having count(*) > 1;
  # select * from pure_eligible_emp_job where emplid = '8003946' order by effdt, effseq;
  assert person.transform_staff_org_assoc_id(jobs.jobs, '6030') == jobs.jobs_with_staff_org_assoc_id

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
    'primary_empl_rcdno': '0',
  }
  transformed_person_dict = person.transform(person_dict)
  expected_transformed_person_dict = {
    'emplid': '5150075',
    'first_name': 'Maximiliano',
    'instl_email_addr': 'mbezada@umn.edu',
    'internet_id': 'mbezada',
    'last_name': 'Bezada',
    'middle_initial': ' ',
    'name': 'Bezada Vierma,Maximiliano J',
    'name_suffix': None,
    'person_id': '8185',
    'primary_empl_rcdno': '0',
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
        'staff_org_assoc_id': 'autoid:8185-IHRBIHRB-Assistant Professor-faculty-2014-08-29',
      },
    ],
  }
  assert transformed_person_dict == expected_transformed_person_dict

def test_serialize():
  transformed_person_dict = {
    'emplid': '5150075',
    'first_name': 'Maximiliano',
    'instl_email_addr': 'mbezada@umn.edu',
    'internet_id': 'mbezada',
    'last_name': 'Bezada',
    'middle_initial': ' ',
    'name': 'Bezada Vierma,Maximiliano J',
    'name_suffix': None,
    'person_id': '8185',
    'primary_empl_rcdno': '0',
    'scival_id': '8185',
    'tenure_flag': 'N',
    'tenure_track_flag': 'Y',
    'visibility': 'Public',
    'profiled': True,
    'jobs': [
      {
        'deptid': '11130',
        'jobcode': '9403',
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
        'staff_org_assoc_id': 'autoid:8185-IHRBIHRB-Assistant Professor-faculty-2014-08-29',
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
    <staffOrganisationAssociation id="autoid:8185-IHRBIHRB-Assistant Professor-faculty-2014-08-29" managedInPure="false">
      <employmentType>faculty</employmentType>
      <primaryAssociation>true</primaryAssociation>
      <organisation>
        <v3:source_id>IHRBIHRB</v3:source_id>
      </organisation>
      <period>
        <v3:startDate>29-08-2014</v3:startDate>
      </period>
      <staffType>academic</staffType>
      <jobDescription><v3:text lang="en">Assistant Professor</v3:text></jobDescription>
    </staffOrganisationAssociation>
  </organisationAssociations>
  <user id="8185">
    <userName>mbezada@umn.edu</userName>
    <email>mbezada@umn.edu</email>
  </user>
  <personIds>
    <v3:id type="employee" id="autoid:8185-employee-5150075">5150075</v3:id>
    <v3:id type="umn" id="autoid:8185-umn-mbezada">mbezada</v3:id>
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
    <staffOrganisationAssociation id="autoid:8185-IHRBIHRB-Assistant Professor-faculty-2014-08-29" managedInPure="false">
      <employmentType>faculty</employmentType>
      <primaryAssociation>true</primaryAssociation>
      <organisation>
        <v3:source_id>IHRBIHRB</v3:source_id>
      </organisation>
      <period>
        <v3:startDate>29-08-2014</v3:startDate>
      </period>
      <staffType>academic</staffType>
      <jobDescription><v3:text lang="en">Assistant Professor</v3:text></jobDescription>
    </staffOrganisationAssociation>
  </organisationAssociations>
  <user id="8185">
    <userName>mbezada@umn.edu</userName>
    <email>mbezada@umn.edu</email>
  </user>
  <personIds>
    <v3:id type="employee" id="autoid:8185-employee-5150075">5150075</v3:id>
    <v3:id type="umn" id="autoid:8185-umn-mbezada">mbezada</v3:id>
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
    <staffOrganisationAssociation id="autoid:898-RQHKJLUF-Professor-faculty-2007-08-27" managedInPure="false">
      <employmentType>faculty</employmentType>
      <primaryAssociation>false</primaryAssociation>
      <organisation>
        <v3:source_id>RQHKJLUF</v3:source_id>
      </organisation>
      <period>
        <v3:startDate>27-08-2007</v3:startDate>
      </period>
      <staffType>academic</staffType>
      <jobDescription><v3:text lang="en">Professor</v3:text></jobDescription>
    </staffOrganisationAssociation>
    <staffOrganisationAssociation id="autoid:898-WDQMICGM-Administrative Manager 2-academic_administrative-2016-06-13" managedInPure="false">
      <employmentType>academic_administrative</employmentType>
      <primaryAssociation>true</primaryAssociation>
      <organisation>
        <v3:source_id>WDQMICGM</v3:source_id>
      </organisation>
      <period>
        <v3:startDate>13-06-2016</v3:startDate>
      </period>
      <staffType>nonacademic</staffType>
      <jobDescription><v3:text lang="en">Administrative Manager 2</v3:text></jobDescription>
    </staffOrganisationAssociation>
    <staffOrganisationAssociation id="autoid:898-LBZUCPBF-Adjunct Associate Professor-adjunct_faculty-2015-04-06" managedInPure="false">
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
      <jobDescription><v3:text lang="en">Adjunct Associate Professor</v3:text></jobDescription>
    </staffOrganisationAssociation>
    <staffOrganisationAssociation id="autoid:898-CBIHJRCYWWAA-Adjunct Associate Professor-adjunct_faculty-2015-04-06" managedInPure="false">
      <employmentType>adjunct_faculty</employmentType>
      <primaryAssociation>false</primaryAssociation>
      <organisation>
        <v3:source_id>CBIHJRCYWWAA</v3:source_id>
      </organisation>
      <period>
        <v3:startDate>06-04-2015</v3:startDate>
      </period>
      <staffType>nonacademic</staffType>
      <jobDescription><v3:text lang="en">Adjunct Associate Professor</v3:text></jobDescription>
    </staffOrganisationAssociation>
    <staffOrganisationAssociation id="autoid:898-SKDPLVKBRNWDZMQ-Adjunct Assistant Professor-adjunct_faculty-2015-04-06" managedInPure="false">
      <employmentType>adjunct_faculty</employmentType>
      <primaryAssociation>false</primaryAssociation>
      <organisation>
        <v3:source_id>SKDPLVKBRNWDZMQ</v3:source_id>
      </organisation>
      <period>
        <v3:startDate>06-04-2015</v3:startDate>
      </period>
      <staffType>nonacademic</staffType>
      <jobDescription><v3:text lang="en">Adjunct Assistant Professor</v3:text></jobDescription>
    </staffOrganisationAssociation>
    <staffOrganisationAssociation id="autoid:898-LBZUCPBF-Adjunct Professor-adjunct_faculty-2017-01-18" managedInPure="false">
      <employmentType>adjunct_faculty</employmentType>
      <primaryAssociation>false</primaryAssociation>
      <organisation>
        <v3:source_id>LBZUCPBF</v3:source_id>
      </organisation>
      <period>
        <v3:startDate>18-01-2017</v3:startDate>
      </period>
      <staffType>nonacademic</staffType>
      <jobDescription><v3:text lang="en">Adjunct Professor</v3:text></jobDescription>
    </staffOrganisationAssociation>
    <staffOrganisationAssociation id="autoid:898-CBIHJRCYWWAA-Adjunct Assistant Professor-adjunct_faculty-2017-07-01" managedInPure="false">
      <employmentType>adjunct_faculty</employmentType>
      <primaryAssociation>false</primaryAssociation>
      <organisation>
        <v3:source_id>CBIHJRCYWWAA</v3:source_id>
      </organisation>
      <period>
        <v3:startDate>01-07-2017</v3:startDate>
      </period>
      <staffType>nonacademic</staffType>
      <jobDescription><v3:text lang="en">Adjunct Assistant Professor</v3:text></jobDescription>
    </staffOrganisationAssociation>
  </organisationAssociations>
  <user id="898">
    <userName>agewirtz@umn.edu</userName>
    <email>agewirtz@umn.edu</email>
  </user>
  <personIds>
    <v3:id type="employee" id="autoid:898-employee-2585238">2585238</v3:id>
    <v3:id type="umn" id="autoid:898-umn-agewirtz">agewirtz</v3:id>
  </personIds>
  <visibility>Public</visibility>
  <profiled>true</profiled>
</person>"""

  assert person_xml_2 == expected_person_xml_2

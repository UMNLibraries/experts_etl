import datetime, pytest
from experts_dw import db
from experts_etl.oit_to_edw import person

@pytest.fixture
def session():
  with db.session('hotel') as session:
    yield session

def test_extract(session):
  person_dict = person.extract(session, '5150075')

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
    'tenure_flag': 'Y',
    'tenure_track_flag': 'N',
    'primary_empl_rcdno': '0',
    'timestamp': datetime.datetime(2020,8,13,10,10,3),
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
  from . import employee_jobs_1217312
  return employee_jobs_1217312

def test_transform_staff_org_assoc_id(jobs):
  # Trouble-shooting queries:
  # select * from pure_eligible_emp_job where emplid = '1217312' order by effdt, effseq;

  # select emplid, status_flg, count(*) from pure_eligible_emp_job where status_flg = 'C' group by emplid, status_flg having count(*) > 1;
  # select * from pure_eligible_emp_job where emplid = '8003946' order by effdt, effseq;
  assert person.transform_staff_org_assoc_id(jobs.jobs_with_primary, '6030') == jobs.jobs_with_staff_org_assoc_id

@pytest.fixture(params=['4604830','2110507'])
def jobs_before_after_primary(request):
  from . import emp_job_entries_4604830
  from . import employee_jobs_2110507
  job_sets = {
    '4604830': emp_job_entries_4604830,
    '2110507': employee_jobs_2110507,
  }
  job_set = job_sets[request.param]
  yield job_set

def test_transform_primary_job(jobs_before_after_primary):
  assert person.transform_primary_job([], [], jobs_before_after_primary.jobs, '0') == jobs_before_after_primary.jobs_with_primary

@pytest.fixture(params=['1217312','2110507'])
def jobs_with_transformed_staff_type(request):
  from . import employee_jobs_1217312
  from . import employee_jobs_2110507
  job_sets = {
    '1217312': employee_jobs_1217312,
    '2110507': employee_jobs_2110507,
  }
  job_set = job_sets[request.param]
  yield job_set

def test_transform_staff_type(jobs_with_transformed_staff_type):
  assert person.transform_staff_type(jobs_with_transformed_staff_type.jobs_with_primary) == jobs_with_transformed_staff_type.jobs_with_transformed_staff_type

@pytest.fixture(params=['1217312','2110507'])
def jobs_with_transformed_profiled(request):
  from . import employee_jobs_1217312
  from . import employee_jobs_2110507
  from . import emp_job_entries_1082441
  job_sets = {
    '1217312': employee_jobs_1217312,
    '2110507': employee_jobs_2110507,
    '1082441': emp_job_entries_1082441,
  }
  job_set = job_sets[request.param]
  yield job_set

def test_transform_profiled(jobs_with_transformed_profiled):
  assert person.transform_profiled(jobs_with_transformed_profiled.jobs_with_primary) == jobs_with_transformed_profiled.transformed_profiled

def test_transform(session):
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
    'tenure_flag': 'Y',
    'tenure_track_flag': 'N',
    'primary_empl_rcdno': '0',
  }
  transformed_person_dict = person.transform(session, person_dict)
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
    'tenure_flag': 'Y',
    'tenure_track_flag': 'N',
    'visibility': 'Public',
    'profiled': True,
    'jobs': [
      {
        'affiliation_id': '9402',
        'deptid': '11130',
        'um_campus': 'TXXX',
        'empl_rcdno': '0',
        'employment_type': 'faculty',
        'end_date': None,
        'job_title': 'Associate Professor',
        'job_description': 'Associate Professor',
        'org_id': 'IHRBIHRB',
        'staff_type': 'academic',
        'start_date': datetime.datetime(2020, 8, 31, 0, 0),
        'primary': True,
        'visibility': 'Public',
        'profiled': True,
        'staff_org_assoc_id': 'autoid:8185-IHRBIHRB-Associate Professor-faculty-2020-08-31',
      },
      {
        'affiliation_id': '9403',
        'deptid': '11130',
        'um_campus': 'TXXX',
        'empl_rcdno': '0',
        'employment_type': 'faculty',
        'end_date': datetime.datetime(2020, 8, 31, 0, 0),
        'job_title': 'Assistant Professor',
        'job_description': 'Assistant Professor',
        'org_id': 'IHRBIHRB',
        'staff_type': 'nonacademic',
        'start_date': datetime.datetime(2014, 8, 29, 0, 0),
        'primary': False,
        'visibility': 'Restricted',
        'profiled': False,
        'staff_org_assoc_id': 'autoid:8185-IHRBIHRB-Assistant Professor-faculty-2014-08-29',
      },
    ],
  }
  assert transformed_person_dict == expected_transformed_person_dict

@pytest.mark.skip(reason="serialization moved to edw_to_pure modules")
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

@pytest.mark.skip(reason="serialization moved to edw_to_pure modules")
def test_extract_transform_serialize(session):
  person_xml = person.extract_transform_serialize(session, '5150075')

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

  person_xml_2 = person.extract_transform_serialize(session, '2585238')

  expected_person_xml_2 = """<person id="898">
  <name>
    <v3:firstname>Abigail</v3:firstname>
    <v3:lastname>Gewirtz</v3:lastname>
  </name>
  <gender>unknown</gender>
  <organisationAssociations>
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
    <staffOrganisationAssociation id="autoid:898-RQHKJLUF-Professor-faculty-2015-08-31" managedInPure="false">
      <employmentType>faculty</employmentType>
      <primaryAssociation>false</primaryAssociation>
      <organisation>
        <v3:source_id>RQHKJLUF</v3:source_id>
      </organisation>
      <period>
        <v3:startDate>31-08-2015</v3:startDate>
      </period>
      <staffType>nonacademic</staffType>
      <jobDescription><v3:text lang="en">Professor</v3:text></jobDescription>
    </staffOrganisationAssociation>
    <staffOrganisationAssociation id="autoid:898-RQHKJLUF-Associate Professor-faculty-2012-05-28" managedInPure="false">
      <employmentType>faculty</employmentType>
      <primaryAssociation>false</primaryAssociation>
      <organisation>
        <v3:source_id>RQHKJLUF</v3:source_id>
      </organisation>
      <period>
        <v3:startDate>28-05-2012</v3:startDate>
        <v3:endDate>31-08-2015</v3:endDate>
      </period>
      <staffType>nonacademic</staffType>
      <jobDescription><v3:text lang="en">Associate Professor</v3:text></jobDescription>
    </staffOrganisationAssociation>
    <staffOrganisationAssociation id="autoid:898-CBIHJRCYWWAA-Adjunct Associate Professor-adjunct_faculty-2015-04-06" managedInPure="false">
      <employmentType>adjunct_faculty</employmentType>
      <primaryAssociation>false</primaryAssociation>
      <organisation>
        <v3:source_id>CBIHJRCYWWAA</v3:source_id>
      </organisation>
      <period>
        <v3:startDate>06-04-2015</v3:startDate>
        <v3:endDate>13-01-2017</v3:endDate>
      </period>
      <staffType>nonacademic</staffType>
      <jobDescription><v3:text lang="en">Adjunct Associate Professor</v3:text></jobDescription>
    </staffOrganisationAssociation>
    <staffOrganisationAssociation id="autoid:898-CBIHJRCYWWAA-Adjunct Assistant Professor-adjunct_faculty-2017-07-01" managedInPure="false">
      <employmentType>adjunct_faculty</employmentType>
      <primaryAssociation>false</primaryAssociation>
      <organisation>
        <v3:source_id>CBIHJRCYWWAA</v3:source_id>
      </organisation>
      <period>
        <v3:startDate>01-07-2017</v3:startDate>
        <v3:endDate>02-07-2017</v3:endDate>
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
  <profiled>false</profiled>
</person>"""

  assert person_xml_2 == expected_person_xml_2

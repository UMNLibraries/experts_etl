import json
import os
import re
import uuid

from sqlalchemy import and_, func, text

from experts_dw import db
from experts_dw.grad_program import update_pure_eligible_graduate_program_view
from experts_dw.models import PureEligiblePersonNew, PureEligiblePersonChngHst, PureEligibleDemogNew, PureEligibleDemogChngHst, Person, PureSyncPersonDataScratch, PureSyncStaffOrgAssociationScratch, PureSyncStudentOrgAssociationScratch, PureSyncUserDataScratch
from experts_dw.sqlapi import sqlapi
from experts_etl import loggers
from experts_etl.demographics import latest_demographics_for_emplid, latest_not_null_internet_id_for_emplid
from experts_etl.umn_data_error import record_person_no_job_data_error
from . import affiliate_job, employee_job, poi_job, graduate_program

# defaults:

db_name = 'hotel'
transaction_record_limit = 100

def run(
    # Do we need other default functions here?
    #extract_api_persons=extract_api_persons,
    db_name=db_name,
    transaction_record_limit=transaction_record_limit,
    experts_etl_logger=None
):
    # This connection API in general needs work. Including this here for the sake of consistency
    # with other ETL module.run() functions.
    sqlapi.setengine(db.engine(db_name))

    if experts_etl_logger is None:
        experts_etl_logger = loggers.experts_etl_logger()
        experts_etl_logger.info('starting: oit -> edw', extra={'pure_sync_job': 'person'})

    engine = db.engine(db_name)
    prepare_target_scratch_tables(engine)

    with db.session(db_name) as session:
        prepare_source_tables(engine, session)

        load_count = 0
        for demog in session.query(PureEligibleDemogChngHst.emplid).distinct().all():
            emplid = demog.emplid
            # Stop-gap prevention of unique key constraint violation:
            if emplid == '8004768':
                continue
            person_dict = transform(session, extract(session, emplid))
            if (len(person_dict['jobs']) == 0 and not person_has_future_staff_associations(session, emplid) \
                and len(person_dict['programs']) == 0 and not person_has_previous_student_associations(session, emplid)
            ):
                record_person_no_job_data_error(
                    session=session,
                    emplid=demog.emplid,
                    internet_id=person_dict['internet_id'],
                )
                continue
            load_into_scratch(session, person_dict)
            load_count += 1
            if load_count >= transaction_record_limit:
                session.commit()
                load_count = 0

        update_targets_from_scratch()

        session.commit()

    experts_etl_logger.info('ending: oit -> edw', extra={'pure_sync_job': 'person'})

def person_has_future_staff_associations(session, emplid):
    # A common data entry pattern is that new employees with pure eligible jobs appear with current employment dates,
    # which are later changed to future employment start dates, with a status flag of 'F' for 'Future', which then
    # makes those jobs pure ineligible. Because those jobs will become eligible again after the future start dates,
    # we check for that here to prevent treating these cases as errors.
    staff_assocs = session.execute(f"SELECT * FROM pure_sync_staff_org_association WHERE person_id='{emplid}'").fetchall()
    for staff_assoc in staff_assocs:
        jobcode, deptid = staff_assoc['affiliation_id'], staff_assoc['deptid']
        if session.execute(
            f"SELECT COUNT(*) FROM ps_dwhr_job@dweprd.oit WHERE emplid='{emplid}' AND status_flg='F' AND jobcode='{jobcode}' AND deptid='{deptid}'"
        ).scalar():
            return True
    return False

def person_has_previous_student_associations(session, emplid):
    # Because UMN creates new student data tables for each new term, some students who had data in previous tables
    # may not have data in the newest table, for example if they have finished their programs. We check for that
    # here to prevent treating the lack of data in our views, based on the newest table, as an error.
    return session.execute(f"SELECT COUNT(*) FROM pure_sync_student_org_association WHERE person_id='{emplid}'").scalar()

def update_targets_from_scratch():
    with sqlapi.transaction():
        sqlapi.update_pure_sync_person_data()
        sqlapi.insert_pure_sync_person_data()

        sqlapi.update_pure_sync_user_data()
        sqlapi.insert_pure_sync_user_data()

        sqlapi.update_pure_sync_staff_org_association()
        sqlapi.insert_pure_sync_staff_org_association()

        sqlapi.delete_obsolete_primary_jobs()

        sqlapi.update_pure_sync_student_org_association()
        sqlapi.insert_pure_sync_student_org_association()

def load_into_scratch(session, person_dict):
    pure_sync_person_data = PureSyncPersonDataScratch(
        person_id=person_dict['person_id'],
        first_name=person_dict['first_name'],
        last_name=person_dict['last_name'],
        visibility=person_dict['visibility'],
        profiled=person_dict['profiled'],
        emplid=person_dict['emplid'],
        internet_id=person_dict['internet_id'],
        postnominal=person_dict['name_suffix'],
    )
    session.add(pure_sync_person_data)

    for job in person_dict['jobs']:
        pure_sync_staff_org_association = PureSyncStaffOrgAssociationScratch(
            affiliation_id=job['affiliation_id'],
            staff_org_association_id=job['staff_org_assoc_id'],
            person_id=person_dict['person_id'],
            period_start_date=job['start_date'],
            period_end_date=job['end_date'],
            org_id=job['org_id'],
            employment_type=job['employment_type'],
            staff_type=job['staff_type'],
            visibility=job['visibility'],
            primary_association=job['primary'],
            job_description=job['job_description'],
            email_address=job['email_address'],
            deptid=job['deptid'],
        )
        session.add(pure_sync_staff_org_association)

    for program in person_dict['programs']:
        pure_sync_student_org_association = PureSyncStudentOrgAssociationScratch(
            affiliation_id=program['affiliation_id'],
            student_org_association_id=program['student_org_association_id'],
            person_id=person_dict['person_id'],
            period_start_date=program['period_start_date'],
            period_end_date=program['period_end_date'],
            org_id=program['org_id'],
            status=program['status'],
            email_address=program['email_address'],
        )
        session.add(pure_sync_student_org_association)

    if person_dict['internet_id'] is not None:
        pure_sync_user_data = PureSyncUserDataScratch(
            person_id=person_dict['person_id'],
            first_name=person_dict['first_name'],
            last_name=person_dict['last_name'],
            user_name=person_dict['internet_id'],
            email=person_dict['internet_id'] + '@umn.edu',
        )
        session.add(pure_sync_user_data)

def prepare_target_scratch_tables(engine):
    engine.execute('delete pure_sync_user_data_scratch')
    engine.execute('delete pure_sync_staff_org_association_scratch')
    engine.execute('delete pure_sync_student_org_association_scratch')
    engine.execute('delete pure_sync_person_data_scratch')

def prepare_source_tables(engine, session):
    # Yes, this is an ugly way to use a cx_Oracle cursor. Will have to do
    # for now, until we modify this whole codebase to use them.
    with db.cx_oracle_connection() as conn:
        update_pure_eligible_graduate_program_view(conn.cursor())

    update_pure_eligible_persons(engine, session)
    update_pure_eligible_demographics(engine, session)

def update_pure_eligible_persons(engine, session):
    engine.execute('truncate table pure_eligible_person_new')
    engine.execute('''
insert into pure_eligible_person_new (emplid)
select emplid from pure_eligible_person
minus
select emplid from pure_eligible_person_chng_hst
    ''')
    for person_new in session.query(PureEligiblePersonNew).all():
        person = PureEligiblePersonChngHst(emplid = person_new.emplid)
        session.add(person)
    session.commit()

def update_pure_eligible_demographics(engine, session):
    engine.execute('truncate table pure_eligible_demog_new')
    engine.execute('''
insert into pure_eligible_demog_new (
  emplid,
  internet_id,
  name,
  last_name,
  first_name,
  middle_initial,
  name_suffix,
  instl_email_addr,
  tenure_flag,
  tenure_track_flag,
  primary_empl_rcdno
)
select
  emplid,
  internet_id,
  name,
  last_name,
  first_name,
  middle_initial,
  name_suffix,
  instl_email_addr,
  tenure_flag,
  tenure_track_flag,
  to_char(primary_empl_rcdno)
from pure_eligible_demographics
minus
select
  pe1.emplid,
  pe1.internet_id,
  pe1.name,
  pe1.last_name,
  pe1.first_name,
  pe1.middle_initial,
  pe1.name_suffix,
  pe1.instl_email_addr,
  pe1.tenure_flag,
  pe1.tenure_track_flag,
  pe1.primary_empl_rcdno
from pure_eligible_demog_chng_hst pe1
where pe1.timestamp = (select max(timestamp) from pure_eligible_demog_chng_hst pe2 where pe1.emplid = pe2.emplid)
    ''')
    count = 0
    for demog_new in session.query(PureEligibleDemogNew).all():
        demog = PureEligibleDemogChngHst(
            emplid = demog_new.emplid,
            internet_id = demog_new.internet_id,
            name = demog_new.name,
            last_name = demog_new.last_name,
            first_name = demog_new.first_name,
            middle_initial = demog_new.middle_initial,
            name_suffix = demog_new.name_suffix,
            instl_email_addr = demog_new.instl_email_addr,
            tenure_flag = demog_new.tenure_flag,
            tenure_track_flag = demog_new.tenure_track_flag,
            primary_empl_rcdno = demog_new.primary_empl_rcdno
        )
        session.add(demog)
        count += 1
        if not count % 100:
            session.commit()
    session.commit()

def extract_transform_serialize(session, emplid):
    person_dict = extract(session, emplid)
    return serialize(transform(session, person_dict))

def extract(session, emplid):
    demog = latest_demographics_for_emplid(session, emplid)
    person_dict = {c.name: getattr(demog, c.name) for c in demog.__table__.columns}

    if person_dict['internet_id'] is None:
        person_dict['internet_id'] = latest_not_null_internet_id_for_emplid(session, emplid)

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

def transform(session, person_dict):
    person_dict['person_id'] = transform_person_id(
        person_dict['emplid'],
        person_dict['scival_id']
    )

    person_dict['first_name'] = transform_first_name(
        person_dict['first_name'],
        person_dict['middle_initial']
    )

    person_dict['programs'] = graduate_program.extract_transform(session, person_dict['emplid'])

    employee_jobs = employee_job.extract_transform(session, person_dict['emplid'])
    poi_jobs = poi_job.extract_transform(session, person_dict['emplid'])
    affiliate_jobs = affiliate_job.extract_transform(session, person_dict['emplid'])
    jobs_with_primary = transform_primary_job(
        affiliate_jobs,
        poi_jobs,
        employee_jobs,
        person_dict['primary_empl_rcdno']
    )

    person_dict['profiled'] = False
    person_dict['visibility'] = 'Restricted'
    if len(jobs_with_primary) > 0:
        person_dict['profiled'] = transform_profiled(jobs_with_primary)
        jobs_with_staff_type = transform_staff_type(jobs_with_primary)
        person_dict['jobs'] = transform_staff_org_assoc_id(jobs_with_staff_type, person_dict['person_id'])

        for job in person_dict['jobs']:
            job['email_address'] = person_dict['instl_email_addr']
            if job['visibility'] == 'Public':
                person_dict['visibility'] = 'Public'
    else:
        person_dict['jobs'] = []

    return person_dict

def serialize(person_dict):
    pass
    #template = env.get_template('person.xml.j2')
    #return template.render(person_dict)

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

def transform_primary_job(affiliate_jobs, poi_jobs, employee_jobs, primary_empl_rcdno):
    # Handle some easy cases first:

    # If there are no jobs, there's nothing to do:
    if (len(affiliate_jobs) == 0 and len(poi_jobs) == 0 and len(employee_jobs) == 0):
        return []

    # Start by setting primary to False for all jobs:
    transformed_aff_jobs = [{**d, **{'primary': False}} for d in affiliate_jobs.copy()]
    transformed_emp_poi_jobs = [{**d, **{'primary': False}} for d in employee_jobs.copy() + poi_jobs.copy()]

    # If there is only one job, it must be primary:
    all_jobs = transformed_aff_jobs + transformed_emp_poi_jobs
    if len(all_jobs) == 1:
        all_jobs[0]['primary'] = True
        return all_jobs

    # Separate active and inactive jobs:
    active_aff_jobs = [job for job in transformed_aff_jobs if job['end_date'] is None]
    active_emp_poi_jobs = [job for job in transformed_emp_poi_jobs if job['end_date'] is None]
    all_active_jobs = active_aff_jobs + active_emp_poi_jobs
    inactive_aff_jobs = [job for job in transformed_aff_jobs if job['end_date']]
    inactive_emp_poi_jobs = [job for job in transformed_emp_poi_jobs if job['end_date']]
    all_inactive_jobs = inactive_aff_jobs + inactive_emp_poi_jobs

    # If there is only one active job, it must be primary:
    if len(all_active_jobs) == 1:
        all_active_jobs[0]['primary'] = True
        return all_active_jobs + all_inactive_jobs

    # If there is at least one active non-affiliate job, and one such job is associated with
    # the primary employee record number, set that job as primary. Otherwise, set the first in
    # the list of those jobs as primary:
    if len(active_emp_poi_jobs) >= 1:
        primary_job_set = False
        for job in active_emp_poi_jobs:
            if re.match(r'^\d$', job['empl_rcdno']) and job['empl_rcdno'] == str(primary_empl_rcdno):
                job['primary'] = True
                primary_job_set = True
                break
        if not primary_job_set:
            active_emp_poi_jobs[0]['primary'] = True
        return active_emp_poi_jobs + active_aff_jobs + all_inactive_jobs

    # If there are no active non-affiliate jobs, but there are active affiliate jobs,
    # set as primary the first active affiliate job:
    if len(active_emp_poi_jobs) == 0 and len(active_aff_jobs) > 0:
        active_aff_jobs[0]['primary'] = True
        return active_aff_jobs + all_inactive_jobs

    # If we get here, there are no active jobs, so set as primary the job with the latest end date:
    all_jobs_reverse_date_sorted = sorted(all_jobs, key=lambda d: d['end_date'], reverse=True)
    all_jobs_reverse_date_sorted[0]['primary'] = True
    return all_jobs_reverse_date_sorted

def transform_staff_type(jobs_with_primary):
    primary_job = next((job for job in jobs_with_primary if job['primary'] is True), None)
    if primary_job['staff_type'] == 'academic':
        return jobs_with_primary

    # If the staff_type of the primary job is not 'academic', We set the staff type of all 
    # jobs to 'nonacademic', to help stay under our contractual limit of academic persons.
    transformed_jobs = jobs_with_primary.copy()
    for job in transformed_jobs:
        job['staff_type'] = 'nonacademic'
    return transformed_jobs

def transform_profiled(jobs_with_primary):
    primary_job = next((job for job in jobs_with_primary if job['primary'] is True), None)
    if primary_job['profiled'] is True and primary_job['end_date'] is None:
        return True
    else:
        return False

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

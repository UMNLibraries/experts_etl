import json
import os
import re
import uuid

from sqlalchemy import and_, func, text

from experts_dw import db
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
            # Stop-gap prevention of unique key constraint violation:
            if demog.emplid == '8004768':
                continue
            person_dict = transform(session, extract(session, demog.emplid))
            if len(person_dict['jobs']) == 0 and len(person_dict['programs']) == 0:
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
        jobs = transform_staff_type(jobs_with_primary)
        person_dict['profiled'] = transform_profiled(jobs)
        person_dict['jobs'] = transform_staff_org_assoc_id(jobs, person_dict['person_id'])

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

    if (len(affiliate_jobs) == 0 and len(poi_jobs) == 0 and len(employee_jobs) == 0):
        return []

    transformed_aff_jobs = affiliate_jobs.copy()
    transformed_emp_poi_jobs = employee_jobs.copy() + poi_jobs.copy()
    transformed_jobs = []
    primary_job_set = False

    # Affiliate jobs have no empl_rcdno, and thus no primary_empl_rcdno will ever match them,
    # so they default to false:
    for job in transformed_aff_jobs:
        job['primary'] = False

    # Since affiliate jobs have no empl_rcdno, if there is only one employee/poi job, it must
    # be primary:
    if (len(transformed_emp_poi_jobs) == 1):
        transformed_emp_poi_jobs[0]['primary'] = True
        primary_job_set = True

    # If the only job we have is one affiliate job, it also must be primary:
    elif (len(transformed_emp_poi_jobs) == 0 and len(transformed_aff_jobs) == 1):
        transformed_aff_jobs[0]['primary'] = True
        primary_job_set = True

    if primary_job_set:
        transformed_jobs.extend(transformed_emp_poi_jobs)
        transformed_jobs.extend(transformed_aff_jobs)

        primary_job = next((job for job in transformed_jobs if job['primary'] is True), None)
        if primary_job is None:
            raise RuntimeError('failed to set a primary association')

        return transformed_jobs

    # End of the easy cases.

    # Some data we may need later, because now we will treat active and inactive jobs differently:
    inactive_emp_poi_jobs = []
    active_emp_poi_jobs = []
    active_earliest_start_dates = []

    # Any currently-active jobs are likely to be at or near the end of the list:
    transformed_emp_poi_jobs.reverse()
    for job in transformed_emp_poi_jobs:
        job['primary'] = False

        if job['end_date'] == None:
            # Try to use an active job as the primary job:
            if not primary_job_set and re.match(r'^\d$', job['empl_rcdno']) and job['empl_rcdno'] == str(primary_empl_rcdno):
                job['primary'] = True
                primary_job_set = True

            active_emp_poi_jobs.append(job)
            if len(active_earliest_start_dates) == 0 or job['start_date'] < min(active_earliest_start_dates):
                active_earliest_start_dates = [job['start_date']]
            elif job['start_date'] == min(active_earliest_start_dates):
                active_earliest_start_dates.append(job['start_date'])

        else:
            inactive_emp_poi_jobs.append(job)

    # From Jan Fransen on setting a primary position in email, 2018-01-23:
    # Rather than being completely arbitrary, make the position with the earliest
    # effective date the primary. If they have the same effective date, then
    # choose the lowest empl_rcdno. Hopefully it's not duplicated to that level,
    # but if it is then just pick one.

    if not primary_job_set and active_emp_poi_jobs:
        if len(active_earliest_start_dates) == 1:
            for job in active_emp_poi_jobs:
                if job['start_date'] == active_earliest_start_dates[0]:
                    job['primary'] = True
                    primary_job_set = True
                    break
        else:
            active_lowest_empl_rcdnos = []
            for job in active_emp_poi_jobs:
                if job['start_date'] == min(active_earliest_start_dates):
                    if len(active_lowest_empl_rcdnos) == 0 or job['empl_rcdno'] < min(active_lowest_empl_rcdnos):
                        active_lowest_empl_rcdnos = [job['empl_rcdno']]
                    elif job['empl_rcdno'] == min(active_lowest_empl_rcdnos):
                        active_lowest_empl_rcdnos.append(job['empl_rcdno'])

            if len(active_lowest_empl_rcdnos) == 1:
                for job in active_emp_poi_jobs:
                    if job['empl_rcdno'] == active_lowest_empl_rcdnos[0] and job['start_date'] == min(active_earliest_start_dates):
                        job['primary'] = True
                        primary_job_set = True
                        break
            else:
                # Just pick one of the active jobs with the earliest start date and lowest empl_rcdno:
                for job in active_emp_poi_jobs:
                    if job['empl_rcdno'] == min(active_lowest_empl_rcdnos) and job['start_date'] == min(active_earliest_start_dates):
                        job['primary'] = True
                        primary_job_set = True
                        break

    if not primary_job_set:
        # This must mean that there were no active employee/poi jobs, so choose any active
        # affiliate job as the primary:
        for job in transformed_aff_jobs:
            if job['end_date'] == None:
                job['primary'] = True
                primary_job_set = True
                break

    if not primary_job_set and inactive_emp_poi_jobs:
        # There must be no active jobs of any kind, so try to set as primary any inactive employee/poi
        # job that matches the primary_empl_rcdno:
        for job in inactive_emp_poi_jobs:
            if not primary_job_set and re.match(r'^\d$', job['empl_rcdno']) and job['empl_rcdno'] == str(primary_empl_rcdno):
                job['primary'] = True
                primary_job_set = True
                break
        if not primary_job_set:
            # In this case none of the inactive emp/poi jobs matched the primary_empl_rcdno, so just pick one:
            inactive_emp_poi_jobs[0]['primary'] = True
            primary_job_set = True

    if not primary_job_set:
        # In this case the person must have nothing but inactive affiliate jobs. Just pick one:
        transformed_aff_jobs[0]['primary'] = True
        primary_job_set = True

    if not primary_job_set:
        # This should never happen!
        raise RuntimeError('failed to set a primary association')

    transformed_jobs = active_emp_poi_jobs
    transformed_jobs.extend(inactive_emp_poi_jobs)
    transformed_jobs.extend(transformed_aff_jobs)

    # This is redundant, but is a better check than what we were doing previously.
    primary_job = next((job for job in transformed_jobs if job['primary'] is True), None)
    if primary_job is None:
        raise RuntimeError('failed to set a primary association')

    return transformed_jobs

def transform_staff_type(jobs_with_primary):
    primary_job = next((job for job in jobs_with_primary if job['primary'] is True), None)
    if primary_job['staff_type'] == 'academic':
        return jobs_with_primary

    transformed_jobs = jobs_with_primary.copy()
    for job in transformed_jobs:
        job['staff_type'] = 'nonacademic'
    return transformed_jobs

def transform_profiled(jobs):
    primary_job = next((job for job in jobs if job['primary'] is True), None)
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

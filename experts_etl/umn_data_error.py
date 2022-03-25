from datetime import datetime
from experts_dw import db
from experts_dw.rawsql import unreported_umn_data_errors, record_reporting_of_umn_data_errors, count_pure_eligible_persons_in_dept
from experts_dw.models import UmnDataError
from experts_etl.exceptions import *
from experts_etl import loggers

from sqlalchemy.sql import text

import os
import csv
from io import StringIO
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib

db_name = 'hotel'

def run(
    db_name=db_name,
    experts_etl_logger=None,
    smtp_server=None,
    from_address=None,
    ticket_address=None
):

    if experts_etl_logger is None:
        experts_etl_logger = loggers.experts_etl_logger()
    experts_etl_logger.info('starting: error reporting', extra={'pure_sync_job': 'error_reporting'})

    if smtp_server is None:
        smtp_server = smtplib.SMTP('localhost', 25)
    if from_address is None:
        from_address = os.environ.get('EXPERTS_ETL_FROM_EMAIL_ADDRESS')
    if ticket_address is None:
        ticket_address = os.environ.get('EXPERTS_ETL_TICKET_EMAIL_ADDRESS')

    with db.cx_oracle_connection() as connection:
        report_via_email(connection, experts_etl_logger, smtp_server=smtp_server, from_address=from_address, ticket_address=ticket_address)
    smtp_server.quit()

    experts_etl_logger.info('ending: error reporting', extra={'pure_sync_job': 'error_reporting'})

def report_via_email(connection, experts_etl_logger, smtp_server, from_address, ticket_address):
    report_cursor = connection.cursor()
    report_cursor.execute(unreported_umn_data_errors)
    report_cursor.rowfactory = lambda *args: dict(
        zip([col[0] for col in report_cursor.description], args)
    )
    # Return a list of dicts
    unreported_errors = report_cursor.fetchall()
    report_cursor.close()

    if len(unreported_errors) == 0:
        return

    # Build and send e-mail with csv attachment of errors
    datetime_string = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')

    message = MIMEMultipart()
    message['Subject'] = f'UMN Data Error Report {datetime_string}'
    message['From'] = from_address
    message['To'] = ticket_address
    message.preamble = 'UMN Data Error Report {datetime_string}.\n'

    message.attach(MIMEText(
        'A sync to Pure found the attached data anomalies/errors, which may require manual intervention.',
        'plain'
    ))

    part = MIMEBase('application', 'octet-stream')
    part.set_payload(create_csv_report(unreported_errors))
    encoders.encode_base64(part)
    part.add_header('Content-Disposition', f'attachment; filename="umn-data-errors-{datetime_string}.csv"')
    message.attach(part)

    smtp_server.send_message(message)

    # Open another cursor and record the reporting
    try:
        record_cursor = connection.cursor()
        record_cursor.execute(record_reporting_of_umn_data_errors)
        connection.commit()
    except Exception as e:
        connection.rollback()
        formatted_exception = loggers.format_exception(e)
        experts_etl_logger.error(
            f'Exception encountered during recording of data errors: {formatted_exception}'
        )

def create_csv_report(unreported_errors):
    # Remove fields which may confuse people doing data entry:
    for error in unreported_errors:
        for key in ['error_id','first_seen','last_seen','count','reported','notes']:
            error.pop(key, None)
    fieldnames = [
      'message',
      'emplid',
      'internet_id',
      'jobcode',
      'jobcode_descr',
      'deptid',
      'deptid_descr',
      'persons_in_dept',
      'um_college',
      'um_college_descr',
      'um_campus',
      'um_campus_descr',
    ]

    csv_report = StringIO()
    writer = csv.DictWriter(csv_report, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(unreported_errors)
    return csv_report.getvalue()

def record_person_no_job_data_error(session, *, emplid, internet_id=None):
    session.add(
        find_or_create_umn_data_error(
            session=session,
            exception=ExpertsEtlPersonNoJobData(emplid=emplid, internet_id=internet_id),
        )
    )
    session.commit()

def record_unknown_dept_errors(
    session,
    *,
    emplid,
    internet_id=None,
    jobcode,
    jobcode_descr=None,
    deptid,
    deptid_descr=None,
    persons_in_dept=None,
    um_college=None,
    um_college_descr=None,
    um_campus=None,
    um_campus_descr=None,
):
    if persons_in_dept is None:
        # We are using a SQL Alchemy session to execute a count statement. A workaround from having to introduce new connection objects into the process.
        persons_in_dept = session.execute(text(count_pure_eligible_persons_in_dept), {"deptid": deptid}).scalars().one_or_none()

    session.add(
        find_or_create_umn_data_error(
            session=session,
            exception=ExpertsEtlUnknownDept(
                deptid=deptid,
                deptid_descr=deptid_descr,
                persons_in_dept=persons_in_dept,
                um_college=um_college,
                um_college_descr=um_college_descr,
                um_campus=um_campus,
                um_campus_descr=um_campus_descr,
            ),
        )
    )
    session.add(
        find_or_create_umn_data_error(
            session=session,
            exception=ExpertsEtlJobWithUnknownDept(
                emplid=emplid,
                internet_id=internet_id,
                jobcode=jobcode,
                jobcode_descr=jobcode_descr,
                deptid=deptid,
                deptid_descr=deptid_descr,
                um_college=um_college,
                um_college_descr=um_college_descr,
                um_campus=um_campus,
                um_campus_descr=um_campus_descr,
            ),
        )
    )
    session.commit()

def record_unknown_jobcode_deptid_errors(
    session,
    *,
    emplid,
    internet_id=None,
    jobcode,
    jobcode_descr=None,
    deptid,
    deptid_descr=None,
    um_college=None,
    um_college_descr=None,
    um_campus=None,
    um_campus_descr=None,
):
    session.add(
        find_or_create_umn_data_error(
            session=session,
            exception=ExpertsEtlUnknownJobcodeDeptid(
                jobcode=jobcode,
                jobcode_descr=jobcode_descr,
                deptid=deptid,
                deptid_descr=deptid_descr,
                um_college=um_college,
                um_college_descr=um_college_descr,
                um_campus=um_campus,
                um_campus_descr=um_campus_descr,
            ),
        )
    )
    session.add(
        find_or_create_umn_data_error(
            session=session,
            exception=ExpertsEtlJobWithUnknownJobcodeDeptid(
                emplid=emplid,
                internet_id=internet_id,
                jobcode=jobcode,
                jobcode_descr=jobcode_descr,
                deptid=deptid,
                deptid_descr=deptid_descr,
                um_college=um_college,
                um_college_descr=um_college_descr,
                um_campus=um_campus,
                um_campus_descr=um_campus_descr,
            ),
        )
    )
    session.commit()

def find_or_create_umn_data_error(session, *, exception):
    umn_data_error = session.query(UmnDataError).filter(
        UmnDataError.error_id == exception.id
    ).one_or_none()
    if umn_data_error:
        umn_data_error.last_seen = datetime.now()
        umn_data_error.count = umn_data_error.count + 1
    else:
        umn_data_error = UmnDataError(
            error_id=exception.id,
            message=exception.message,
            emplid=exception.emplid,
            internet_id=exception.internet_id,
            jobcode=exception.jobcode,
            jobcode_descr=exception.jobcode_descr,
            deptid=exception.deptid,
            deptid_descr=exception.deptid_descr,
            persons_in_dept=exception.persons_in_dept,
            um_college=exception.um_college,
            um_college_descr=exception.um_college_descr,
            um_campus=exception.um_campus,
            um_campus_descr=exception.um_campus_descr,
            count=1,
        )
    return umn_data_error

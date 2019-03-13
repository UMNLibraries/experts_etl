from datetime import datetime
from experts_dw.models import UmnDataError
from experts_etl.exceptions import *

def report_person_no_job_data_error(session, emplid):
    session.add(
        get_umn_data_error(
            session=session,
            exception=ExpertsEtlPersonNoJobData(emplid=emplid),
            emplid=emplid
        )
    )
    session.commit()

def report_unknown_dept_errors(session, jobcode, deptid, emplid):
    session.add(
        get_umn_data_error(
            session=session,
            exception=ExpertsEtlUnknownDept(deptid=deptid),
            deptid=deptid,
        )
    )
    session.add(
        get_umn_data_error(
            session=session,
            exception=ExpertsEtlJobWithUnknownDept(jobcode=jobcode, deptid=deptid, emplid=emplid),
            jobcode=jobcode,
            deptid=deptid,
            emplid=emplid,
        )
    )
    session.commit()

def report_unknown_jobcode_deptid_errors(session, jobcode, deptid, emplid):
    session.add(
        get_umn_data_error(
            session=session,
            exception=ExpertsEtlUnknownJobcodeDeptid(jobcode=jobcode, deptid=deptid),
            jobcode=jobcode,
            deptid=deptid,
        )
    )
    session.add(
        get_umn_data_error(
            session=session,
            exception=ExpertsEtlJobWithUnknownJobcodeDeptid(jobcode=jobcode, deptid=deptid, emplid=emplid),
            jobcode=jobcode,
            deptid=deptid,
            emplid=emplid,
        )
    )
    session.commit()

def get_umn_data_error(session, exception, jobcode=None, deptid=None, emplid=None):
    umn_data_error = session.query(UmnDataError).filter(
        UmnDataError.error_id == exception.id
    ).one_or_none()
    if umn_data_error:
        umn_data_error.last_seen = datetime.now()
        umn_data_error.count = umn_data_error.count + 1
    else:
        umn_data_error = UmnDataError(
            error_id = exception.id,
            message = exception.message,
            jobcode = jobcode,
            deptid = deptid,
            emplid = emplid,
            count = 1
        )
    return umn_data_error

from experts_dw import db
from experts_dw.sqlapi import sqlapi

from experts_etl import loggers

# defaults:

db_name = 'hotel'

def run(
    db_name=db_name,
    experts_etl_logger=None
):
    if experts_etl_logger is None:
        experts_etl_logger = loggers.experts_etl_logger()
        experts_etl_logger.info('starting: oit -> edw', extra={'pure_sync_job': 'award_project'})

    # This connection API in general needs work. Including this here for the sake of consistency
    # with other ETL module.run() functions.
    engine = db.engine(db_name)
    sqlapi.setengine(engine)
    try:
        with sqlapi.transaction():
            # Due to FK constraints, these two functions must be run in this order,
            # and before all those below:
            sqlapi.update_pure_sync_project()
            sqlapi.update_pure_sync_award()

            sqlapi.update_pure_sync_award_internal_holder()
            sqlapi.update_pure_sync_award_external_holder()
            sqlapi.update_pure_sync_project_internal_participant()
            sqlapi.update_pure_sync_project_external_participant()
    except Exception as e:
        experts_etl_logger.exception('Attempt to update awards & projects failed', extra={'pure_sync_job': 'award_project'})

    experts_etl_logger.info('ending: oit -> edw', extra={'pure_sync_job': 'award_project'})

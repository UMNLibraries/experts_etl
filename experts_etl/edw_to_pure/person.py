import os
from datetime import datetime
from experts_dw import db
from experts_dw.cx_oracle_helpers import select_list_of_dicts, select_keyed_lists_of_dicts
from experts_etl.umn_data_error import record_person_no_org_associations_error
from experts_etl import loggers

from jinja2 import Environment, PackageLoader, Template, select_autoescape
env = Environment(
    loader=PackageLoader('experts_etl', 'templates'),
    autoescape=select_autoescape(['html', 'xml'])
)

# defaults:

template = env.get_template('person.xml.j2')
db_name = 'hotel'
# This dirname could use improvement before deploying to a remote machine:
dirname = os.path.dirname(os.path.realpath(__file__))
if 'EXPERTS_ETL_SYNC_DIR' in os.environ:
  dirname = os.environ['EXPERTS_ETL_SYNC_DIR']
output_filename = dirname + '/person_' + datetime.now().strftime('%Y-%m-%dT%H:%M:%S') + '.xml'

def run(
    db_name=db_name,
    template=template,
    output_filename=output_filename,
    experts_etl_logger=None
):
    if experts_etl_logger is None:
        experts_etl_logger = loggers.experts_etl_logger()
    experts_etl_logger.info('starting: edw -> pure', extra={'pure_sync_job': 'person'})

    with open(output_filename, 'w') as output_file, db.cx_oracle_connection() as connection:
        cursor = connection.cursor()

        # Preload these to avoid the n+1 queries problem:
        jobs = select_keyed_lists_of_dicts(
            cursor,
            "SELECT * FROM pure_sync_staff_org_association",
            key_column_name='PERSON_ID',
        )
        programs = select_keyed_lists_of_dicts(
            cursor,
            "SELECT * FROM pure_sync_student_org_association",
            key_column_name='PERSON_ID',
        )

        output_file.write('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n')
        output_file.write('<persons xmlns="v1.unified-person-sync.pure.atira.dk" xmlns:v3="v3.commons.pure.atira.dk">\n')
        for person in select_list_of_dicts(cursor, 'SELECT * FROM pure_sync_person_data'):
            person_id = person['PERSON_ID']
            person['jobs'] = jobs[person_id] if person_id in jobs else []
            person['programs'] = programs[person_id] if person_id in programs else []
            if len(person['jobs']) == 0 and len(person['programs']) == 0:
                record_person_no_org_associations_error(
                    session=db.session(),
                    emplid=person['EMPLID'],
                    internet_id=person['INTERNET_ID'],
                )
                continue
            output_file.write(template.render(person))

        output_file.write('</persons>')

    experts_etl_logger.info('ending: edw -> pure', extra={'pure_sync_job': 'person'})

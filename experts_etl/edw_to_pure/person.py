from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())
import os
from datetime import datetime
from experts_dw import db
from experts_dw.models import PureSyncPersonData, PureSyncStaffOrgAssociation

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
output_filename = dirname + '/person_' + datetime.utcnow().strftime('%Y_%m_%d') + '.xml'

def run(
    db_name=db_name,
    template=template,
    output_filename=output_filename,
    experts_etl_logger=None
):
    if experts_etl_logger is None:
        experts_etl_logger = loggers.experts_etl_logger()
    experts_etl_logger.info('starting: edw -> pure', extra={'pure_sync_job': 'person'})

    with open(output_filename, 'w') as output_file:
        output_file.write('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n')
        output_file.write('<persons xmlns="v1.unified-person-sync.pure.atira.dk" xmlns:v3="v3.commons.pure.atira.dk">\n')
        with db.session(db_name) as session:
            for person in session.query(PureSyncPersonData).all():
                person_dict = {c.name: getattr(person, c.name) for c in person.__table__.columns}
                person_dict['jobs'] = []
                for job in session.query(PureSyncStaffOrgAssociation).filter(
                    PureSyncStaffOrgAssociation.person_id == person.person_id
                ).all():
                    job_dict = {c.name: getattr(job, c.name) for c in job.__table__.columns}
                    person_dict['jobs'].append(job_dict)
                output_file.write(template.render(person_dict))

        output_file.write('</persons>')

    experts_etl_logger.info('ending: edw -> pure', extra={'pure_sync_job': 'person'})

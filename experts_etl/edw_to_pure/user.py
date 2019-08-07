from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())
import os
from datetime import datetime
from experts_dw import db
from experts_dw.models import PureSyncUserData

from jinja2 import Environment, PackageLoader, Template, select_autoescape
env = Environment(
    loader=PackageLoader('experts_etl', 'templates'),
    autoescape=select_autoescape(['html', 'xml'])
)

# defaults:

template = env.get_template('user.xml.j2')
db_name = 'hotel'
# This dirname could use improvement before deploying to a remote machine:
dirname = os.path.dirname(os.path.realpath(__file__))
if 'EXPERTS_ETL_SYNC_DIR' in os.environ:
  dirname = os.environ['EXPERTS_ETL_SYNC_DIR']
output_filename = dirname + '/user_' + datetime.now().strftime('%Y-%m-%dT%H:%M:%S') + '.xml'

def run(
    db_name=db_name,
    template=template,
    output_filename=output_filename,
    experts_etl_logger=None
):
    if experts_etl_logger is None:
        experts_etl_logger = loggers.experts_etl_logger()
    experts_etl_logger.info('starting: edw -> pure', extra={'pure_sync_job': 'user'})

    with open(output_filename, 'w') as output_file:
        output_file.write('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n')
        output_file.write('<users xmlns="v1.user-sync.pure.atira.dk" xmlns:v3="v3.commons.pure.atira.dk">\n')
        with db.session(db_name) as session:
            for user in session.query(PureSyncUserData).all():
                user_dict = {c.name: getattr(user, c.name) for c in user.__table__.columns}
                output_file.write(template.render(user_dict))

        output_file.write('</users>')

    experts_etl_logger.info('ending: edw -> pure', extra={'pure_sync_job': 'user'})

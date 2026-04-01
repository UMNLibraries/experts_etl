import os
from datetime import datetime
from experts_dw import db
from experts_dw.cx_oracle_helpers import select_list_of_dicts, select_keyed_lists_of_dicts

from jinja2 import Environment, PackageLoader, Template, select_autoescape
env = Environment(
    loader=PackageLoader('experts_etl', 'templates'),
    autoescape=select_autoescape(['html', 'xml'])
)

# defaults:

template = env.get_template('project.xml.j2')
db_name = 'hotel'
# This dirname could use improvement before deploying to a remote machine:
dirname = os.path.dirname(os.path.realpath(__file__))
if 'EXPERTS_ETL_SYNC_DIR' in os.environ:
  dirname = os.environ['EXPERTS_ETL_SYNC_DIR']
output_filename = dirname + '/project_' + datetime.now().strftime('%Y-%m-%dT%H:%M:%S') + '.xml'

def run(
    db_name=db_name,
    template=template,
    output_filename=output_filename,
    experts_etl_logger=None
):
    if experts_etl_logger is None:
        experts_etl_logger = loggers.experts_etl_logger()
    experts_etl_logger.info('starting: edw -> pure', extra={'pure_sync_job': 'project'})

    with open(output_filename, 'w') as output_file, db.cx_oracle_connection() as connection:
        cursor = connection.cursor()

        # Preload these to avoid the n+1 queries problem:
        internal_participants = select_keyed_lists_of_dicts(
            cursor,
            'SELECT * FROM pure_sync_project_internal_participant',
            key_column_name='PROJECT_ID',
        )
        external_participants = select_keyed_lists_of_dicts(
            cursor,
            'SELECT * FROM pure_sync_project_external_participant',
            key_column_name='PROJECT_ID',
        )
        related_awards = select_keyed_lists_of_dicts(
            cursor,
            'SELECT DISTINCT project_id, award_id FROM pure_sync_award',
            key_column_name='PROJECT_ID',
        )

        output_file.write('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n')
        output_file.write('<project:upmprojects xmlns:common="v3.commons.pure.atira.dk" xmlns:project="v1.upmproject.pure.atira.dk">\n')

        for project in select_list_of_dicts(cursor, 'SELECT * FROM pure_sync_project'):
            project_id = project['PROJECT_ID']
            project['internal_participants'] = internal_participants[project_id] if project_id in internal_participants else []
            project['external_participants'] = external_participants[project_id] if project_id in external_participants else []
            project['related_awards'] = related_awards[project_id] if project_id in related_awards else []
            output_file.write(template.render(project))

        output_file.write('</project:upmprojects>')

    experts_etl_logger.info('ending: edw -> pure', extra={'pure_sync_job': 'project'})

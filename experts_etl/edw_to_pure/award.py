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

template = env.get_template('award.xml.j2')
db_name = 'hotel'
# This dirname could use improvement before deploying to a remote machine:
dirname = os.path.dirname(os.path.realpath(__file__))
if 'EXPERTS_ETL_SYNC_DIR' in os.environ:
  dirname = os.environ['EXPERTS_ETL_SYNC_DIR']
output_filename = dirname + '/award_' + datetime.now().strftime('%Y-%m-%dT%H:%M:%S') + '.xml'

def run(
    db_name=db_name,
    template=template,
    output_filename=output_filename,
    experts_etl_logger=None
):
    if experts_etl_logger is None:
        experts_etl_logger = loggers.experts_etl_logger()
    experts_etl_logger.info('starting: edw -> pure', extra={'pure_sync_job': 'award'})

    with open(output_filename, 'w') as output_file, db.cx_oracle_connection() as connection:
        cursor = connection.cursor()

        # Preload these to avoid the n+1 queries problem:
        internal_holders = select_keyed_lists_of_dicts(
            cursor,
            "SELECT * FROM pure_sync_award_internal_holder",
            key_column_name='AWARD_ID',
        )
        external_holders = select_keyed_lists_of_dicts(
            cursor,
            "SELECT * FROM pure_sync_award_external_holder",
            key_column_name='AWARD_ID',
        )

        output_file.write('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n')
        output_file.write('<award:upmawards xmlns:common="v3.commons.pure.atira.dk" xmlns:award="v1.upmaward.pure.atira.dk">\n')

        for award in select_list_of_dicts(cursor, 'SELECT * FROM pure_sync_award'):
            award_id = award['AWARD_ID']
            award['internal_holders'] = internal_holders[award_id] if award_id in internal_holders else []
            award['external_holders'] = external_holders[award_id] if award_id in external_holders else []
            output_file.write(template.render(award))

        output_file.write('</award:upmawards>')

    experts_etl_logger.info('ending: edw -> pure', extra={'pure_sync_job': 'award'})

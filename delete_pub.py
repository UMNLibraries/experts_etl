import dotenv_switch.auto

from datetime import datetime
import json
import sys

from sqlalchemy import and_

from pureapi import client, response
from experts_dw import db
from experts_dw.models import PureApiExternalOrg, PureApiExternalPerson, PureApiPub, Pub
from experts_etl.extractor_loaders.pure_api_external_persons import \
    delete_db_person, \
    external_person_external_org_uuids, \
    get_db_person, \
    load_external_orgs
from experts_etl.extractor_loaders.pure_api_research_outputs import \
    delete_db_pub, \
    get_db_pub, \
    pub_external_person_uuids, \
    load_external_persons

pure_uuid = sys.argv[1]

with db.session('hotel') as session:
    db_pub = get_db_pub(session, pure_uuid)
    if db_pub:
        delete_db_pub(session, db_pub)
        session.commit()
        print(f'Research output with Pure uuid {pure_uuid} deleted')
    else:
        print(f'Research output with Pure uuid {pure_uuid} does not exist in local Experts DB')

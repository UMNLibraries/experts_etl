import dotenv_switch.auto

from datetime import datetime
import json

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

with db.session('hotel') as session:
#    external_org_uuids = set()
#    api_external_person_count = session.query(PureApiExternalPerson).count()
#    step = api_external_person_count//10
#    if step == 0:
#        step = 1
#    steps = list(range(0, api_external_person_count, step))
#    
#    row_num = 0
#    for api_external_person_row in session.query(PureApiExternalPerson).all():
#        row_num += 1
#        if row_num in steps:
#            percentage = round(row_num / api_external_person_count * 100)
#            print(datetime.now(), f': {percentage}%'), 
#         
#        api_external_person = response.transform(
#            'external-persons',
#            json.loads(api_external_person_row.json),
#            version='524'
#        )
#        #print(f'{api_external_person.uuid=}')
#        #break
#
#        try:
#            r = client.get(f'external-persons/{api_external_person.uuid}')
#        except client.PureAPIHTTPError as e:
#            if e.response.status_code == 404:
#                #print(f'{e.response.status_code=}')
#                #break
#                db_person = get_db_person(session, api_external_person.uuid)
#                if db_person:
#                    delete_db_person(session, db_person)
#                session.delete(api_external_person_row)
#            #else:
#            #    print(f'{e.response.status_code=}')
#            continue
#        except Exception:
#            raise
#
#        #print(f'{api_external_person.uuid=}')
#        #break
#
#        external_org_uuids.update(
#            external_person_external_org_uuids(api_external_person)
#        )
#
#    print(datetime.now(), f': loading external orgs...'), 
#    load_external_orgs(session, external_org_uuids)
#    print(datetime.now(), f': committing...'), 
#    session.commit()
#    print(datetime.now(), f': done'), 

    external_person_uuids = set()
    api_pub_count = session.query(PureApiPub).count()
    step = api_pub_count//10
    if step == 0:
        step = 1
    steps = list(range(0, api_pub_count, step))
    
    row_num = 0
    for api_pub_row in session.query(PureApiPub).all():
        row_num += 1
        if row_num in steps:
            percentage = round(row_num / api_pub_count * 100)
            print(datetime.now(), f': {percentage}%'), 
         
        api_pub = response.transform(
            'research-outputs',
            json.loads(api_pub_row.json),
            version='524'
        )

        try:
            r = client.get(f'research-outputs/{api_pub.uuid}')
        except client.PureAPIHTTPError as e:
            if e.response.status_code == 404:
                #print(f'{e.response.status_code=}')
                db_pub = get_db_pub(session, api_pub.uuid)
                if db_pub:
                    delete_db_pub(session, db_pub)
                session.delete(api_pub_row)
            #else:
            #    print(f'{e.response.status_code=}')
            continue
        except Exception:
            raise

        external_person_uuids.update(
            pub_external_person_uuids(api_pub)
        )

    print(datetime.now(), f': loading external persons...'), 
    load_external_persons(session, external_person_uuids)
    print(datetime.now(), f': committing...'), 
    session.commit()
    print(datetime.now(), f': done'), 

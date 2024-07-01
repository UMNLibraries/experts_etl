import pandas as pd
import re
from experts_dw.models import PureEligibleAffiliateJob, PureEligibleAffiliateJobcode, UmnDeptPureOrg
from experts_etl.demographics import latest_demographics_for_emplid
from experts_etl.umn_data_error import record_unknown_dept_errors
from sqlalchemy import and_

def extract_transform(session, emplid):
    entries = extract(session, emplid)
    return transform(session, entries)

def extract(session, emplid):
    entries = []
    for entry in session.query(PureEligibleAffiliateJob).filter(PureEligibleAffiliateJob.emplid == emplid).order_by(PureEligibleAffiliateJob.effdt):
        entries.append(
            {c.name: getattr(entry, c.name) for c in entry.__table__.columns}
        )
    return entries

"""
status_flag values:
C Current
F Future (We exclude these entries in the SQL views.)
H Historical

status values:
A Active
I Inactive
"""
active_states = ['A']

def transform(session, entries):
    if len(entries) == 0:
        return []
  
    return transform_entry_groups(
        session,
        group_entries(entries)
    )

def get_org_id(session, deptid):
    umn_dept_pure_org = (
        session.query(UmnDeptPureOrg)
        .filter(UmnDeptPureOrg.deptid == deptid)
        .one_or_none()
    )
    if umn_dept_pure_org:
        return umn_dept_pure_org.pure_org_id
    return None

def transform_entry_groups(session, entry_groups):
    if len(entry_groups) == 0:
        return []
  
    jobs = []
    for group in entry_groups:
        last_entry = group['entries'][-1]
    
        job = {
            'affiliation_id': group['um_affil_relation'], # jobcode
            'deptid': group['deptid'],
            'um_campus': last_entry['um_campus'],
            'start_date': group['start_date'],
            'job_title': last_entry['title'],
        }
    
        org_id = get_org_id(session, job['deptid'])
        if org_id is None:
            # The ps_dwhr_um_affiliates table, and therefore our views derived from it, don't provide
            # internet id, so we must make a separate query for it:
            demog = latest_demographics_for_emplid(session, last_entry['emplid'])
            internet_id = demog.internet_id if demog else None
    
            record_unknown_dept_errors(
                session=session,
                emplid=last_entry['emplid'],
                internet_id=internet_id,
                jobcode=group['um_affil_relation'],
                jobcode_descr=last_entry['title'],
                deptid=group['deptid'],
                deptid_descr=last_entry['deptid_descr'],
                um_college=last_entry['um_college'],
                um_college_descr=last_entry['um_college_descr'],
                um_campus=last_entry['um_campus'],
                um_campus_descr=last_entry['um_campus_descr'],
            )
            continue
        job['org_id'] = org_id
    
        if last_entry['status'] not in active_states or last_entry['status_flg'] == 'H':
            job['end_date'] = last_entry['effdt']
        else:
            job['end_date'] = None
    
        jobcode_defaults = session.query(PureEligibleAffiliateJobcode).filter(
            PureEligibleAffiliateJobcode.jobcode == group['um_affil_relation']
        ).one()
        job['job_description'] = jobcode_defaults.pure_job_description
        job['employment_type'] = jobcode_defaults.default_employed_as
        job['visibility'] = jobcode_defaults.default_visibility
        job['profiled'] = jobcode_defaults.default_profiled
        job['staff_type'] = jobcode_defaults.default_staff_type
    
        jobs.append(job)
  
    return jobs

def split_entries_into_stints(entries):
    '''Because affiliate jobs have so little metadata, we have to do some extra
    work to find cases where a person has started and left the same job
    multiple times.
    '''
    if len(entries) == 0:
        return []
  
    stints = []
    current_stint = []
    current_stint_ending = False
  
    for entry in entries:
        if current_stint_ending:
            if entry['status'] in active_states:
                # We've passed the end of the current stint, and this is a new stint in the same position.
                stints.append(current_stint)
                current_stint = []
                current_stint_ending = False
            current_stint.append(entry)
            if entry['status_flg'] == 'C': # C is current.
                # Sometimes there are historical (H) entries with effdt's later than a C entry. Ignore them.
                break
            else:
                continue
    
        if entry['status'] not in active_states:
            # This is the first entry with an inactive state for this stint, so it's ending.
            # Other entries with inactive states may follow.
            current_stint_ending = True
        current_stint.append(entry)
        if entry['status_flg'] == 'C': # C is current.
            # Sometimes there are historical (H) entries with effdt's later than a C entry. Ignore them.
            break
        else:
            continue
  
    if len(current_stint) > 0:
        stints.append(current_stint)
  
    return stints

def group_entries_by_dept_affiliate_id_and_jobcode(entries):
    return [
        {
            'deptid': key[0],
            'um_affiliate_id': key[1],
            'um_affil_relation': key[2],
            'entries': df_to_dicts(rows),
        }
        for key, rows in pd.DataFrame(data=entries).groupby([
            'deptid',
            'um_affiliate_id',
            'um_affil_relation',
        ])
    ]

def group_entries(entries):
    if len(entries) == 0:
        return []
  
    return [
        {
            'deptid': group['deptid'],
            'um_affil_relation': group['um_affil_relation'],
            'start_date': stint[0]['effdt'],
            'entries': stint,
        }
        for group in group_entries_by_dept_affiliate_id_and_jobcode(entries)
        for stint in split_entries_into_stints(group['entries'])
    ]

def df_to_dicts(df):
    def transform_nulls_and_dates(d):
        for k,v in d.items():
            if pd.isnull(v):
                # Convert all Pandas NaN's, NaT's etc to None:
                d[k] = None
            if k == 'effdt' and d[k] is not None:
                d[k] = d[k].to_pydatetime()
        return d

    return [
        transform_nulls_and_dates(d)
        for d in df.to_dict('records')
    ]

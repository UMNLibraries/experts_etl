from copy import deepcopy

from .employee_jobs import jobs as employee_jobs

jobs = employee_jobs

person_id = '6030'
primary_empl_rcdno = '0'

jobs_with_primary = [
    {**job, **{'primary': False}}
    for job in jobs[:-1]
]
jobs_with_primary.append({**jobs[-1], **{'primary': True}})

# Set according to the primary job:
transformed_profiled = False

# None of this person's jobs qualify to be 'academic', so
# all jobs should have the default of 'nonacademic', and
# should be unchanged from jobs_with_primary:
jobs_with_transformed_staff_type = deepcopy(jobs_with_primary)

jobs_with_staff_org_assoc_id = [
    {**jobs_with_transformed_staff_type[3], **{'staff_org_assoc_id': 'autoid:6030-PIXEZPPAPIRGQ-Administrative Manager 2-exec_admin_manage-2017-07-10'}},
    {**jobs_with_transformed_staff_type[1], **{'staff_org_assoc_id': 'autoid:6030-PIXEZPPAPIRGQ-Senior Research Fellow-researcher-2000-11-22'}},
    {**jobs_with_transformed_staff_type[2], **{'staff_org_assoc_id': 'autoid:6030-PIXEZPPAPIRGQ-Researcher 7-researcher-2015-09-07',}}
]

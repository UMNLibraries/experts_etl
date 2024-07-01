from copy import deepcopy

from .employee_jobs import jobs as employee_jobs
from .poi_jobs import jobs as poi_jobs

jobs = employee_jobs + poi_jobs

primary_empl_rcdno = '0'

# The employee job should be given preference when setting a primary job,
# and this person has only one employee job and one poi job, 
# both former jobs, with end dates:
jobs_with_primary = [
    {**employee_jobs[0], **{'primary': True}},
    {**poi_jobs[0], **{'primary': False}},
]

# Set according to the primary job:
transformed_profiled = False

# None of this person's jobs qualify to be 'academic', so
# all jobs should have the default of 'nonacademic', and 
# should be unchanged from jobs_with_primary:
jobs_with_transformed_staff_type = deepcopy(jobs_with_primary)

# The first job should be the employee job, as defined above:
jobs_with_staff_org_assoc_id = [
  {**jobs_with_transformed_staff_type[0], **{'staff_org_assoc_id': 'autoid:7462-DOLQQNOVD-Medical Fellow-pit-2014-08-28'}},
  {**jobs_with_transformed_staff_type[1], **{'staff_org_assoc_id': 'autoid:7462-ZWLCEAOWFEA-Post-Doctoral Fellow-pit-2015-09-07'}},
]

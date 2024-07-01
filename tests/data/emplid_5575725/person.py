from copy import deepcopy

from .poi_jobs import jobs as poi_jobs

# This person has only poi jobs
jobs = poi_jobs

primary_empl_rcdno = '1'

# Only one job for this person, so it must be primary:
jobs_with_primary = [
    {**jobs[0], **{'primary': True}}
]

## Set according to the primary job:
transformed_profiled = False

# None of this person's jobs qualify to be 'academic', so
# all jobs should have the default of 'nonacademic', and
# should be unchanged from jobs_with_primary:
jobs_with_transformed_staff_type = deepcopy(jobs_with_primary)

jobs_with_staff_org_assoc_id = [
    {**jobs_with_transformed_staff_type[0], **{'staff_org_assoc_id': 'autoid:5575725-IHRBIHRB-Graduate School Fellow-student-2019-08-26'}},
]

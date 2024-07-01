from copy import deepcopy

from .employee_jobs import jobs as employee_jobs

jobs = employee_jobs

primary_empl_rcdno = '0'

jobs_with_primary = [
    # This person has only one job, so it must be primary:
    {**jobs[0], **{'primary': True}}
]

# Set according to the primary job:
transformed_profiled = False

# None of this person's jobs qualify to be 'academic', so
# all jobs should have the default of 'nonacademic', and
# should be unchanged from jobs_with_primary:
jobs_with_transformed_staff_type = deepcopy(jobs_with_primary)

jobs_with_staff_org_assoc_id = [
    {**jobs_with_transformed_staff_type[0], **{'staff_org_assoc_id': 'autoid:8000397-AOVMGGA-Assistant Professor-faculty-2015-09-01'}}
]

from datetime import datetime

from .employee_jobs import jobs as employee_jobs

jobs = employee_jobs

primary_empl_rcdno = '0'

jobs_with_primary = [
    {**jobs[0], **{'primary': False}},
    {**jobs[1], **{'primary': True}},
]

# Set according to the primary job:
transformed_profiled = True

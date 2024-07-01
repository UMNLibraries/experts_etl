from datetime import datetime

from .employee_jobs import jobs as employee_jobs
from .affiliate_jobs import jobs as affiliate_jobs

jobs = employee_jobs + affiliate_jobs

primary_empl_rcdno = '0'

jobs_with_primary = [
    # This person has only one current job (without an end date) so it must be primary:
    {**job, **{'primary': False}} if job['end_date'] else {**job, **{'primary': True}}
    for job in jobs
]

# Primary Job Cases

In all cases below, all jobs are Pure-eligible jobs

* No jobs: return an empty list
* Only one job: must be primary
* Only one active job: must be primary
* Multiple, active, non-affiliate jobs:
  * If one of those jobs matches the primary employee record number, set that job as primary
  * Else set as primary the first in the list
* No active non-affiliate jobs, but active affiliate jobs exist: set as primary the first in the list
* No active jobs: Doesn't matter which is primary, because the profile will be back-end only.
  Set as primary the first job in the list of those with the latest end date

## Questions

* Should we take PRS-eligible jobs into account, choosing one of them, if present, to be primary?

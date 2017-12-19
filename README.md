# Experts@Minnesota ETL

Moves data from UMN to Pure (Experts@Minnesota), and vice versa.

## High-level Data Flow

* Create "all jobs new", by subtracting the "all jobs previous" snapshot from "all jobs current".
  * Create "all jobs deferred", by subtracting from "all jobs new" any jobs where "dept id" exists in "pure internal orgs", i.e., the org already exists in Pure. (The org must exist before we can add the job to Pure.)
    * Set "defer" to "Y", and "defer reason" to "Org with $deptid does not yet exist in Pure" in each corresponding "all jobs new" record.
    * Add all emplids to "person changes deferred".
    * Add all deptids to "orgs to be added to Pure".
  * For all other "all jobs new", set "defer" to "N".
* Create "emplids new", by subtracting the distinct emplids in the "all jobs previous" snapshot from the distinct emplids in "all jobs current".
  * For "all jobs new" where emplid is in "emplids new", set "person change description" to "add".
  * For all other "all jobs new", set "person change description" to "update".
* Create "emplids removed", by subtracting the distinct emplids from "all jobs new" from the distinct emplids in the "all jobs previous" snapshot.
  * For "all jobs previous" where emplid is in "emplids removed", add a record to "all jobs new", with a "person change description" of "remove".
  * Not sure "remove" is the best language here, because we should never remove persons from Pure. The most we will do is discontinue refining their profiles.
* Add "all jobs new" to the "all jobs history" table, along with a timestamp.
* Add "all jobs new" emplids to "persons needing new Pure records", _except_ for those emplids in "person changes deferred".
* Add "emplids new" to a list of "all emplids", along with a timestamp.
* Create "demographics new", by subtracting the "demographics previous" snapshot from "demographics current", which queries demographics for "all emplids".
  * For all emplids in "person changes deferred", set "defer" to "Y", and "defer reason" to "Org with $deptid does not yet exist in Pure". For all other "demographics new", set "defer" to "N".
  * For all emplids in the "demographics history" table, set "change description" to "update". For all other emplids, set "change description" to "add".
* Add "demographics new" to the "demographics history" table, along with a timestamp.
* Add "demographics new" emplids to "persons needing new Pure records", if they're not already there, _and_ if they're not in "person changes deferred".
* For all emplids in "persons needing new Pure records", generate new Pure records, using data from "all jobs current" and "demographics current".

## Possibly-problematic Situations

Quotations below from section "4.3.2: Configuration area fields and buttons", in [Populating Pure in Bulk](https://experts.umn.edu/admin/services/import/documentation.pdf).
 
* How to handle organisations, like centers and institutes, that have no UMN deptid, and exist only in Pure?
  * This is one reason we decided to maintain organisations in Pure, manually. Affiliations of persons to such organisations must also be maintained manually in Pure. 
* Related: how to handle people who have jobs with those organisations, which also exist only in Pure? (Meaning neither of these exist in PS.)
  * These people will always also have at least one job that is associated with a PS deptid. The PS job info we can automatically upload, but the other jobs will exist only in Pure, and must be maintained manually. So there are couple of things we need to do:
    * We must set "PERSON_ORG_RELATION" > "Lock relation list" to "Do not lock", "to allow both synchronizations with an external data source and users working in the Pure interface to add relations from records synchronized by this job, so that the record in Pure may have additional changes that are not reflected in the external data source."
    * A person's primary affiliation will always be with an org with a PS deptid, and we do not allow people to change that in Pure. So we set "PRIMARY_ASSOCIATION" to "Sync", "to update the field in Pure to the value from the external data source each time the job is run. Fields populated in this manner cannot be edited in the Pure interface."
* Will any of the syncing options for Pure uploads overwrite that data that we created in Pure, if we're not careful?
  * For data about which we're concerned, we should use the "Sync once" option, "to set the field in Pure to the value from the external data source the first time this field has content, but do not change the field's value on consequent runs. Once the field is populated it is maintained in the Pure interface, so it is no longer updated from the external data source."
* Also, how do we handle cases where a person leaves UMN, but had a job at one of these organisations? Can we automatically alert center/insitute administrators about the situation?
  * Any such jobs that exist only in Pure will require manual updates. We may be able to detect when associated persons leave UMN, though, because they will no longer have any affiliations with orgs that have PS deptid's. Maybe we can check Pure for these Pure-only jobs when people leave UMN, and create notifications of the need for manually updates. While these jobs remained unmodified in Pure, it will appear in Pure as if they are still with UMN.


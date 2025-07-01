* Student types classification scheme
  * /dk/atira/pure/person/studenttype/{degree ID}
  * label is degree description
  * Jan suggests adding student types the same way we add deptid's/organisations. But since these will
    always be based on the degree ID, maybe we can add these automatically? No; the Pure API does not
    currently support modifying classification schemes, and there's no planned date for it. We'll have to
    generate tickets and wait for these to be added manually.
  * This classification scheme is used for the "Degree description" dropdown in the Pure core (admin) UI.
    Jan changed the name of this field in the UI; it was originally labeled "Student type".
* example students
  * 4401577: Elena Marie Fransen
  * 5288744: Alexander Lucas Guzman
  * Are these persons still in staging? Or did Jan remove them? Either she removed them, or Pure removed them,
    likely because they were missing from the automated sync, because I can't find them.
* Minor point about private data: email address _may_ be private, if students choose to suppress display of it.
* org_id is already a string in Experts DW. Treating it as a number happened only in the spreadsheet.
* We actually can add student organisations (academic programs) to Pure automatically, _unless_ the parent deptid
  does not already exist in Pure. In that case, we would use a similar process to what we already have to add the missing deptid.
* Use ps_descr, but not prog_status. To that end, Jan has renamed the "Student ID" field in the Pure core UI to "Program Status".
  Similarly, Jan has previously(?) changed the field under "Status" in staff organisations to be "PeopleSoft Job Code".
  * Was that heading always called "Status"? If so, it seems odd that Pure would put affiliation_id under that.
* affiliation_id and student_type_description are Pure field names. We use those in Experts DW to be as clear as possible
  about how the data in the final tables maps to the Pure fields.
  * The column names in this spreadsheet are a mix of OIT Data Warehouse column names and Pure field names.
  * We use affiliation_id for staff organisations (departments, etc), so it seemed nicely parallel to also use it
    for student orgs. Is affiliation_id a required field in Pure? No.
* TODO: Create a table that maps all the column/field names from various sources to each other.
* Has John Barneson taken the FERPA training? Yes.
* We will _not_ designate a primary student org (academic program). It's not important to Jan, we rarely have students
  in more than one program, and this was super complicated for employees.


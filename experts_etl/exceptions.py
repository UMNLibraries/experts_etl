import hashlib
import json

class ExpertsEtlError(Exception):
    pass

class ExpertsEtlUmnDataError(ExpertsEtlError):
    ids = []
    attrs = [
      'emplid',
      'internet_id',
      'jobcode',
      'jobcode_descr',
      'deptid',
      'deptid_descr',
      'persons_in_dept',
      'um_college',
      'um_college_descr',
      'um_campus',
      'um_campus_descr',
    ]
    def __init__(self, message, **kwargs):
        self.id = self.generate_id(**kwargs)
        for attr in self.attrs:
            self.__dict__[attr] = kwargs[attr] if attr in kwargs else None
        self.message = message
        super().__init__(
            self.message
        )

    def generate_id(self, **kwargs):
        ids = {k: kwargs[k] for k in filter(lambda k: k in self.ids, kwargs.keys())}
        ids['exception'] = type(self).__name__
        return hashlib.sha1(
            json.dumps(sorted(ids.items())).encode()
        ).hexdigest()

class ExpertsEtlUnknownDept(ExpertsEtlUmnDataError):
    ids = ['deptid']
    def __init__(
        self,
        *,
        deptid,
        deptid_descr=None,
        persons_in_dept=None,
        um_college=None,
        um_college_descr=None,
        um_campus=None,
        um_campus_descr=None,
    ):
        super().__init__(
            'unknown department',
            **{
                'deptid': deptid,
                'deptid_descr': deptid_descr,
                'persons_in_dept': persons_in_dept,
                'um_college': um_college,
                'um_college_descr': um_college_descr,
                'um_campus': um_campus,
                'um_campus_descr': um_campus_descr,
            }
        )

class ExpertsEtlJobWithUnknownDept(ExpertsEtlUmnDataError):
    ids = ['jobcode','deptid','emplid']
    def __init__(
        self,
        *,
        emplid,
        internet_id,
        jobcode,
        jobcode_descr=None,
        deptid,
        deptid_descr=None,
        um_college=None,
        um_college_descr=None,
        um_campus=None,
        um_campus_descr=None,
    ):
        super().__init__(
            'job with unknown department',
            **{
                'emplid':emplid,
                'internet_id':internet_id,
                'jobcode':jobcode,
                'jobcode_descr': jobcode_descr,
                'deptid':deptid,
                'deptid_descr': deptid_descr,
                'um_college': um_college,
                'um_college_descr': um_college_descr,
                'um_campus': um_campus,
                'um_campus_descr': um_campus_descr,
            }
        )

class ExpertsEtlUnknownJobcodeDeptid(ExpertsEtlUmnDataError):
    ids = ['jobcode','deptid']
    def __init__(
        self,
        *,
        jobcode,
        jobcode_descr=None,
        deptid,
        deptid_descr=None,
        um_college=None,
        um_college_descr=None,
        um_campus=None,
        um_campus_descr=None,
    ):
        super().__init__(
            'unknown overrideable jobcode/deptid pair',
            **{
                'jobcode': jobcode,
                'jobcode_descr': jobcode_descr,
                'deptid':deptid,
                'deptid_descr': deptid_descr,
                'um_college': um_college,
                'um_college_descr': um_college_descr,
                'um_campus': um_campus,
                'um_campus_descr': um_campus_descr,
            }
        )

class ExpertsEtlJobWithUnknownJobcodeDeptid(ExpertsEtlUmnDataError):
    ids = ['jobcode','deptid','emplid']
    def __init__(
        self,
        *,
        emplid,
        internet_id,
        jobcode,
        jobcode_descr=None,
        deptid,
        deptid_descr=None,
        um_college=None,
        um_college_descr=None,
        um_campus=None,
        um_campus_descr=None,
    ):
        super().__init__(
            'job with unknown overrideable jobcode/deptid pair',
            **{
                'emplid':emplid,
                'internet_id':internet_id,
                'jobcode':jobcode,
                'jobcode_descr': jobcode_descr,
                'deptid':deptid,
                'deptid_descr': deptid_descr,
                'um_college': um_college,
                'um_college_descr': um_college_descr,
                'um_campus': um_campus,
                'um_campus_descr': um_campus_descr,
            }
        )

class ExpertsEtlPersonNoJobData(ExpertsEtlUmnDataError):
    ids = ['emplid']
    def __init__(self, *, emplid, internet_id):
        super().__init__('unable to find or generate any job data for person', **{'emplid':emplid, 'internet_id':internet_id})

class ExpertsEtlPersonNoOrgAssociations(ExpertsEtlUmnDataError):
    '''This should never happen, yet it has happened at least once. We use this
    exception to prevent submitting person records to Pure XML bulk loading,
    because it will cause a fatal error that kills the entire process.'''
    ids = ['emplid']
    def __init__(self, *, emplid, internet_id):
        super().__init__('person has no organisation associations', **{'emplid':emplid, 'internet_id':internet_id})

# The rest of these should never happen, because the associated columns are not nullable in EDW.
# Will uncomment if we happen to need them, for some strange reason.
#
#class ExpertsEtlUnknownJobcode(ExpertsEtlUmnDataError):
#    def __init__(self, jobcode):
#        super().__init__('unknown jobcode', **{'jobcode': jobcode})
#
### These next three are required to generate a unique staff organisation association ID:
#
#class ExpertsEtlDeptNoOrg(ExpertsEtlUmnDataError):
#    def __init__(self, deptid):
#        super().__init__('department has no Pure organisation ID', **{'deptid':deptid})
#
#class ExpertsEtlJobcodeNoDescr(ExpertsEtlUmnDataError):
#    def __init__(self, jobcode):
#        super().__init__('jobcode has no jobcode_descr', **{'jobcode': jobcode})
#
#class ExpertsEtlJobcodeNoEmployedAs(ExpertsEtlUmnDataError):
#    def __init__(self, jobcode):
#        super().__init__('jobcode has no default_employed_as', **{'jobcode': jobcode})
#
###
#
#class ExpertsEtlJobcodeNoPureDescr(ExpertsEtlUmnDataError):
#    def __init__(self, jobcode):
#        super().__init__('jobcode has no pure_job_description', **{'jobcode': jobcode})
#
#class ExpertsEtlJobcodeNoStaffType(ExpertsEtlUmnDataError):
#    def __init__(self, jobcode):
#        super().__init__('jobcode has no default_staff_type', **{'jobcode': jobcode})
#
#class ExpertsEtlJobcodeNoVisibility(ExpertsEtlUmnDataError):
#    def __init__(self, jobcode):
#        super().__init__('jobcode has no default_visibility', **{'jobcode': jobcode})
#
#class ExpertsEtlJobcodeNoProfiled(ExpertsEtlUmnDataError):
#    def __init__(self, jobcode):
#        super().__init__('jobcode has no default_profiled', **{'jobcode': jobcode})

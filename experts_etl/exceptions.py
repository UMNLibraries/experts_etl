import hashlib
import json

class ExpertsEtlError(Exception):
    pass

class ExpertsEtlUmnDataError(ExpertsEtlError):
    def __init__(self, message, **kwargs):
        self.id = self.generate_id(**kwargs)
        for attr in ['jobcode','deptid','emplid']:
          if attr in kwargs:
            self.__dict__[attr] = kwargs[attr]
        self.message = message
        super().__init__(
            self.message
        )

    def generate_id(self, **kwargs):
        ids = {'exception': type(self).__name__, **kwargs}
        return hashlib.sha1(
            json.dumps(sorted(ids.items())).encode()
        ).hexdigest()

class ExpertsEtlUnknownDept(ExpertsEtlUmnDataError):
    def __init__(self, deptid):
        super().__init__('unknown department', **{'deptid':deptid})

class ExpertsEtlJobWithUnknownDept(ExpertsEtlUmnDataError):
    def __init__(self, jobcode, deptid, emplid):
        super().__init__('job with unknown department', **{'jobcode':jobcode, 'deptid':deptid, 'emplid':emplid})

class ExpertsEtlUnknownJobcodeDeptid(ExpertsEtlUmnDataError):
    def __init__(self, jobcode, deptid):
        super().__init__('unknown overrideable jobcode/deptid pair', **{'jobcode': jobcode, 'deptid':deptid})

class ExpertsEtlJobWithUnknownJobcodeDeptid(ExpertsEtlUmnDataError):
    def __init__(self, jobcode, deptid, emplid):
        super().__init__('job with unknown overrideable jobcode/deptid pair', **{'jobcode':jobcode, 'deptid':deptid, 'emplid':emplid})

class ExpertsEtlPersonNoJobData(ExpertsEtlUmnDataError):
    def __init__(self, emplid):
        super().__init__('unable to find or generate any job data for person', **{'emplid':emplid})

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

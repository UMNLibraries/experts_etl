from experts_dw.models import PureEligibleGraduateProgram

# Later, we should be able to elminiate the use of sqlalchemy here.
def extract_transform(session, emplid):
    programs = []
    for program in session.query(PureEligibleGraduateProgram).filter(PureEligibleGraduateProgram.person_id == emplid):
        programs.append(
            {c.name: getattr(program, c.name) for c in program.__table__.columns}
        )
    return programs

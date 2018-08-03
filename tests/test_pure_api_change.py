import datetime, pytest
from experts_dw import db
from experts_etl import pure_api_change

@pytest.fixture
def session():
  with db.session('hotel') as session:
    yield session

def test_ensure_valid_startdate(session):
  startdate_from_none = pure_api_change.ensure_valid_startdate(session)
  assert isinstance(startdate_from_none, datetime.datetime)

  now = datetime.datetime.utcnow()
  startdate_from_startdate = pure_api_change.ensure_valid_startdate(session, now)
  assert isinstance(startdate_from_startdate, datetime.datetime)
  assert startdate_from_startdate == now

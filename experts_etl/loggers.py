import json
import logging
from logging.handlers import TimedRotatingFileHandler
import os
import shutil
import gzip

dirname = os.path.dirname(os.path.realpath(__file__ + '/..'))

class PureApiRecordFormatter(logging.Formatter):
  def format(self, record):
    if isinstance(record.msg, str):
      # Ensure we get a single-line record by loading and then dumping:
      return json.dumps(json.loads(record.msg))
    elif isinstance(record.msg, dict):
      return json.dumps(record.msg)

def namer(name):
  return name + '.gz'

def rotator(source, dest):
  with open(source, 'rb') as sf:
    with gzip.open(dest, 'wb') as df:
      shutil.copyfileobj(sf, df)
  os.remove(source)

def serialize_pure_api_record(record):
  if isinstance(record, str):
    # Ensure we get a single-line record by loading and then dumping:
    return json.dumps(json.loads(record))
  elif isinstance(record, dict):
    return json.dumps(record)

def pure_api_record_logger(name='pure_api_record', dirname=dirname):
  path = dirname + '/' + name + '.log'
  logger = logging.getLogger('experts_etl.' + name)
  logger.setLevel(logging.INFO)
   
  handler = TimedRotatingFileHandler(
    path,
    when='d',
    interval=1,
    backupCount=365
  )
  handler.setFormatter(PureApiRecordFormatter())
  handler.rotator = rotator
  handler.namer = namer
  logger.addHandler(handler)

  return logger

def experts_etl_logger(name='experts_etl', dirname=dirname):
  path = dirname + '/' + name + '.log'
  logger = logging.getLogger('experts_etl.' + name)
  logger.setLevel(logging.INFO)
   
  handler = TimedRotatingFileHandler(
    path,
    when='d',
    interval=1,
    backupCount=365
  )
  handler.rotator = rotator
  handler.namer = namer
   

  logger.addHandler(handler)

  return logger

def rollover(logger):
  for handler in logger.handlers:
    if isinstance(handler, TimedRotatingFileHandler):
      handler.doRollover()
      break

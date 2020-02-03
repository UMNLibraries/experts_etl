from datetime import datetime
import json
import logging
from logging.handlers import TimedRotatingFileHandler
import os
import shutil
import gzip
import traceback

from pythonjsonlogger import jsonlogger

# defaults

dirname = os.path.dirname(os.path.realpath(__file__ + '/..'))
if 'EXPERTS_ETL_LOG_DIR' in os.environ:
  dirname = os.environ['EXPERTS_ETL_LOG_DIR']

# formatters

class PureApiRecordFormatter(logging.Formatter):
  def format(self, record):
    if isinstance(record.msg, str):
      # Ensure we get a single-line record by loading and then dumping:
      return json.dumps(json.loads(record.msg))
    elif isinstance(record.msg, dict):
      return json.dumps(record.msg)

class ExpertsEtlFormatter(jsonlogger.JsonFormatter):
  def add_fields(self, log_record, record, message_dict):
    super(ExpertsEtlFormatter, self).add_fields(log_record, record, message_dict)
    if not log_record.get('timestamp'):
      # This doesn't use record.created, so it will be slightly off.
      log_record['timestamp'] = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ')

def namer(name):
  return name + '.gz'

def rotator(source, dest):
  with open(source, 'rb') as sf:
    with gzip.open(dest, 'wb') as df:
      shutil.copyfileobj(sf, df)
  os.remove(source)

def pure_api_record_logger(name='pure_api_record', dirname=dirname, type='pure-api-record-type'):
  path = dirname + '/' + type + '.log'
  logger = logging.getLogger('experts_etl.' + name)
  logger.setLevel(logging.INFO)

  handler = TimedRotatingFileHandler(
    path,
    when='S',
    interval=86400, # seconds/day
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
    when='S',
    interval=86400, # seconds/day
    backupCount=365
  )
  handler.setFormatter(ExpertsEtlFormatter(
    '(timestamp) (levelname) (name) (message) (pathname) (funcName) (lineno)'
  ))
  handler.rotator = rotator
  handler.namer = namer

  logger.addHandler(handler)

  return logger

def rollover(logger):
  for handler in logger.handlers:
    if isinstance(handler, TimedRotatingFileHandler):
      handler.doRollover()
      break

def format_exception(e):
    # Inspired by: https://realpython.com/the-most-diabolical-python-antipattern/
    tb_lines = traceback.format_exception(e.__class__, e, e.__traceback__)
    return ''.join(tb_lines)

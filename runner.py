from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())
import importlib
import multiprocessing as mp
import os
import time
from experts_etl import sync_file_rotator
from experts_etl import loggers
experts_etl_logger = loggers.experts_etl_logger()

default_interval = 14400 # 4 hours, in seconds

extractor_loaders = list(map(
  lambda x: 'experts_etl.extractor_loaders.' + x, 
  [
    'pure_api_changes',
    'pure_api_external_organisations',
    'pure_api_organisational_units',
    'pure_api_external_persons',
    'pure_api_persons',
    'pure_api_research_outputs',
  ]
))

transformer_loaders = list(map(
  lambda x: 'experts_etl.transformer_loaders.' + x, 
  [
    'pure_api_external_org',
    'pure_api_internal_org',
    'pure_api_external_person',
    'pure_api_internal_person',
    'pure_api_pub',
  ]
))

syncers = list(map(
  lambda x: 'experts_etl.' + x, 
  [
    'oit_to_edw.person',
    'edw_to_pure.person',
    'edw_to_pure.user',
    'umn_data_error',
    'sync_file_rotator',
  ]
))

def subprocess(module_name):
  module = importlib.import_module(module_name)

  experts_etl_logger.info(
    'starting: ' + module_name,
    extra={
      'pid': str(os.getpid()),
      'ppid': str(os.getppid()),
    }
  )

  try:
    module.run(
      experts_etl_logger=experts_etl_logger
    )
  except Exception as e:
    experts_etl_logger.error(
      'attempt to execute ' + module_name + '.run() failed: {}'.format(e),
      extra={
        'pid': str(os.getpid()),
        'ppid': str(os.getppid()),
      }
    )

  experts_etl_logger.info(
    'ending: ' + module_name,
    extra={
      'pid': str(os.getpid()),
      'ppid': str(os.getppid()),
    }
  )

def run():
  experts_etl_logger.info(
    'starting: experts etl',
    extra={
      'pid': str(os.getpid()),
      'ppid': str(os.getppid()),
    }
  )

  for module_name in extractor_loaders + transformer_loaders + syncers: 
    try:
      # Must include a comma after a single arg, or multiprocessing seems to
      # get confused and think we're passing multiple arguments:
      p = mp.Process(target=subprocess, args=(module_name,))
      p.start()
      p.join()
    except Exception as e:
      experts_etl_logger.error(
        'attempt to execute ' + module_name + 'in a new process failed: {}'.format(e),
        extra={
          'pid': str(os.getpid()),
          'ppid': str(os.getppid()),
        }
      )

  experts_etl_logger.info(
    'ending: experts etl',
    extra={
      'pid': str(os.getpid()),
      'ppid': str(os.getppid()),
    }
  )
  loggers.rollover(experts_etl_logger)

def daemon(interval=default_interval):
  while True:
    run()
    time.sleep(interval)

if __name__ == '__main__':
  daemon()

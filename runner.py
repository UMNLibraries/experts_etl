from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())
import importlib
import multiprocessing as mp
from experts_etl import loggers
experts_etl_logger = loggers.experts_etl_logger()

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

def subprocess(module_name):
  module = importlib.import_module(module_name)
  module.run(
    experts_etl_logger=experts_etl_logger
  )

def run():
  experts_etl_logger.info('starting: experts etl')

  for module_name in extractor_loaders + transformer_loaders:
    # Must include a comma after a single arg, or multiprocessing seems to
    # get confused and think we're passing multiple arguments:
    p = mp.Process(target=subprocess, args=(module_name,))
    p.start()
    p.join()

  experts_etl_logger.info('ending: experts etl')
  loggers.rollover(experts_etl_logger)

if __name__ == '__main__':
  run()

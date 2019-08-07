from glob import glob
import os
from experts_etl import loggers

sync_dir = os.environ['EXPERTS_ETL_SYNC_DIR']
keep_limit = int(os.environ['EXPERTS_ETL_SYNC_KEEP_LIMIT'])
sync_types = [
    'person',
    'user',
]

def run(
    experts_etl_logger=None,
    sync_dir=sync_dir,
    sync_types=sync_types,
    keep_limit=keep_limit
):
    if experts_etl_logger is None:
        experts_etl_logger = loggers.experts_etl_logger()
    experts_etl_logger.info('starting: sync file rotation', extra={'pure_sync_job': 'sync_file_rotator'})

    deleted_filenames = []
    for sync_type in sync_types:
        deleted_filenames.extend(
            rotate(sync_type, sync_dir=sync_dir, keep_limit=keep_limit)
        )

    experts_etl_logger.info('ending: sync file rotation', extra={'pure_sync_job': 'sync_file_rotator'})

    return deleted_filenames

def rotate(sync_type, sync_dir=sync_dir, keep_limit=keep_limit):
    # list of sync files in descending mtime order:
    filenames = sorted(
        glob(f'{sync_dir}/{sync_type}*.xml'),
        reverse=True,
        key=lambda f: os.stat(f).st_mtime
    )

    deleted_filenames = []
    if len(filenames) == 0:
        return deleted_filenames

    latest_linkname = f'{sync_dir}/latest_{sync_type}.xml'
    if os.path.exists(latest_linkname):
        os.remove(latest_linkname)
    os.symlink(filenames[0], latest_linkname)

    if len(filenames) > keep_limit:
        for filename in filenames[keep_limit:]:
            os.remove(filename)
            deleted_filenames.append(filename)

    return deleted_filenames

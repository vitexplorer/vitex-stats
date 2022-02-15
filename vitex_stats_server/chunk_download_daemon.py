import logging
from datetime import datetime

import daemon

from flask import current_app as app
from vitex_stats_server.ledger.data_accessor import gvite_get_chunks, gvite_get_snapshot_chain_height, save_account_block_from_dict, save_snapshot_block_dict
from .models import ConfigStatus, db

ERR_NO_RESULT = -1
SLICE_SIZE = 5
PROGRESS_KEY = 'current_chunk_height'


def chunk_download_daemon_main(target_date):
    with daemon.DaemonContext():
        logging.basicConfig(level=logging.INFO,
                            format='%(asctime)s %(levelname)-8s %(message)s',
                            filename='/tmp/vitex_sync_daemon.log')
        logging.info(f'Chunk downloader started, target date {target_date}')
        sync_loop(target_date)


def sync_loop(target_date):
    if app.config['LOGLEVEL'] == 'WARNING':
        logging.getLogger().setLevel(logging.WARNING)
    elif app.config['LOGLEVEL'] == 'ERROR':
        logging.getLogger().setLevel(logging.ERROR)
    start_height, end_height = get_initial_height()
    timestamp = int(datetime.now().timestamp())
    target_timestamp = int(target_date.timestamp())
    while timestamp > target_timestamp:
        chunks = gvite_get_chunks(start_height, end_height)
        for chunk in chunks:
            snapshot_block = chunk.get('SnapshotBlock')
            snapshot_timestamp = snapshot_block.get("timestamp", 0)
            if snapshot_block is None:
                logging.error(
                    f'Snapshot block is empty in chunk {start_height} - {end_height} ')
            else:
                save_snapshot_block_dict(snapshot_block)

            if snapshot_timestamp > 0:
                timestamp = snapshot_timestamp

            account_blocks = chunk.get('AccountBlocks', None)
            if account_blocks is None:
                continue
            for account_block in account_blocks:
                save_account_block_from_dict(account_block, timestamp)

        chunk_date = datetime.fromtimestamp(timestamp)
        logging.info(
            f'downloaded chunks {start_height} - {end_height}, chunk date {chunk_date}')
        start_height -= SLICE_SIZE
        end_height -= SLICE_SIZE
        save_progress(end_height)


def get_initial_height():
    conf_stat = db.session.get(ConfigStatus, PROGRESS_KEY)
    if conf_stat is None:
        current_height = gvite_get_snapshot_chain_height()
        logging.info(f'Initial height {current_height}')
    else:
        current_height = int(conf_stat.value)
        logging.info(f'Resume download height {current_height}')
    return current_height - SLICE_SIZE, current_height


def save_progress(chunk_height):
    current_height_obj = db.session.get(ConfigStatus, PROGRESS_KEY)
    if current_height_obj is None:
        current_height_obj = ConfigStatus(
            key=PROGRESS_KEY, value=str(chunk_height))
        db.session.add(current_height_obj)
    else:
        current_height_obj.value = str(chunk_height)
    try:
        db.session.commit()
    except Exception as e:
        logging.error(f'Error saving progress {e}')
        db.session.rollback()


def reset_progress():
    current_height_obj = db.session.get(ConfigStatus, PROGRESS_KEY)
    if current_height_obj is None:
        return
    else:
        db.session.delete(current_height_obj)
    try:
        db.session.commit()
    except Exception as e:
        logging.error(f'Error reseting progress {e}')
        db.session.rollback()


def download_chunk_interval(start_height, end_height):
    slice_start_height = start_height - SLICE_SIZE
    slice_end_height = slice_start_height + SLICE_SIZE
    timestamp = 0
    while slice_start_height <= end_height:
        chunks = gvite_get_chunks(slice_start_height, slice_end_height)
        for chunk in chunks:
            snapshot_block = chunk.get('SnapshotBlock')
            if snapshot_block is None:
                logging.error(
                    f'Snapshot block is empty in chunk {slice_start_height} - {slice_end_height} ')
            else:
                save_snapshot_block_dict(snapshot_block)

            snapshot_timestamp = snapshot_block.get("timestamp", 0)
            if snapshot_timestamp > 0:
                timestamp = snapshot_timestamp

            account_blocks = chunk.get('AccountBlocks', None)
            if account_blocks is None:
                continue
            for account_block in account_blocks:
                save_account_block_from_dict(account_block, timestamp)

        if (timestamp == 0) and (len(chunks) > 0):
            logging.error(
                f'No timestamp in chunk {slice_start_height} - {slice_end_height} ')
        else:
            chunk_date = datetime.fromtimestamp(timestamp)
            logging.info(
                f'downloaded chunks {start_height} - {end_height}, chunk date {chunk_date}')

        slice_start_height = slice_start_height - SLICE_SIZE
        slice_end_height = slice_end_height - SLICE_SIZE

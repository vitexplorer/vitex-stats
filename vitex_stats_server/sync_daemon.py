import time
import logging
from datetime import datetime

import daemon
import requests

from flask import current_app as app
from sqlalchemy.exc import SQLAlchemyError, NoResultFound

from vitex_stats_server.statistic.data_accessor import update_sbp_activity

from .ledger.data_accessor import gvite_get_account, gvite_get_account_block_by_hash, gvite_get_snapshot_block, save_account_block_from_dict, save_account_from_dict, save_snapshot_block_dict
from vitex_stats_server.models import Account,  db

ERR_REQUIRE_NEW_FILTER = -32002
ERR_NO_RESULT = -1


def sync_daemon_main():
    with daemon.DaemonContext():
        logging.basicConfig(level=logging.INFO,
                            format='%(asctime)s %(levelname)-8s %(message)s',
                            filename='/tmp/vitex_sync_daemon.log')
        sync_loop()


def sync_loop():
    if app.config['LOGLEVEL'] == 'WARNING':
        logging.getLogger().setLevel(logging.WARNING)
    elif app.config['LOGLEVEL'] == 'ERROR':
        logging.getLogger().setLevel(logging.ERROR)

    account_block_filter = register_account_block_filter()
    if account_block_filter == '':
        logging.error('register account block filter failed')
        return

    logging.info(f'account block filter id: {account_block_filter}')

    snapshot_block_filter = register_snapshot_block_filter()
    if snapshot_block_filter == '':
        logging.error('register snapshot block filter failed')
        return
    logging.info(f'snapshot block filter id: {snapshot_block_filter}')

    while True:
        err, account_block_changes = get_account_block_changes(
            account_block_filter)
        if err == ERR_REQUIRE_NEW_FILTER:
            account_block_filter = register_account_block_filter()
            if account_block_filter == '':
                logging.error('register account block filter failed')
                continue

        elif err == ERR_NO_RESULT:
            continue
        elif len(account_block_changes) == 0:
            # wait 1 second for changes
            time.sleep(1)

        timestamp_now = int(datetime.now().timestamp())

        account_addresses_to_update = []

        for change in account_block_changes:
            account_block_hash = change['hash']
            account_block = gvite_get_account_block_by_hash(
                account_block_hash, deserialize=False)
            if account_block is None:
                logging.error(f'account block {account_block_hash} not found')
                continue
            block_timestamp = account_block.get('timestamp', 0)
            if block_timestamp == 0:
                block_hash = account_block.get('hash')

                account_block_retried = gvite_get_account_block_by_hash(
                    block_hash, deserialize=False)
                if account_block_retried is not None:
                    account_block = account_block_retried
                    block_timestamp = account_block.get('timestamp', 0)
                    if block_timestamp == 0:
                        logging.warn(
                            f'save_account_block_from_dict() invalid timestamp: {block_timestamp}, download again block: {block_hash}, fallback to {timestamp_now}')
                        account_block['timestamp'] = timestamp_now

            save_account_block_from_dict(account_block, timestamp_now)
            logging.info(f'saved account block {account_block_hash}')
            if account_block['accountAddress'] not in account_addresses_to_update:
                account_addresses_to_update.append(
                    account_block['accountAddress'])
            if account_block['toAddress'] not in account_addresses_to_update:
                account_addresses_to_update.append(
                    account_block['toAddress'])
            if account_block['fromAddress'] not in account_addresses_to_update:
                account_addresses_to_update.append(
                    account_block['fromAddress'])

        for account_address in account_addresses_to_update:
            update_account_balance_and_timestamp(
                account_address, timestamp_now)

        err, snapshot_block_changes = get_snapshot_block_changes(
            snapshot_block_filter)
        if err == ERR_REQUIRE_NEW_FILTER:
            snapshot_block_filter = register_snapshot_block_filter()
            if snapshot_block_filter == '':
                logging.error('register snapshot block filter failed')
                continue

        producer_addresses_to_update = []

        for change in snapshot_block_changes:
            snapshot_block_height = change['height']
            snapshot_block = gvite_get_snapshot_block(snapshot_block_height)
            if snapshot_block is None:
                logging.error(
                    f'snapshot block {snapshot_block_height} not found')
                continue
            save_snapshot_block_dict(snapshot_block)
            logging.info(f'saved snapshot block {snapshot_block_height}')
            if snapshot_block['producer'] not in producer_addresses_to_update:
                producer_addresses_to_update.append(
                    snapshot_block['producer'])
        for producer_address in producer_addresses_to_update:
            touch_sbp_activity(producer_address, timestamp_now)


def register_account_block_filter():
    headers = {'content-type': 'application/json'}
    request_body = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "subscribe_createAccountBlockFilter",
        "params": []
    }

    response = requests.post(
        app.config['URL_RPC'],  json=request_body, headers=headers)

    result = response.json().get('result', {})

    return result


def get_account_block_changes(filter_id):
    '''
    get account block changes
    return error_code, account_block_changes[]
    '''
    headers = {'content-type': 'application/json'}
    request_body = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "subscribe_getChangesByFilterId",
        "params": [filter_id, ]
    }

    response = requests.post(
        app.config['URL_RPC'],  json=request_body, headers=headers)
    response = response.json()

    err = response.get('error', None)

    if err is not None:
        err_code = err.get('code', None)
        if err_code == ERR_REQUIRE_NEW_FILTER:
            logging.warning('filter expired, will create a new one')
        else:
            err_msg = err.get('message', '')
            logging.error(
                f'get account block changes failed, error code: {err_code}, {err_msg}')

        return err_code, []

    result = response.get('result', None)
    if result is None:
        logging.error('get account block changes failed, no result')
        return ERR_NO_RESULT, []

    account_block_changes = result.get('result')
    if account_block_changes is None:
        account_block_changes = []

    return None, account_block_changes


def update_account_last_transaction_date(account_block, default_timestamp=0):
    timestamp = account_block.get('timestamp', default_timestamp)
    new_datetime = datetime.fromtimestamp(timestamp)
    account_address = account_block['accountAddress']
    try:
        account = db.session.query(Account).filter_by(
            address=account_address).one()

    except NoResultFound:
        logging.info(f'account {account_address} not found, downloading')
        account = gvite_get_account(account_address)
        if account is None:
            logging.error(f'account {account_address} not found')
            return
        account.update({'lastTransactionDate': new_datetime})
        account = save_account_from_dict(account)
        return account

    account.last_transaction_date = new_datetime
    try:
        db.session.commit()
    except SQLAlchemyError as err:
        logging.error(
            f'Fail to commit account {account.address}, SQLAlchemyError {err}')

    except Exception as err:
        logging.error(
            f'Fail to commit account {account.address}, General Error {err}')

    return account


def update_account_balance_and_timestamp(account_address, timestamp):
    if account_address == '':
        return

    new_datetime = datetime.fromtimestamp(timestamp)

    account_dict = gvite_get_account(account_address)
    account_dict.update({'lastTransactionDate': new_datetime})
    if account_dict is None:
        logging.error(f'account {account_address} not found')
    save_account_from_dict(account_dict)


def register_snapshot_block_filter():
    headers = {'content-type': 'application/json'}
    request_body = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "subscribe_createSnapshotBlockFilter",
        "params": []
    }

    response = requests.post(
        app.config['URL_RPC'],  json=request_body, headers=headers)

    result = response.json().get('result', None)

    return result


def get_snapshot_block_changes(filter_id):
    '''
    get account block changes
    return error_code, account_block_changes[]
    '''
    headers = {'content-type': 'application/json'}
    request_body = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "subscribe_getChangesByFilterId",
        "params": [filter_id, ]
    }

    response = requests.post(
        app.config['URL_RPC'],  json=request_body, headers=headers)
    response = response.json()

    err = response.get('error', None)

    if err is not None:
        err_code = err.get('code', None)
        if err_code == ERR_REQUIRE_NEW_FILTER:
            logging.warning('filter expired, will create a new one')
        else:
            err_msg = err.get('message', '')
            logging.error(
                f'get snapshot block changes failed, error code: {err_code}, {err_msg}')

        return err_code, []

    result = response.get('result', None)
    if result is None:
        logging.error('get snapshot block changes failed, no result')
        return ERR_NO_RESULT, []

    snapshot_block_changes = result.get('result')
    if snapshot_block_changes is None:
        snapshot_block_changes = []

    return None, snapshot_block_changes


def touch_sbp_activity(producer_address, timestamp):
    update_sbp_activity(producer_address, timestamp)

from flask import current_app as app
import psycopg2
import requests
from sqlalchemy.exc import SQLAlchemyError, NoResultFound, PendingRollbackError, IntegrityError
from marshmallow.exceptions import ValidationError

from vitex_stats_server.contract.data_accessor import db_save_token_info_dict, gvite_get_token_info
from ..models import Account, AccountBlock, AccountBlockSchema, AccountSchema, AccountSchemaSimple, Balance, BalanceSchema, CompleteAccountBlockSchema, SnapshotBlock, SnapshotBlockSchema, SnapshotData, db


schema_account_block = AccountBlockSchema()
schema_account_block_complete = CompleteAccountBlockSchema()
schema_account = AccountSchema()
schema_account_simple = AccountSchemaSimple()
schema_snapshot_block = SnapshotBlockSchema()
schema_balance = BalanceSchema()


SORT_FIELD_ACCOUNT_BLOCK = {
    'timestamp': AccountBlock.timestamp,
    'height': AccountBlock.height,
    'hash': AccountBlock.hash,
    'fromAddress': AccountBlock.from_address,
    'toAddress': AccountBlock.to_address
}


def get_sort_criteria_account_block(order='desc', sort_field='timestamp'):
    result = SORT_FIELD_ACCOUNT_BLOCK.get(sort_field, AccountBlock.hash)

    if order == 'desc':
        return result.desc()

    return result.asc()


SORT_FIELD_ACCOUNT = {
    'address': Account.address,
    'blockCount': Account.block_count,
    'currentQuota': Account.current_quota,
    'maxQuota': Account.max_quota,
    'stakeAmount': Account.stake_amount,
    'lastTransactionDate': Account.last_transaction_date,
    'viteBalance': Account.vite_balance,
}


def get_sort_criteria_account(order='desc', sort_field='address'):
    result = SORT_FIELD_ACCOUNT.get(sort_field, Account.address)

    if order == 'desc':
        return result.desc()

    return result.asc()


def da_get_account_blocks():
    return AccountBlock.query.all()


def gvite_get_snapshot_block(height):
    headers = {'content-type': 'application/json'}
    request_body = {
        "jsonrpc": "2.0",
        "id": 2,
        "method": "ledger_getSnapshotBlockByHeight",
        "params": [height, ]
    }

    response = requests.post(
        app.config['URL_RPC'],  json=request_body, headers=headers)

    result = response.json().get('result', {})

    return result


def save_snapshot_data_from_dict(snapshot_block_hash, address, data_dict):
    account = db_get_account(address)
    if account is None:
        account_dict = gvite_get_account(address)
        account = save_account_from_dict(account_dict)

    snapshot_data = SnapshotData(
        snapshot_block_hash=snapshot_block_hash,
        account_address=address,
        height=data_dict.get('height'),
        hash=data_dict.get('hash'),
    )
    db.session.add(snapshot_data)
    try:
        db.session.commit()
    except IntegrityError:  # normally it is a duplicate key error
        db.session.rollback()
        db.session.merge(snapshot_data)
        db.session.commit()
    except SQLAlchemyError as e:
        db.session.rollback()
        app.logger.error(f'save_snapshot_data_from_dict SQLAlchemyError: {e}')
        raise e


def save_snapshot_block_dict(snapshot_block_dict):

    snapshot_block = SnapshotBlock(
        producer=snapshot_block_dict.get('producer'),
        hash=snapshot_block_dict.get('hash'),
        prev_hash=snapshot_block_dict.get('prevHash'),
        height=snapshot_block_dict.get('height'),
        public_key=snapshot_block_dict.get('publicKey'),
        signature=snapshot_block_dict.get('signature'),
        version=snapshot_block_dict.get('version'),
        timestamp=snapshot_block_dict.get('timestamp'),
    )
    existing_snapshot_block = db.session.get(
        SnapshotBlock, snapshot_block.hash)
    if existing_snapshot_block:
        db.session.merge(snapshot_block)
    else:
        db.session.add(snapshot_block)

    try:
        db.session.commit()
    except SQLAlchemyError as err:
        app.logger.error(
            f'fail to commit snapshot block at {snapshot_block.height}: SQLAlchemyError {err}')
        db.session.rollback()
        return None
    except Exception as err:
        app.logger.error(
            f'fail to commit snapshot block {snapshot_block.height}: General Error {err}')
        db.session.rollback()
        return None

    snapshot_block_hash = snapshot_block.hash

    snapshot_data = snapshot_block_dict.get('snapshotData')

    if snapshot_data is None:
        return snapshot_block

    for address, data in snapshot_data.items():
        save_snapshot_data_from_dict(snapshot_block_hash, address, data)

    return snapshot_block


def db_get_snapshot_block_by_hash(hashstr):
    return db.session.get(SnapshotBlock, hashstr)


def db_get_snapshot_blocks_by_address(address, order, sort_field, page_idx, page_size):
    '''
    address: account address
    '''

    offset = page_idx * page_size

    count = db.session.query(SnapshotData).filter(
        SnapshotData.account_address == address).count()

    snapshot_datas = db.session.query(SnapshotData).filter(
        SnapshotData.account_address == address).order_by(SnapshotData.height.desc()).offset(offset).limit(page_size)

    result = []

    for snapshot_data in snapshot_datas:
        snapshot_block = snapshot_data.snapshot_block
        result.append(snapshot_block)

    return result, count


def db_get_snapshot_blocks(order, sort_field, page_idx, page_size):

    offset = page_idx * page_size

    count = db.session.query(SnapshotBlock).count()

    snapshot_blocks = db.session.query(SnapshotBlock).order_by(
        SnapshotBlock.height.desc()).offset(offset).limit(page_size)

    return snapshot_blocks, count


def db_get_latest_snapshot_blocks(page_size):

    snapshot_blocks = db.session.query(SnapshotBlock).order_by(
        SnapshotBlock.height.desc()).limit(page_size)

    return snapshot_blocks


def gvite_get_chunks(start_height: int, end_height: int):
    headers = {'content-type': 'application/json'}
    request_body = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "ledger_getChunks",
        "params": [start_height, end_height]
    }

    response_chunks = requests.post(
        app.config['URL_RPC'],  json=request_body, headers=headers)

    if not response_chunks.ok:
        app.logger.error(
            f'RPC call failed: ledger_getChunks, code: {response_chunks.status_code}, msg: {response_chunks.text}')
        return []

    response = response_chunks.json().get('result', [])

    return response


def gvite_get_snapshot_chain_height():
    headers = {'content-type': 'application/json'}
    request_body = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "ledger_getSnapshotChainHeight",
        "params": []
    }

    response = requests.post(
        app.config['URL_RPC'],  json=request_body, headers=headers)

    if not response.ok:
        app.logger.error(
            f'RPC call failed: ledger_getSnapshotChainHeight, code: {response.status_code}, msg: {response.text}')
        return None

    result = int(response.json().get('result', 0))

    return result


def gvite_get_account_block_by_hash(hashstr, deserialize=True):
    '''
    hashstr: block hash
    deserialize: parse json result to AccountBlock object
    '''

    headers = {'content-type': 'application/json'}
    request_body = {
        "jsonrpc": "2.0",
        "id": 17,
        "method": "ledger_getAccountBlockByHash",
        "params": [hashstr, ]
    }

    response_account_block = requests.post(
        app.config['URL_RPC'],  json=request_body, headers=headers)

    if not response_account_block.ok:
        app.logger.error(
            f'RPC call failed: ledger_getAccountBlockByHash, code: {response_account_block.status_code}, msg: {response_account_block.text}')
        return None

    result = response_account_block.json().get('result', {})

    if deserialize:
        try:
            account_block = schema_account_block.load(result, partial=True)
        except ValidationError as e:
            app.logger.error(
                f'gvite_get_account_block_by_hash() ValidationError: {e}, account_block: {result}')
            return None

        return account_block

    return result


def db_get_account_block_by_hash(hashstr):
    return db.session.get(AccountBlock, hashstr)


def account_block_complete(account_block):
    return account_block is not None and account_block.from_address != ''


def db_save_account_block(account_block):
    q = db.session.query(AccountBlock).filter_by(hash=account_block.hash)
    try:
        existing_account_block = q.one()
    except NoResultFound:
        db.session.add(account_block)
    else:
        app.logger.info(
            f'account block {account_block.hash} already exists, updating')
        db.session.merge(account_block)
    finally:
        try:
            db.session.commit()
            return account_block
        except SQLAlchemyError as err:
            app.logger.error(
                f'fail to commit account block {account_block.hash}: SQLAlchemyError {err}')
            db.session.rollback()
            return None
        except Exception as err:
            app.logger.error(
                f'fail to commit account block {account_block.hash}: General Error {err}')
            db.session.rollback()
            return None


def none_to_zero(src):
    if src:
        return src
    return 0


def sanitize_hash(src):
    if src is None:
        return ''
    # convert multiple '0' such as '0000...' to a single '0'
    if all(map(lambda x: x == '0', src)):
        return None
    return src


def save_account_block_from_dict(src, default_timestamp=0, triggered_by=None):
    '''
    src is a dict parsed from JSON reply of getChunks or getAccountBlockByHash.
    It can be either of the following 2 kinds:
    1. complete account block
    2. simple account block from getChunks, for example:
        {
            "blockType": 2,
            "hash": "603e64f972960cd886a1dc77308b9a7d127cb462567b1ecd2ef68b4330a6d0c0",
            "prevHash": "592a1e46ad746fd7d0b12b6e51ad62ffaea42574e494f8ffeab611f11021efc0",
            "height": 4,
            "accountAddress": "vite_483eed4ba0b5cd984d480ca048d5ee8ef5fa6b0ae23774c09b",
            "publicKey": "oDxoxc75R4NnzWb4Vj3P9kmSercIquNzZlPlDnhRN2M=",
            "toAddress": "vite_0000000000000000000000000000000000000003f6af7459b9",
            "amount": 1000000000000000000000,
            "tokenId": "tti_5649544520544f4b454e6e40",
            "fromBlockHash": "0000000000000000000000000000000000000000000000000000000000000000",
            "data": "jefc/QAAAAAAAAAAAAAA14lDHx2CBQbIP9U5oK6YY9aWE4IB",
            "quota": 82000,
            "quotaUsed": 82000,
            "fee": 0,
            "logHash": null,
            "difficulty": null,
            "nonce": null,
            "sendBlockList": [],
            "signature": "hWfLf7vXqIevTPdu6acc0PVVyNYxGtN9OU73F7Vn8+ZJ5vN6Lo76q1yalt5+OdqDRA4IzlQ8ruTNgvuwT0ooCw=="
        }
    '''
    hashstr = src['hash']
    quota_by_stake = src.get('quotaByStake', 0)
    total_quota = src.get('totalQuota', 0)
    timestamp = src.get('timestamp', default_timestamp)
    token_id = src['tokenId']
    if timestamp == 0:
        app.logger.warn(
            f'save_account_block_from_dict() invalid timestamp: {timestamp}, block: {src}')

    account_block = AccountBlock(
        block_type=src['blockType'],
        height=src['height'],
        hash=hashstr,
        previous_hash=src['prevHash'],
        address=src['accountAddress'],
        public_key=src['publicKey'],
        producer=sanitize_hash(src.get('producer')),
        from_address=sanitize_hash(src.get('fromAddress')),
        to_address=src['toAddress'],
        send_block_hash=sanitize_hash(src.get('sendBlockHash')),
        token_id=token_id,
        amount=none_to_zero(src.get('amount')),

        fee=src['fee'],
        data=src['data'],
        difficulty=none_to_zero(src['difficulty']),
        nonce=sanitize_hash(src['nonce']),
        signature=src['signature'],
        quota_by_stake=quota_by_stake,
        total_quota=total_quota,
        vm_log_hash=sanitize_hash(src.get('vmlogHash')),

        triggered_by_account_block_hash=triggered_by,

        confirmations=none_to_zero(src.get('confirmations')),
        first_snapshot_hash=sanitize_hash(
            src.get('firstSnapshotHash')),
        timestamp=timestamp,
        receive_block_height=none_to_zero(
            src.get('receiveBlockHeight')),
        receive_block_hash=sanitize_hash(src.get('receiveBlockHash'))
    )

    q = db.session.query(AccountBlock).filter_by(hash=hashstr)
    try:
        existing_account_block = q.one()
    except NoResultFound:
        db.session.add(account_block)
    else:
        app.logger.info(f'account block {hashstr} already exists, updating')
        db.session.merge(account_block)
    finally:
        try:
            db.session.commit()
        except SQLAlchemyError as err:
            db.session.rollback()
            # foreign key violation, probably a new token appears,
            # AccountBlock has a Token relation and an AccountBlock relation
            # ref: https://www.postgresql.org/docs/current/errcodes-appendix.html#ERRCODES-TABLE
            if int(err.orig.pgcode) == 23503:
                app.logger.info(f'find new Token {token_id}, downloading')
                token_json = gvite_get_token_info(token_id)
                db_save_token_info_dict(token_json)
                try:
                    db.session.query(AccountBlock).filter_by(
                        hash=hashstr).one()
                except NoResultFound:
                    db.session.add(account_block)
                else:
                    app.logger.info(
                        f'account block {hashstr} already exists, updating')
                    db.session.merge(account_block)
                finally:
                    try:
                        db.session.commit()
                    except SQLAlchemyError as err:
                        app.logger.error(
                            f'fail to commit account block {hashstr}: SQLAlchemyError {err}')
                        db.session.rollback()
            else:
                app.logger.error(
                    f'fail to commit account block {hashstr}: SQLAlchemyError {err}')

        except Exception as err:
            app.logger.error(
                f'fail to commit account block {src["hash"]}: General Error {err}')
            db.session.rollback()

    sendBlockList = src.get('sendBlockList', [])
    if sendBlockList is None:
        sendBlockList = []

    # save triggered send blocks before save the account block
    for send_block in sendBlockList:
        save_account_block_from_dict(send_block, default_timestamp, hashstr)


def db_get_account_block_by_token_id(token_id, order='desc', sort_field='timestamp', page_idx=0, page_size=10):

    offset = page_idx * page_size

    sort_criteria = get_sort_criteria_account_block(order, sort_field)

    # count_sql = f'select count(hash) from public.account_block where account_block.token_id=\'{token_id}\' '
    # count = db.session.execute(count_sql).scalar()

    account_blocks = db.session.query(AccountBlock).filter(
        AccountBlock.token_id == token_id).order_by(sort_criteria).offset(offset).limit(page_size)

    count = account_blocks.count()
    if count >= page_size:
        count = 10000  # assume count is 10k until we can accelerate count operation

    return account_blocks, count


def db_get_account_blocks(order='desc', sort_field='timestamp', page_idx=0, page_size=10):

    offset = page_idx * page_size

    sort_criteria = get_sort_criteria_account_block(order, sort_field)

    account_blocks = db.session.query(AccountBlock).order_by(
        sort_criteria).offset(offset).limit(page_size)

    count = account_blocks.count()
    if count >= page_size:
        count = 10000  # assume count is 10k until we can accelerate count operation

    return account_blocks, count


def db_get_account_blocks_by_account(address, order='desc', sort_field='timestamp', page_idx=0, page_size=10):

    offset = page_idx * page_size

    sort_criteria = get_sort_criteria_account_block(order, sort_field)

    account_blocks = db.session.query(AccountBlock).filter(
        AccountBlock.address == address).order_by(sort_criteria).offset(offset).limit(page_size)

    count = account_blocks.count()

    if count >= page_size:
        count = 10000

    return account_blocks, count


def gvite_get_account_blocks_by_account(address, order='desc', sort_field='timestamp', page_idx=0, page_size=10):
    headers = {'content-type': 'application/json'}
    request_body = {
        "jsonrpc": "2.0",
        "id": 17,
        "method": "ledger_getAccountBlocksByAddress",
        "params": [address, page_idx, page_size]
    }
    response_account_blocks = requests.post(
        app.config['URL_RPC'],  json=request_body, headers=headers)
    if not response_account_blocks.ok:
        app.logger.error(
            f'RPC call failed: ledger_getAccountBlockByHash, code: {response_account_blocks.status_code}, msg: {response_account_blocks.text}')
        return [], 0

    result = response_account_blocks.json()['result']

    result_size = len(result)
    try:
        account_blocks = schema_account_block.load(result, many=True)
    except ValidationError as err:
        app.logger.error(
            f'fail to load account block schema: {err} - {result}')
        return [], 0

    if result_size == page_size:
        count = 10000
    else:
        count = result_size

    return account_blocks, count


def gvite_get_unreceived_account_blocks_by_account(address, order='desc', sort_field='timestamp', page_idx=0, page_size=10):
    headers = {'content-type': 'application/json'}
    request_body = {
        "jsonrpc": "2.0",
        "id": 17,
        "method": "ledger_getUnreceivedBlocksByAddress",
        "params": [address, page_idx, page_size]
    }
    response_account_blocks = requests.post(
        app.config['URL_RPC'],  json=request_body, headers=headers)
    if not response_account_blocks.ok:
        app.logger.error(
            f'RPC call failed: ledger_getUnreceivedBlocksByAddress, code: {response_account_blocks.status_code}, msg: {response_account_blocks.text}')
        return [], 0

    result = response_account_blocks.json()['result']

    result_size = len(result)
    try:
        account_blocks = schema_account_block.load(result, many=True)
    except ValidationError as err:
        app.logger.error(
            f'fail to load account block schema: {err} - {result}')
        return None, 0

    if result_size == page_size:
        count = 10000
    else:
        count = result_size

    return account_blocks, count


def gvite_get_account(address):
    '''
    address: account address
    '''

    headers = {'content-type': 'application/json'}
    request_body = {
        "jsonrpc": "2.0",
        "id": 17,
        "method": "ledger_getAccountInfoByAddress",
        "params": [address, ]
    }

    response_account = requests.post(
        app.config['URL_RPC'],  json=request_body, headers=headers)

    if not response_account.ok:
        app.logger.error(
            f'RPC call failed: ledger_getAccountBlockByHash, code: {response_account.status_code}, msg: {response_account.text}')
        return None

    result = response_account.json().get('result', {})
    return result


def db_get_account(address):
    '''
    address: account address
    '''
    return db.session.get(Account, address)


def save_account_from_dict(src):
    address = src.get('address')
    balance_dict = src.get('balanceInfoMap', dict())
    vite_balance = 0
    for token_id, balance_src in balance_dict.items():
        if balance_src['tokenInfo']['tokenSymbol'] == 'VITE':
            vite_balance = balance_src.get('balance', 0)
            break
    account = Account(
        address=address,
        block_count=src.get('blockCount', 0),
        current_quota=src.get('currentQuota', 0),
        max_quota=src.get('maxQuota', 0),
        stake_amount=src.get('stakeAmount', 0),
        vite_balance=vite_balance
    )
    last_transaction_date = src.get('lastTransactionDate', None)
    if last_transaction_date:
        account.last_transaction_date = last_transaction_date

    try:
        existing_account = db.session.query(Account).get(address)
    except PendingRollbackError as err:
        app.logger.error(
            f'Fail to get account {address}, PendingRollbackError: {err}')
        db.session.rollback()
        return
    except Exception as err:
        app.logger.error(
            f'Fail to get account {address}, General Error {err}')
        db.session.rollback()
        return

    if existing_account:
        db.session.merge(account)
    else:
        db.session.add(account)

    try:
        db.session.commit()
    except psycopg2.IntegrityError as err:
        app.logger.error(
            f'Fail to commit account {address}: psycopg2.IntegrityError {err}')
        db.session.rollback()
        return
    except SQLAlchemyError as err:
        app.logger.error(
            f'Fail to commit account {address}: SQLAlchemyError {err}')
        db.session.rollback()
        return

    except Exception as err:
        app.logger.error(
            f'Fail to commit account {address}: General Error {err}')
        db.session.rollback()
        return

    for token_id, balance_src in balance_dict.items():
        balance = Balance(
            account_address=address,
            token_id=token_id,
            balance=balance_src.get('balance', 0)
        )
        q = db.session.query(Balance).filter_by(
            token_id=token_id, account_address=address)
        try:
            existing_account_block = q.one()
        except NoResultFound:
            db.session.add(balance)
        else:
            if str(balance.balance) == str(existing_account_block.balance):
                continue
            else:
                db.session.merge(balance)
        finally:
            try:
                db.session.commit()
            except SQLAlchemyError as err:
                app.logger.error(
                    f'fail to commit balance {address} - {token_id}: SQLAlchemyError {err}')
                db.session.rollback()

                # foreign key violation, probably a new token appears
                # ref: https://www.postgresql.org/docs/current/errcodes-appendix.html#ERRCODES-TABLE
                if int(err.orig.pgcode) == 23503:
                    app.logger.info(f'find new Token {token_id}, downloading')
                    token_json = gvite_get_token_info(token_id)
                    db_save_token_info_dict(token_json)
                    app.logger.info(f'downloaded new Token {token_id}')
                    # save again the balance
                    balance = Balance(
                        account_address=address,
                        token_id=token_id,
                        balance=balance_src.get('balance', 0)
                    )
                    q = db.session.query(Balance).filter_by(
                        token_id=token_id, account_address=address)
                    try:
                        existing_account_block = q.one()
                    except NoResultFound:
                        db.session.add(balance)
                    else:
                        app.logger.info(
                            f'balance {address} - {token_id} already exists, updating')
                        db.session.merge(balance)
                    try:
                        db.session.commit()
                    except SQLAlchemyError as err:
                        app.logger.error(
                            f'fail to commit balance {address} - {token_id} after downloading token: SQLAlchemyError Error {err}')
                        db.session.rollback()
                    continue
                return
            except Exception as err:
                app.logger.error(
                    f'fail to commit balance {address} - {token_id}: General Error {err}')
                db.session.rollback()
                return

    return account


def db_get_accounts(order='desc', sort_field='viteBalance', page_idx=0, page_size=10):
    offset = page_idx * page_size

    sort_criteria = get_sort_criteria_account(order, sort_field)

    if sort_field == 'lastTransactionDate':
        accounts = db.session.query(Account).filter(Account.last_transaction_date.isnot(None)).order_by(sort_criteria).offset(
            offset).limit(page_size)
    else:
        accounts = db.session.query(Account).order_by(
            sort_criteria).offset(offset).limit(page_size)

    count = accounts.count()
    if count >= page_size:
        count = 10000  # assume count is 10k until we can accelerate count operation

    return accounts, count


def db_search_accounts(keyword='', order='asc', sort_field='address', page_idx=0, page_size=10):
    # skip common prefix "vite_" or empty keyword
    if (len(keyword) == 0) or (keyword in 'vite_'):
        return db_get_accounts(order, sort_field, page_idx, page_size)

    offset = page_idx * page_size

    sort_criteria = get_sort_criteria_account(order, sort_field)

    accounts = db.session.query(Account).filter(Account.address.ilike(f'{keyword}%')).order_by(
        sort_criteria).offset(offset).limit(page_size)

    count = accounts.count()

    return accounts, count


def da_get_token_balances_desc(token_id, page_idx=0, page_size=10):
    offset = page_idx * page_size

    balances = db.session.query(Balance).filter(
        Balance.token_id == token_id).order_by(Balance.balance.desc()).offset(offset).limit(page_size)
    count = db.session.query(Balance).filter(
        Balance.token_id == token_id).count()

    return balances, count

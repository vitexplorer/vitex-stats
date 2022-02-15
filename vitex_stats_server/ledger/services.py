from datetime import datetime, timedelta
from vitex_stats_server.contract.data_accessor import gvite_get_account_quota
from flask import request, jsonify, Blueprint
from flask import current_app as app
from .data_accessor import da_get_token_balances_desc, db_get_account, db_get_account_blocks_by_account, db_get_accounts, db_get_latest_snapshot_blocks, db_get_snapshot_blocks, db_get_snapshot_blocks_by_address, db_save_account_block, db_search_accounts, gvite_get_account, gvite_get_account_block_by_hash, db_get_account_block_by_hash, account_block_complete, gvite_get_account_blocks_by_account, gvite_get_unreceived_account_blocks_by_account, save_account_block_from_dict, save_account_from_dict,  schema_account_block, schema_account_block_complete, schema_account, db_get_account_block_by_token_id, db_get_account_blocks, schema_snapshot_block, schema_balance

bp_ledger = Blueprint('ledger', __name__, url_prefix='/ledger')


@bp_ledger.route('/get_account_block_by_hash/<hash_str>', methods=('GET', 'POST'))
def get_account_block_by_hash(hash_str):
    if request.method == 'POST':
        pass

    # fixed length hash, example:
    # fc648d40ce2d26316a1121d06ff94c76de1bb186de53a37e6cb039c801f9a954
    if len(hash_str) != 64:
        return jsonify({
            'err': f'hash length must be 64, got {len(hash_str)}',
            'result': {}})

    account_block = db_get_account_block_by_hash(hash_str)

    if account_block and account_block_complete(account_block):
        app.logger.info(f'DB hit account block {hash_str}')
        return jsonify({'err': 'ok', 'result': schema_account_block.dump(account_block)})

    account_block = gvite_get_account_block_by_hash(
        hash_str, deserialize=False)

    if account_block is None:
        app.logger.info(f'cannot find account block #{hash_str}')
        return jsonify({
            'err': f'cannot find account block #{hash_str}',
            'result': {}})

    save_account_block_from_dict(account_block)
    app.logger.info(f'completed account block to DB {hash_str}')

    account_block = db_get_account_block_by_hash(hash_str)

    return jsonify({'err': 'ok', 'result': schema_account_block.dump(account_block)})


@bp_ledger.route('/get_complete_account_block_by_hash/<hash_str>', methods=('GET', 'POST'))
def get_complete_account_block_by_hash(hash_str):
    if request.method == 'POST':
        pass

    # fixed length hash, example:
    # fc648d40ce2d26316a1121d06ff94c76de1bb186de53a37e6cb039c801f9a954
    if len(hash_str) != 64:
        return jsonify({
            'err': f'hash length must be 64, got {len(hash_str)}',
            'result': {}})

    account_block = db_get_account_block_by_hash(hash_str)

    if account_block and account_block_complete(account_block):
        app.logger.info(f'DB hit account block {hash_str}')
        return jsonify({'err': 'ok', 'result': schema_account_block_complete.dump(account_block)})

    account_block = gvite_get_account_block_by_hash(
        hash_str, deserialize=False)

    if account_block is None:
        app.logger.info(f'cannot find account block #{hash_str}')
        return jsonify({
            'err': f'cannot find account block #{hash_str}',
            'result': {}})

    save_account_block_from_dict(account_block)
    app.logger.info(f'completed account block to DB {hash_str}')

    account_block = db_get_account_block_by_hash(hash_str)

    return jsonify({'err': 'ok', 'result': schema_account_block_complete.dump(account_block)})


@bp_ledger.route('/get_account_block_by_token/<token_id>/<int:page_idx>/<int:page_size>', methods=('GET', 'POST'))
def get_account_block_by_token(token_id, page_idx, page_size):
    account_blocks, count = db_get_account_block_by_token_id(
        token_id, 'desc', 'timestamp', page_idx, page_size)

    result = {
        'err': 'ok',
        'count': count,
        'pageIdx': page_idx,
        'pageSize': page_size,
        'accountBlocks': schema_account_block.dump(account_blocks, many=True)
    }

    return jsonify(result)


@bp_ledger.route('/get_account_block_by_token/<token_id>/<order>/<sort_field>/<int:page_idx>/<int:page_size>', methods=('GET', 'POST'))
def get_account_block_by_token_order(token_id, order, sort_field, page_idx, page_size):
    account_blocks, count = db_get_account_block_by_token_id(
        token_id, order, sort_field, page_idx, page_size)
    result = {
        'err': 'ok',
        'count': count,
        'pageIdx': page_idx,
        'pageSize': page_size,
        'accountBlocks': schema_account_block.dump(account_blocks, many=True)
    }

    return jsonify(result)


@bp_ledger.route('/get_account_blocks/<order>/<sort_field>/<int:page_idx>/<int:page_size>',  methods=('GET', 'POST'))
def get_account_blocks(order, sort_field, page_idx, page_size):
    if request.method == 'POST':
        pass

    account_blocks, count = db_get_account_blocks(
        order, sort_field, page_idx, page_size)

    result = {
        'err': 'ok',
        'count': count,
        'pageIdx': page_idx,
        'pageSize': page_size,
        'accountBlocks': schema_account_block.dump(account_blocks, many=True)
    }

    return jsonify(result)


@bp_ledger.route('/get_account_blocks_by_account/<address>/<order>/<sort_field>/<int:page_idx>/<int:page_size>', methods=('GET', 'POST'))
def get_account_blocks_by_account(address, order, sort_field, page_idx, page_size):
    if request.method == 'POST':
        pass

    # assume gvite backend is more up to date than db
    if order == 'desc' and sort_field == 'timestamp':
        account_blocks, count = gvite_get_account_blocks_by_account(
            address, order, sort_field, page_idx, page_size)

        if len(account_blocks) == 0:
            app.logger.info(
                f'cannot fetch from gvite the account blocks of account {address}')
            account_blocks, count = db_get_account_blocks_by_account(
                address, order, sort_field, page_idx, page_size)
        else:
            saved_account_blocks = []
            for account_block in account_blocks:
                saved_account_block = db_save_account_block(account_block)
                saved_account_blocks.append(saved_account_block)

            account_blocks = saved_account_blocks
    else:
        account_blocks, count = db_get_account_blocks_by_account(
            address, order, sort_field, page_idx, page_size)

    result = {
        'err': 'ok',
        'count': count,
        'pageIdx': page_idx,
        'pageSize': page_size,
        'accountBlocks': schema_account_block.dump(account_blocks, many=True)
    }

    return jsonify(result)


@bp_ledger.route('/get_unreceived_account_blocks_by_account/<address>/<order>/<sort_field>/<int:page_idx>/<int:page_size>', methods=('GET', 'POST'))
def get_unreceived_account_blocks_by_account(address, order, sort_field, page_idx, page_size):
    if request.method == 'POST':
        pass

    account_blocks, count = gvite_get_unreceived_account_blocks_by_account(
        address, order, sort_field, page_idx, page_size)

    result = {
        'err': 'ok',
        'count': count,
        'pageIdx': page_idx,
        'pageSize': page_size,
        'accountBlocks': schema_account_block.dump(account_blocks, many=True)
    }

    return jsonify(result)


def account_need_update(account):
    if account is None:
        return True
    return datetime.now() - account.last_modified > timedelta(minutes=5)


@bp_ledger.route('/get_account/<address>',  methods=('GET', 'POST'))
def get_account(address):
    account = db_get_account(address)
    if account_need_update(account):
        account = gvite_get_account(address)
        if account is None:
            app.logger.error(f'account {address} not found')
            return jsonify({
                'err': f'cannot find account {address}',
                'result': {}})
        quota = gvite_get_account_quota(address)
        if quota:
            account.update(quota)
        else:
            app.logger.error(f'account quota {address} not found')

        save_account_from_dict(account)
        account = db_get_account(address)

    return jsonify({
        'err': 'ok',
        'result': schema_account.dump(account)
    })


@bp_ledger.route('/get_accounts/<order>/<sort_field>/<int:page_idx>/<int:page_size>',  methods=('GET', 'POST'))
def get_accounts(order, sort_field, page_idx, page_size):
    if request.method == 'POST':
        pass

    accounts, count = db_get_accounts(order, sort_field, page_idx, page_size)

    result = {
        'err': 'ok',
        'count': count,
        'pageIdx': page_idx,
        'pageSize': page_size,
        'accounts': schema_account.dump(accounts, many=True)
    }

    return jsonify(result)


# search account by address prefix
@bp_ledger.route('/search_accounts/<address>/<order>/<sort_field>/<int:page_idx>/<int:page_size>',  methods=('GET', 'POST'))
def search_accounts(address, order, sort_field, page_idx, page_size):
    if request.method == 'POST':
        pass

    accounts, count = db_search_accounts(
        address, order, sort_field, page_idx, page_size)

    result = {
        'err': 'ok',
        'count': count,
        'pageIdx': page_idx,
        'pageSize': page_size,
        'accounts': schema_account.dump(accounts, many=True)
    }

    return jsonify(result)


@bp_ledger.route('/get_snapshot_blocks_by_address/<address>/<order>/<sort_field>/<int:page_idx>/<int:page_size>',  methods=('GET', 'POST'))
def get_snapshot_blocks_by_address(address, order, sort_field, page_idx, page_size):
    if request.method == 'POST':
        pass

    snapshot_blocks, count = db_get_snapshot_blocks_by_address(
        address, order, sort_field, page_idx, page_size)

    result = {
        'err': 'ok',
        'count': count,
        'pageIdx': page_idx,
        'pageSize': page_size,
        'snapshotBlocks': schema_snapshot_block.dump(snapshot_blocks, many=True)
    }

    return jsonify(result)


@bp_ledger.route('/get_snapshot_blocks/<order>/<sort_field>/<int:page_idx>/<int:page_size>',  methods=('GET', 'POST'))
def get_snapshots(order, sort_field, page_idx, page_size):
    if request.method == 'POST':
        pass

    snapshot_blocks, count = db_get_snapshot_blocks(
        order, sort_field, page_idx, page_size)

    result = {
        'err': 'ok',
        'count': count,
        'pageIdx': page_idx,
        'pageSize': page_size,
        'snapshotBlocks': schema_snapshot_block.dump(snapshot_blocks, many=True)
    }

    return jsonify(result)


@bp_ledger.route('/get_latest_snapshot_blocks/<int:page_size>',  methods=('GET', 'POST'))
def get_latest_snapshots(page_size):
    if request.method == 'POST':
        pass

    snapshot_blocks = db_get_latest_snapshot_blocks(page_size)

    result = {
        'err': 'ok',
        'count': page_size,
        'pageIdx': 0,
        'pageSize': page_size,
        'snapshotBlocks': schema_snapshot_block.dump(snapshot_blocks, many=True)
    }

    return jsonify(result)


@bp_ledger.route('/get_token_balanced_desc/<token_id>/<int:page_idx>/<int:page_size>',  methods=('GET', 'POST'))
def get_token_balanced_desc(token_id, page_idx, page_size):
    if request.method == 'POST':
        pass

    balances, count = da_get_token_balances_desc(token_id, page_idx, page_size)

    result = {
        'err': 'ok',
        'count': count,
        'pageIdx': page_idx,
        'pageSize': page_size,
        'balances': schema_balance.dump(balances, many=True)
    }

    return jsonify(result)

from datetime import date, datetime, timedelta
import logging
from flask import Blueprint
import click


from .models import db

bp_cli = Blueprint('manage', __name__)

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)-8s %(message)s')


@bp_cli.cli.command('create-tables')
def create_tables():
    print('creating tables')
    db.create_all()
    print('done')


@bp_cli.cli.command('download-snapshotblock')
@click.argument('height', required=True, type=int)
def download_snapshotblock(height):
    from .tasks.chain import download_snapshot_block
    print(f'download_snapshotblock {height}')
    snapshot_block = download_snapshot_block(height)
    print(f'downloaded snapshot block {snapshot_block}')


@bp_cli.cli.command('download-tokens')
def download_tokens():
    from .tasks.chain import download_tokens, copy_token
    print('download tokens')
    download_tokens()
    # copy VITEX token to old tokenID
    copy_token('tti_5649544520544f4b454e6e40', 'tti_000000000000000000004cfd')
    copy_token('tti_5649544520544f4b454e6e40', 'tti_2445f6e5cde8c2c70e446c83')
    print(f'done downloading tokens')


@bp_cli.cli.command('create-token')
@click.argument('token_id', required=True)
def create_token(token_id):
    from .tasks.chain import create_token_empty
    print(f'create token {token_id}')
    create_token_empty(token_id)
    print(f'done creating token')


@bp_cli.cli.command('copy-token')
@click.argument('src_token_id', required=True)
@click.argument('dest_token_id', required=True)
def create_token(src_token_id, dest_token_id):
    from .tasks.chain import copy_token
    print(f'copy token {src_token_id} to {dest_token_id}')
    copy_token(src_token_id, dest_token_id)
    print(f'done copying token {src_token_id} to {dest_token_id}')


@bp_cli.cli.command('download-sbp')
def download_sbps():
    from vitex_stats_server.tasks.chain import refresh_sbp_list
    print('downloading SBPs')
    refresh_sbp_list()
    print('downloaded SBPs')


@bp_cli.cli.command('download-account-block')
@click.argument('hash', required=True)
def download_account_block(hash):
    from .tasks.chain import download_account_block_by_hash
    print(f'download account block {hash}')
    download_account_block_by_hash(hash)
    print(f'done downloading account block {hash}')


@bp_cli.cli.command('show-account-block')
@click.argument('hash', required=True)
def download_account_block(hash):
    from .ledger.data_accessor import db_get_account_block_by_hash, gvite_get_account_block_by_hash, account_block_complete, schema_account_block_complete
    from .ledger.data_accessor import save_account_block_from_dict

    # fixed length hash, example:
    # fc648d40ce2d26316a1121d06ff94c76de1bb186de53a37e6cb039c801f9a954
    if len(hash) != 64:
        print(f'hash length must be 64, got {len(hash)}')
        return

    account_block = db_get_account_block_by_hash(hash)

    if account_block and account_block_complete(account_block):
        print(f'DB hit account block {hash}')
        print(schema_account_block_complete.dump(account_block))
        return

    account_block = gvite_get_account_block_by_hash(
        hash, deserialize=False)

    if account_block is None:
        print(f'cannot find account block #{hash}')
        return

    save_account_block_from_dict(account_block)
    print(f'completed account block to DB {hash}')

    account_block = db_get_account_block_by_hash(hash)

    print(schema_account_block_complete.dump(account_block))


@bp_cli.cli.command('download-account')
@click.argument('address', required=True)
def download_account_cli(address):
    from .tasks.chain import download_account
    print(f'download account {address}')
    download_account(address)
    print(f'done download account {address}')


@bp_cli.cli.command('update-account-last-transaction-timestamp')
@click.argument('address', required=True)
@click.argument('timestamp', required=True)
def update_account_last_transaction_timestamp(address, timestamp):
    from .tasks.chain import update_account_last_transaction_date
    print(
        f'update account {address} last transaction timestamp to {timestamp}')
    try:
        timestamp = int(timestamp)
    except ValueError:
        print(f'timestamp must be an integer, got: {timestamp}')
        return
    account = update_account_last_transaction_date(address, timestamp)
    if account:
        print(
            f'done update account {address} last transaction timestamp to {timestamp}')
    else:
        print(f'account {address} not found')


@bp_cli.cli.command('download-snapshot-block-by-height')
@click.argument('height', required=True)
def download_snapshot_block_by_height(height):
    from .tasks.chain import download_snapshot_block_by_height
    print(f'download snapshot block {height}')
    download_snapshot_block_by_height(height)
    print(f'done downloading snapshot block {height}')


@bp_cli.cli.command('update-sbp-reward')
@click.argument('date_str', required=True)
def update_sbp_reward(date_str):
    target_date = datetime.strptime(date_str, '%Y-%m-%d')
    timestamp = target_date.timestamp()
    from .tasks.chain import update_sbp_reward
    print(f'update SBP Reward for {target_date}')
    update_sbp_reward(timestamp)
    print(f'done updating SBP Reward for {target_date}')


@bp_cli.cli.command('update-daily-statistics')
@click.argument('datestr', required=True)
def update_daily_stats(datestr):
    from .tasks.chain import update_daily_statistics
    target_date = datetime.strptime(datestr, '%Y-%m-%d')
    print(f'update daily statistics for {target_date}')
    stat = update_daily_statistics(target_date)
    print(
        f'done updating daily statistics for {target_date}: transaction_count = {stat.transaction_count}')


@bp_cli.cli.command('update-daily-statistics-today')
def update_daily_stats_today():
    from .tasks.chain import update_daily_statistics
    target_date = date.today()
    print(f'update daily statistics for {target_date}')
    stat = update_daily_statistics(target_date)
    print(
        f'done updating daily statistics for {target_date}: transaction_count = {stat.transaction_count}')


@bp_cli.cli.command('launch-sync-daemon')
def launch_sync_daemon():
    print('Launching chain sync daemon')
    from .sync_daemon import sync_daemon_main
    sync_daemon_main()


@bp_cli.cli.command('launch-sync')
def launch_sync():
    print('Launching chain sync')
    from .sync_daemon import sync_loop
    sync_loop()


@bp_cli.cli.command('chunk-download-reset-progress')
def launch_sync():
    print(f'Resetting progress of chunk download')
    from .chunk_download_daemon import reset_progress
    reset_progress()
    print(f'Progress of chunk download reset')


@bp_cli.cli.command('chunk-download')
@click.argument('target_date_str', required=True)
def launch_sync(target_date_str):
    target_date = datetime.strptime(target_date_str, '%Y-%m-%d')
    print(f'Launching chunk download, target date: {target_date}')
    from .chunk_download_daemon import sync_loop
    sync_loop(target_date)


@bp_cli.cli.command('chunk-download-interval')
@click.argument('start_height', required=True)
@click.argument('end_height', required=True)
def launch_sync(start_height, end_height):
    start_height = int(start_height)
    end_height = int(end_height)
    print(f'Download chunks from {start_height} to {end_height}')
    from .chunk_download_daemon import download_chunk_interval
    download_chunk_interval(start_height, end_height)


@bp_cli.cli.command('chunk-download-daemon')
@click.argument('target_date_str', required=True)
def launch_sync(target_date_str):
    target_date = datetime.strptime(target_date_str, '%Y-%m-%d')
    print(f'Launching chunk download, target date: {target_date}')
    from .chunk_download_daemon import chunk_download_daemon_main
    chunk_download_daemon_main(target_date)


@bp_cli.cli.command('clean-db-after-date')
@click.argument('target_date_str', required=True)
def clean_db_after_date(target_date_str):
    target_date = datetime.strptime(target_date_str, '%Y-%m-%d')
    print(f'Cleaning transactions and snapshots after {target_date}')
    from .db_manage import delete_account_block_after_date
    delete_account_block_after_date(target_date)


@bp_cli.cli.command('clean-db-days-before')
@click.argument('days_before', required=True)
def clean_db_after_date(days_before):
    days_before = int(days_before)
    target_date = datetime.now() - timedelta(days=days_before)
    print(
        f'Cleaning transactions and snapshots after {target_date} ({days_before} days before today)')
    from .db_manage import delete_account_block_after_date
    delete_account_block_after_date(target_date)

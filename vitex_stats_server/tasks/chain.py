from datetime import date, datetime
import logging
from sqlalchemy.orm.session import make_transient
from vitex_stats_server.ledger.data_accessor import gvite_get_account, gvite_get_account_block_by_hash, gvite_get_snapshot_block, gvite_get_chunks, save_account_block_from_dict, save_account_from_dict, save_snapshot_block_dict
from vitex_stats_server.models import Account, SBPSchema, db, Token, TokenSchema
from vitex_stats_server.contract.data_accessor import get_sbp_reward_gvite, get_token_info_list_da, get_token_info_list_gvite, gvite_get_account_quota, save_sbp_reward
from vitex_stats_server.contract.data_accessor import db_save_sbp,  get_sbp_gvite, get_sbp_list_gvite
from sqlalchemy.exc import SQLAlchemyError, NoResultFound

from vitex_stats_server.statistic.data_accessor import get_statistic_daily_by_date


def download_snapshot_block_by_height(height, recursive=False):
    snapshot_block_dict = gvite_get_snapshot_block(height)
    logging.info(f'received snapshot block {snapshot_block_dict["hash"]}')

    snapshot_block = save_snapshot_block_dict(snapshot_block_dict)

    logging.info(f'saved snapshot block {snapshot_block.hash}')
    return snapshot_block


def save_account_block(src, default_timestamp, triggered_by=None):
    return save_account_block_from_dict(src, default_timestamp, triggered_by)


def download_tokens():

    tokenSchema = TokenSchema()
    logging.info('downloading tokens')
    pageIdx = 0
    pageSize = 10

    while True:
        token_reponse = get_token_info_list_gvite(pageIdx, pageSize)
        if token_reponse.get('err', '') != '':
            logging.error(
                f'download tokens error {token_reponse.get("err", "")}')
            break
        tokens = token_reponse.get('tokenInfoList', [])
        if len(tokens) == 0:
            break
        for token in tokens:
            logging.info(
                f'saving token {token.get("tokenName", "unknown token name")}')
            token = tokenSchema.load(token)

            q = db.session.query(Token).filter_by(token_id=token.token_id)
            try:
                existing_token = q.one()
            except NoResultFound:
                db.session.add(token)
            else:
                logging.info(
                    f'token {token.token_id} already exists, updating')
                db.session.merge(token)
            finally:
                try:
                    db.session.commit()
                except SQLAlchemyError as err:
                    logging.error(f'Fail to commit token {token.token_id}')
                    logging.error(f'SQLAlchemyError {err}')

                except Exception as err:
                    logging.error(f'Fail to commit token {token.token_id}')
                    logging.error(f'General Error {err}')

        pageIdx += 1

    logging.info('downloaded all tokens')


def create_token_empty(token_id):
    token = Token(token_id=token_id,
                  token_name=f'unknown',
                  token_symbol=f'UNKOWN',
                  total_supply=0,
                  decimals=18,
                  owner='',
                  is_reissuable=False,
                  max_supply=0,
                  is_owner_burn_only=False,
                  index=0)

    q = db.session.query(Token).filter_by(token_id=token.token_id)
    try:
        existing_token = q.one()
    except NoResultFound:
        db.session.add(token)
    else:
        logging.info(f'token {token.token_id} already exists, updating')
        db.session.merge(token)
    finally:
        try:
            db.session.commit()
        except SQLAlchemyError as err:
            logging.error(f'Fail to commit token {token.token_id}')
            logging.error(f'SQLAlchemyError {err}')

        except Exception as err:
            logging.error(f'Fail to commit token {token.token_id}')
            logging.error(f'General Error {err}')


def copy_token(src_token_id, dest_token_id):
    src_token = db.session.query(Token).get(src_token_id)
    if src_token is None:
        logging.error(f'cannot find token {src_token_id}')
        return

    db.session.expunge(src_token)
    make_transient(src_token)
    src_token.token_id = dest_token_id

    dest_token = db.session.query(Token).get(dest_token_id)
    if dest_token:
        db.session.merge(src_token)
    else:
        db.session.add(src_token)

    try:
        db.session.commit()
    except SQLAlchemyError as err:
        logging.error(f'Fail to commit token {src_token.token_id}')
        logging.error(f'SQLAlchemyError {err}')

    except Exception as err:
        logging.error(f'Fail to commit token {src_token.token_id}')
        logging.error(f'General Error {err}')


def rank_sbp(sbp_list):
    sbp_list.sort(key=lambda x: int(x['votes']), reverse=True)
    for i, sbp in enumerate(sbp_list):
        sbp['rank'] = i + 1


def refresh_sbp_list():
    sbp_schema = SBPSchema()
    logging.info('refreshing SBP list')
    sbp_list = get_sbp_list_gvite()
    rank_sbp(sbp_list)
    for sbp_item in sbp_list:
        sbp_raw = get_sbp_gvite(sbp_item['sbpName'])
        sbp = sbp_schema.load(sbp_raw)
        sbp.votes = sbp_item.get('votes', 0)
        sbp.rank = sbp_item.get('rank', 0)
        db_save_sbp(sbp)
    logging.info('done refreshing SBP list')


def download_account_block_by_hash(hash):
    logging.info(f'downloading account block {hash}')
    account_block = gvite_get_account_block_by_hash(hash, deserialize=False)
    if account_block is None:
        logging.error(f'account block {hash} not found')
        return
    save_account_block(account_block, account_block.get('timestamp', 0))
    logging.info(f'downloaded account block {hash}')
    return account_block


def download_account(hash):
    logging.info(f'downloading account {hash}')
    account = gvite_get_account(hash)
    if account is None:
        logging.error(f'account {hash} not found')
        return
    quota = gvite_get_account_quota(hash)
    if quota:
        account.update(quota)
    else:
        logging.error(f'account quota {hash} not found')
        return
    save_account_from_dict(account)
    logging.info(f'downloaded account {hash}')
    return account


def download_account_with_transaction_time(hash, timestamp: int):
    logging.info(f'downloading account {hash}')
    account = gvite_get_account(hash)
    if account is None:
        logging.error(f'account {hash} not found')
        return
    quota = gvite_get_account_quota(hash)
    if quota:
        account.update(quota)
    else:
        logging.error(f'account quota {hash} not found')
        return

    account.update({
        'lastTransactionDate': datetime.fromtimestamp(timestamp)
    })
    save_account_from_dict(account)
    logging.info(f'downloaded account {hash}')
    return account


def update_account_last_transaction_date(address, timestamp: int):
    new_datetime = datetime.fromtimestamp(timestamp)
    logging.info(
        f'updating account {address} last transaction date to {new_datetime}')
    try:
        account = db.session.query(Account).filter_by(address=address).one()
    except NoResultFound:
        logging.info(f'account {address} not found, downloading')
        download_account_with_transaction_time.delay(address, timestamp)
        return

    if account.last_transaction_date and new_datetime < account.last_transaction_date:
        logging.info(
            f'account {address} last transaction date is more recent than {new_datetime}')
        return account

    account.last_transaction_date = new_datetime

    try:
        db.session.commit()
    except SQLAlchemyError as err:
        logging.error(f'Fail to commit account {account.address}')
        logging.error(f'SQLAlchemyError {err}')

    except Exception as err:
        logging.error(f'Fail to commit account {account.address}')
        logging.error(f'General Error {err}')
    return account


def update_sbp_reward(timestamp: int):
    logging.info('updating sbp reward')
    sbp_rewards = get_sbp_reward_gvite(timestamp)
    save_sbp_reward(sbp_rewards)
    logging.info('done updating sbp reward')


def update_daily_statistics(target_date: date):
    return get_statistic_daily_by_date(target_date, refresh=True)

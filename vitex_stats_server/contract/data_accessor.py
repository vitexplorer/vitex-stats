from flask import current_app as app
from marshmallow import ValidationError
import requests

from sqlalchemy.exc import SQLAlchemyError, NoResultFound
from ..models import SBP, SBPReward, SBPRewardSchema, SBPSchema, SnapshotBlock, Token, TokenSchema, db

sbp_schema = SBPSchema()
sbp_reward_schema = SBPRewardSchema()
token_schema = TokenSchema()

TOKEN_SORT_FIELD = {
    'tokenId': Token.token_id,
    'tokenName': Token.token_name,
    'tokenSymbol': Token.token_symbol,
    'decimals': Token.decimals,
    'tokenSupply': Token.total_supply,
    'owner': Token.owner,
}

EMPTY_TOKEN_REPLACEMENT = {
    'tti_000000000000000000004cfd': 'tti_5649544520544f4b454e6e40',
}


def token_get_sort_criteria(order='desc', sort_field='tokenName'):
    result = TOKEN_SORT_FIELD.get(sort_field, Token.token_name)

    if order == 'desc':
        return result.desc()

    return result.asc()


def get_sbp_detail_da(name):

    sbp = db.session.query(SBP).get(name)
    return sbp


def get_sbp_list_da(active_only=True):

    sbps = db.session.query(SBP).order_by(SBP.rank)
    if active_only:
        sbps = sbps.filter(SBP.votes > 0)
    return sbps


def get_sbp_list_gvite():

    headers = {'content-type': 'application/json'}
    request_body = {
        "jsonrpc": "2.0",
        "id": 2,
        "method": "contract_getSBPVoteList",
        "params": None
    }

    resp_sbp_list = requests.post(
        app.config['URL_RPC'],  json=request_body, headers=headers)

    response = resp_sbp_list.json().get('result', [])

    return response


def get_sbp_gvite(name):
    headers = {'content-type': 'application/json'}
    request_body = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "contract_getSBP",
        "params": [name, ]
    }

    resp_sbp_list = requests.post(
        app.config['URL_RPC'],  json=request_body, headers=headers)

    response = resp_sbp_list.json().get('result', None)
    if response is None:
        app.logger.error(f'Fail to get SBP {name}')
        return None
    return response


def get_sbp_da(name):

    sbp = db.session.get(SBP, name)
    if sbp:
        return sbp

    sbp_raw = get_sbp_gvite(name)

    sbp = sbp_schema.load(sbp_raw)

    db.session.add(sbp)
    try:
        db.session.commit()
    except SQLAlchemyError as err:
        db.session.rollback()
        app.logger.error(
            f'fail to commit SBP {name}: SQLAlchemyError {err}')

    except Exception as err:
        db.session.rollback()
        app.logger.error(f'fail to commit SBP {name}: General Error {err}')

    return sbp


def db_save_sbp(sbp):
    q = db.session.query(SBP).filter_by(name=sbp.name)
    try:
        existing_sbp = q.one()
    except NoResultFound:
        db.session.add(sbp)
    else:
        app.logger.info(
            f'SBP {sbp.name} already exists, updating')
        db.session.merge(sbp)
    finally:
        try:
            db.session.commit()
        except SQLAlchemyError as err:
            db.session.rollback()
            app.logger.error(
                f'fail to commit SBP {sbp.name}: SQLAlchemyError {err}')

        except Exception as err:
            db.session.rollback()
            app.logger.error(
                f'fail to commit SBP {sbp.name}: General Error {err}')


def get_sbp_reward_gvite(timestamp):

    headers = {'content-type': 'application/json'}
    request_body = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "contract_getSBPRewardByTimestamp",
        "params": [int(timestamp)]
    }

    resp_sbp_reward = requests.post(
        app.config['URL_RPC'],  json=request_body, headers=headers)

    response = resp_sbp_reward.json().get('result', None)

    return response


def save_sbp_reward(sbp_reward_src):
    for sbp_name, sbp_reward_src in sbp_reward_src['rewardMap'].items():
        existing_sbp = get_sbp_da(sbp_name)
        if existing_sbp is None:
            app.logger.error(
                f'SBP {sbp_name} does not exist in DB and cannot fetch from gvite')
            continue

        sbp_reward_src['sbpName'] = sbp_name
        sbp_reward = sbp_reward_schema.load(sbp_reward_src)
        existing_sbp_reward = db.session.get(SBPReward, sbp_name)
        if existing_sbp_reward:
            db.session.merge(sbp_reward)
        else:
            db.session.add(sbp_reward)
        try:
            db.session.commit()
        except SQLAlchemyError as e:
            db.session.rollback()
            app.logger.error(
                f'fail to commit SBPReward for {sbp_name}: SQLAlchemyError {e}')
        except Exception as err:
            db.session.rollback()
            app.logger.error(
                f'fail to commit SBP {sbp_name}: General Error {err}')


def get_token_info_list_da(order, sort_field, page_idx=0, page_size=10):

    offset = page_idx * page_size

    sort_criteria = token_get_sort_criteria(order, sort_field)

    count = db.session.query(Token).count()

    tokens = db.session.query(Token).order_by(
        sort_criteria).offset(offset).limit(page_size)

    return tokens, count


def search_token_name_da(token_name, order, sort_field, page_idx=0, page_size=10):
    offset = page_idx * page_size

    sort_criteria = token_get_sort_criteria(order, sort_field)

    count = db.session.query(Token).filter(
        Token.token_name.ilike(f'%{token_name}%') | Token.token_symbol.ilike(f'%{token_name}%')).count()

    tokens = db.session.query(Token).filter(
        Token.token_name.ilike(f'%{token_name}%') | Token.token_symbol.ilike(f'%{token_name}%')).order_by(sort_criteria).offset(offset).limit(page_size)

    return tokens, count


def gvite_get_token_info(token_id):
    replacing_token_id = EMPTY_TOKEN_REPLACEMENT.get(token_id, None)
    if replacing_token_id:
        target_token_id = token_id
        token_id = replacing_token_id
    headers = {'content-type': 'application/json'}
    request_body = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "contract_getTokenInfoById",
        "params": [token_id]
    }

    try:
        resp_token_list = requests.post(
            app.config['URL_RPC'],  json=request_body, headers=headers)

        response = resp_token_list.json()['result']
        if replacing_token_id:
            response['tokenId'] = target_token_id
    except requests.Timeout:
        response = {'err': 'timeout'}

    return response


def db_save_token_info_dict(token_dict):
    try:
        token = token_schema.load(token_dict)
    except ValidationError as err:
        app.logger.error(
            f'Fail to load token: {err}, token_dict: {token_dict}')
        return None
    q = db.session.query(Token).filter_by(token_id=token.token_id)
    try:
        q.one()
    except NoResultFound:
        db.session.add(token)
    else:
        app.logger.info(
            f'Token {token.token_name} already exists, updating')
        db.session.merge(token)
    finally:
        try:
            db.session.commit()
        except SQLAlchemyError as err:
            db.session.rollback()
            app.logger.error(
                f'fail to commit Token {token.token_name}: SQLAlchemyError {err}')

        except Exception as err:
            db.session.rollback()
            app.logger.error(
                f'fail to commit Token {token.token_name}: General Error {err}')


def get_token_info_list_gvite(page_idx=0, page_size=10):
    headers = {'content-type': 'application/json'}
    request_body = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "contract_getTokenInfoList",
        "params": [page_idx, page_size]
    }

    try:
        resp_token_list = requests.post(
            app.config['URL_RPC'],  json=request_body, headers=headers)

        response = resp_token_list.json()['result']
    except requests.Timeout:
        response = {'err': 'timeout'}

    return response


def get_token_info_by_Id(id):

    try:
        token = db.session.query(Token).get(id)
    except NoResultFound:
        app.logger.error(f'token id {id} does not exist in DB')
        return None
    return token


def da_get_active_sbp(count):
    try:
        snapshot_blocks = db.session.query(SnapshotBlock).order_by(
            SnapshotBlock.height.desc()).limit(100).all()
        producer_adresses = [sb.producer for sb in snapshot_blocks]
        producers = db.session.query(SBP).filter(
            SBP.block_producing_address.in_(producer_adresses)).filter(SBP.votes > 0).limit(count)
    except NoResultFound:
        app.logger.error(f'cannot find {count} active SBPs')
        return None
    return producers


def gvite_get_account_quota(address):
    headers = {'content-type': 'application/json'}
    request_body = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "contract_getQuotaByAccount",
        "params": [address, ]
    }

    try:
        resp_account_quota = requests.post(
            app.config['URL_RPC'],  json=request_body, headers=headers)

        response = resp_account_quota.json().get('result')
    except requests.Timeout:
        response = {'err': 'timeout'}
    if response is None:
        app.logger.error(
            f'Fail to get account quota {address}, raw response: {response}')
        return None

    return response


def gvite_get_contract_info(address):
    headers = {'content-type': 'application/json'}
    request_body = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "contract_getContractInfo",
        "params": [address, ]
    }

    try:
        resp_contract = requests.post(
            app.config['URL_RPC'],  json=request_body, headers=headers)

        response = resp_contract.json().get('result')
    except requests.Timeout:
        response = {'err': 'timeout'}
    if response is None:
        app.logger.error(
            f'Fail to get contract info {address}, raw response: {response}')
        return None

    return response


def gvite_get_voted_sbp(address):
    headers = {'content-type': 'application/json'}
    request_body = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "contract_getVotedSBP",
        "params": [address, ]
    }

    try:
        resp_voted_sbp = requests.post(
            app.config['URL_RPC'],  json=request_body, headers=headers)
        response = resp_voted_sbp.json().get('result')
    except requests.Timeout:
        response = {'err': 'timeout'}
    if response is None:
        app.logger.error(
            f'Fail to get voted SBP for {address}, raw response: {response}')
        return None

    return response

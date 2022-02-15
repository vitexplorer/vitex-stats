from vitex_stats_server.models import SBPSchema, TokenSchema
from flask import request, jsonify, Blueprint
from .data_accessor import da_get_active_sbp, get_sbp_detail_da, get_sbp_list_da, get_token_info_list_da, get_token_info_by_Id, gvite_get_contract_info, gvite_get_voted_sbp, search_token_name_da

bp_contract = Blueprint('contract', __name__, url_prefix='/contract')

sbp_schema = SBPSchema()
token_schema = TokenSchema()


@bp_contract.route('/get_sbp_by_name/<name>', methods=['GET', ])
def get_sbp_by_name(name):
    if request.method != 'GET':
        return jsonify({'err': 'mothed not allowed'}), 405

    sbp = get_sbp_detail_da(name)

    if sbp:
        result = sbp_schema.dump(sbp)
        return jsonify(result)

    result = {'err': 'no sbp found'}
    return jsonify(result), 404


@bp_contract.route('/get_sbp_list', methods=['GET', ])
def get_sbp_list():
    if request.method != 'GET':
        return jsonify({'err': 'mothed not allowed'}), 405

    sbps = get_sbp_list_da(active_only=True)

    result = sbp_schema.dump(sbps, many=True)

    return jsonify(result)


def pack_token_info(token_info):

    return {
        'tokenId': token_info['tokenId'],
        'tokenName': token_info['tokenName'],
        'tokenSymbol': token_info['tokenSymbol'],
        'decimals': token_info['decimals'],
        'tokenSupply': token_info['totalSupply'],
        'owner': token_info['owner'],
    }


@bp_contract.route('/get_token_info_list/<order>/<sort_field>/<int:page_idx>/<int:page_size>', methods=['GET'])
def get_token_info_list_order(order, sort_field, page_idx, page_size):
    tokens, count = get_token_info_list_da(
        order, sort_field, page_idx, page_size)
    response = {
        'err': 'ok',
        'totalCount': count,
        'pageIdx': page_idx,
        'pageSize': page_size,
        'tokenInfoList': token_schema.dump(tokens, many=True),
    }

    return jsonify(response)


@bp_contract.route('/search_token_name/<name>/<order>/<sort_field>/<int:page_idx>/<int:page_size>', methods=['GET'])
def search_token_info_list_order(name, order, sort_field, page_idx, page_size):
    tokens, count = search_token_name_da(name,
                                         order, sort_field, page_idx, page_size)
    response = {
        'err': 'ok',
        'totalCount': count,
        'pageIdx': page_idx,
        'pageSize': page_size,
        'tokenInfoList': token_schema.dump(tokens, many=True),
    }

    return jsonify(response)


@bp_contract.route('/get_token_info_list/<int:page_idx>/<int:page_size>', methods=['GET'])
def get_token_info_list(page_idx, page_size):
    return get_token_info_list_order('asc', 'token_name', page_idx, page_size)


@bp_contract.route('/get_token_info/<token_id>', methods=['GET'])
def get_token_info(token_id):
    token = get_token_info_by_Id(token_id)
    result = token_schema.dump(token)

    return jsonify(result)


@bp_contract.route('/get_active_sbp/<count>', methods=['GET'])
def get_active_sbp(count):
    sbps = da_get_active_sbp(count)
    result = sbp_schema.dump(sbps, many=True)

    return jsonify(result)


@bp_contract.route('/get_contract_info/<address>', methods=['GET'])
def get_contract_info(address):
    contract = gvite_get_contract_info(address)
    if contract:
        result = {
            'err': 'ok',
            'contract': contract
        }
    else:
        result = {
            'err': 'not found',
            'contract': None
        }

    return jsonify(result)


@bp_contract.route('/get_voted_sbp/<address>', methods=['GET'])
def get_voted_sbp(address):
    voted_sbp = gvite_get_voted_sbp(address)
    if voted_sbp:
        result = {
            'err': 'ok',
            'sbp': voted_sbp
        }
    else:
        result = {
            'err': 'not found',
            'sbp': None
        }

    return jsonify(result)

from flask import request, jsonify, Blueprint
from flask import current_app as app
from datetime import datetime, timedelta

from .data_accessor import get_statistic_daily_by_date, statistic_daily_schema

bp_statistic = Blueprint('statistic', __name__, url_prefix='/statistic')


@bp_statistic.route('/get_daily_statistics/<start_date_str>/<end_date_str>', methods=('GET', 'POST'))
def get_account_block_by_hash(start_date_str, end_date_str):
    if request.method == 'POST':
        pass

    start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
    end_date = datetime.strptime(end_date_str, '%Y-%m-%d')

    if start_date > end_date:
        return jsonify({"error": f'start_date {start_date} > end_date {end_date}'})

    statistics_arr = []
    current_date = start_date
    while current_date <= end_date:
        daily_statistic = get_statistic_daily_by_date(
            current_date)
        if daily_statistic is None:
            continue
        statistics_arr.append(daily_statistic)
        current_date += timedelta(days=1)

    result = {
        'err': 'ok',
        'result': statistic_daily_schema.dump(statistics_arr, many=True)
    }
    return jsonify(result)

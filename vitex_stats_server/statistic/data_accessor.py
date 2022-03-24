from datetime import date, timedelta

from sqlalchemy.sql.functions import func
from sqlalchemy.orm.exc import NoResultFound
from ..models import SBPActivity, db, StatisticDaily, StatisticDailySchema, AccountBlock

from flask import current_app as app

statistic_daily_schema = StatisticDailySchema()


def get_statistic_daily_by_date(src_date: date, refresh=False):
    statistic_daily = db.session.query(StatisticDaily).get(src_date)
    if statistic_daily and not refresh:
        return statistic_daily

    timestamp_start = src_date.strftime("%s")
    timestamp_end = (src_date + timedelta(days=1)).strftime("%s")

    count = db.session.query(func.count(AccountBlock.hash)).filter(AccountBlock.timestamp >
                                                                   timestamp_start).filter(AccountBlock.timestamp < timestamp_end).scalar()
    statistic_daily = StatisticDaily(date=src_date, transaction_count=count)
    save_statistic_daily(statistic_daily, override=refresh)

    return statistic_daily


def save_statistic_daily(statistic_daily: StatisticDaily, override=False):
    existing_record = db.session.query(
        StatisticDaily).get(statistic_daily.date)

    if existing_record and not override:
        return existing_record
    elif existing_record and override:
        db.session.merge(statistic_daily)
    else:
        db.session.add(statistic_daily)

    db.session.commit()


def update_sbp_activity(producer_address, last_timestamp):
    try:
        sbp_activity = db.session.query(
            SBPActivity).filter_by(block_producing_address=producer_address).one()
        sbp_activity.last_timestamp = last_timestamp
        db.session.commit()
    except NoResultFound:
        sbp_activity = SBPActivity(
            block_producing_address=producer_address, last_timestamp=last_timestamp)
        db.session.add(sbp_activity)
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        app.logger.error(f'Error when update sbp activity: {e}')

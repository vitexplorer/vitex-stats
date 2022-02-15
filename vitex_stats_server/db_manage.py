import logging
from sqlalchemy import delete
from .models import db, AccountBlock
from sqlalchemy.exc import SQLAlchemyError


def delete_account_block_after_date(target_date):
    target_timestamp = target_date.timestamp()
    try:
        db.session.execute(delete(AccountBlock).where(
            AccountBlock.timestamp < target_timestamp).execution_options(synchronize_session=False))
    except SQLAlchemyError as err:
        db.session.rollback()
        logging.error(f'Fail to delete account blocks, SQLAlchemyError {err}')

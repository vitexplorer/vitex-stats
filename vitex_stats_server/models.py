from flask_sqlalchemy import SQLAlchemy
from flask_sqlalchemy.model import Model
from marshmallow import Schema, fields, EXCLUDE, post_load
from sqlalchemy.orm import relationship
from sqlalchemy.sql.functions import func
from sqlalchemy.sql.schema import ForeignKey

# definition at https://vite.wiki/api/rpc/common_models_v2.html#tokeninfo

db = SQLAlchemy()


class Token(db.Model):

    # TokenInfndex between 0-999. For token having the same symbol, sequential indexes will be allocated according to when the token is issued.
    token_id = db.Column('token_id', db.String(length=28), primary_key=True)
    token_name = db.Column('token_name', db.String(length=64))
    token_symbol = db.Column('token_symbol', db.String(length=64))
    total_supply = db.Column('total_supply', db.DECIMAL(128, 0))
    decimals = db.Column('decimals', db.Integer)
    owner = db.Column('owner', db.String(length=64))
    # Whether the token can be re-issued
    is_reissuable = db.Column('is_reissuable', db.Boolean)
    max_supply = db.Column('max_supply', db.DECIMAL(128, 0))
    # Whether the token can be burned by the owner only
    is_owner_burn_only = db.Column('owner_burn_only', db.Boolean)
    # For token having the same symbol, sequential indexes will be allocated according to when the token is issued
    index = db.Column('index', db.Integer)


class TokenSchema(Schema):
    class Meta:
        unknown = EXCLUDE

    token_id = fields.Str(data_key='tokenId', primary_key=True)
    token_name = fields.Str(data_key='tokenName')
    token_symbol = fields.Str(data_key='tokenSymbol')
    total_supply = fields.Integer(data_key='totalSupply')
    decimals = fields.Integer()
    owner = fields.Str()
    is_reissuable = fields.Boolean(data_key='isReIssuable')
    max_supply = fields.Integer(data_key='maxSupply')
    is_owner_burn_only = fields.Boolean(data_key='isOwnerBurnOnly')
    index = fields.Integer()

    @post_load
    def make_token(self, data, **kwargs):
        return Token(**data)


class AccountBlock(db.Model):

    block_type = db.Column('block_type', db.Integer, index=True)
    height = db.Column('height', db.Integer, index=True)
    hash = db.Column('hash', db.String(length=64), primary_key=True)
    previous_hash = db.Column('previous_hash', db.String(length=64))
    address = db.Column('address', db.String)
    public_key = db.Column('public_key', db.String)
    producer = db.Column('producer', db.String)
    from_address = db.Column('from_address', db.String, index=True)
    to_address = db.Column('to_address', db.String, index=True)
    send_block_hash = db.Column('send_block_hash', db.String)
    token_id = db.Column('token_id', ForeignKey('token.token_id'), index=True)
    token = relationship('Token')
    amount = db.Column('amount', db.DECIMAL(128, 0))

    fee = db.Column('fee', db.DECIMAL(128, 0))
    data = db.Column('data', db.String)
    difficulty = db.Column('difficulty', db.BigInteger)
    nonce = db.Column('nonce', db.String)
    signature = db.Column('signature', db.String)
    quota_by_stake = db.Column('quota_by_stake', db.Integer)
    total_quota = db.Column('total_quota', db.Integer)
    vm_log_hash = db.Column('vm_log_hash', db.String)

    triggered_send_block_list = relationship('AccountBlock')
    triggered_by_account_block_hash = db.Column(
        'triggered_by_account_block_hash', ForeignKey("account_block.hash"),  nullable=True, index=True)

    confirmations = db.Column('confirmations', db.Integer)
    first_snapshot_hash = db.Column('first_snapshot_hash', db.String)
    timestamp = db.Column('timestamp', db.Integer, index=True)
    receive_block_height = db.Column('receive_block_height', db.Integer)
    receive_block_hash = db.Column('receive_block_hash', db.String)


class AccountBlockSchema(Schema):
    class Meta:
        unknown = EXCLUDE

    block_type = fields.Integer(data_key='blockType')
    height = fields.Integer()
    hash = fields.Str()
    previous_hash = fields.Str(data_key='previousHash')
    address = fields.Str()
    public_key = fields.Str(data_key='publicKey', allow_none=True)
    producer = fields.Str()
    from_address = fields.Str(data_key='fromAddress')
    to_address = fields.Str(data_key='toAddress')
    send_block_hash = fields.Str(data_key='sendBlockHash')
    token_id = fields.Str(data_key='tokenId')
    token = fields.Nested(TokenSchema, exclude=('token_id',))
    amount = fields.Integer(allow_none=True)

    # token_info  field here, ignored for now

    fee = fields.Integer(allow_none=True)
    data = fields.Str(allow_none=True)
    difficulty = fields.Integer(allow_none=True)
    nonce = fields.Str(allow_none=True)
    signature = fields.Str(allow_none=True)
    quota_by_stake = fields.Integer(data_key='quotaByStake')
    total_quota = fields.Integer(data_key='totalQuota')
    vm_log_hash = fields.Str(data_key='vmlogHash')

    confirmations = fields.Integer()
    first_snapshot_hash = fields.Str(data_key='firstSnapshotHash')
    timestamp = fields.Integer()
    receive_block_height = fields.Integer(
        data_key='receiveBlockHeight', allow_none=True)
    receive_block_hash = fields.Str(
        data_key='receiveBlockHash', allow_none=True)

    @post_load
    def make_account_block(self, data, **kwargs):
        return AccountBlock(**data)


class CompleteAccountBlockSchema(AccountBlockSchema):
    triggered_send_block_list = fields.Nested(
        AccountBlockSchema, data_key='triggeredSendBlockList', allow_none=True, many=True)


class SBPReward(db.Model):
    sbp_name = db.Column('sbp_name', ForeignKey(
        'SBP.name'),  primary_key=True)
    block_producing_reward = db.Column(
        'block_producing_reward', db.DECIMAL(128, 0))
    voting_reward = db.Column('voting_reward', db.DECIMAL(128, 0))
    total_reward = db.Column('total_reward', db.DECIMAL(128, 0))
    produced_blocks = db.Column('produced_blocks', db.Integer)
    target_blocks = db.Column('target_blocks', db.Integer)
    all_reward_withdrawed = db.Column('all_reward_withdrawed', db.Boolean)


class SBPRewardSchema(Schema):
    class Meta:
        unknown = EXCLUDE

    sbp_name = fields.Str(data_key='sbpName')
    block_producing_reward = fields.Integer(
        data_key='blockProducingReward')
    voting_reward = fields.Integer(data_key='votingReward')
    total_reward = fields.Integer(data_key='totalReward')
    produced_blocks = fields.Integer(data_key='producedBlocks')
    target_blocks = fields.Integer(data_key='targetBlocks')
    all_reward_withdrawed = fields.Boolean(data_key='allRewardWithdrawed')

    @post_load
    def make_sbp_reward(self, data, **kwargs):
        return SBPReward(**data)


class SBP(db.Model):
    name = db.Column('name', db.String(length=64), primary_key=True)
    block_producing_address = db.Column(
        'block_producing_address', db.String(length=64))
    stake_address = db.Column('stake_address', db.String(length=64))
    stake_amount = db.Column('stake_amount', db.DECIMAL(64, 0))
    expiration_height = db.Column('expiration_height', db.Integer)
    expiration_time = db.Column('expiration_time', db.Integer)
    revoke_time = db.Column('revoke_time', db.Integer)
    votes = db.Column('votes', db.DECIMAL(64, 0))
    rank = db.Column('rank', db.Integer, nullable=True)

    reward = relationship("SBPReward", uselist=False)


class SBPSchema(Schema):
    class Meta:
        unknown = EXCLUDE

    name = fields.Str()
    block_producing_address = fields.Str(data_key='blockProducingAddress')
    stake_address = fields.Str(data_key='stakeAddress')
    stake_amount = fields.Integer(data_key='stakeAmount')
    expiration_height = fields.Integer(data_key='expirationHeight')
    expiration_time = fields.Integer(data_key='expirationTime')
    revoke_time = fields.Integer(data_key='revokeTime')
    votes = fields.Integer(data_key='votes', allow_none=True)
    rank = fields.Integer(data_key='rank', allow_none=True)

    reward = fields.Nested(SBPRewardSchema, data_key='reward',
                           allow_none=True, exclude=('sbp_name',))

    @post_load
    def make_sbp(self, data, **kwargs):
        return SBP(**data)


class Balance(db.Model):
    __tablename__ = 'balance'
    __table_args__ = (
        db.PrimaryKeyConstraint('account_address', 'token_id'),
    )

    account_address = db.Column('account_address', db.String(
        length=64), ForeignKey('account.address'), index=True)
    token_id = db.Column('token_id', ForeignKey('token.token_id'), index=True)
    token = relationship('Token')
    balance = db.Column('balance', db.DECIMAL(128, 0), index=True)
    account = relationship('Account')


class BalanceSchema(Schema):
    class Meta:
        unknown = EXCLUDE
    account_address = fields.Str(data_key='accountAddress')
    balance = fields.Integer(data_key='balance', allow_none=True)
    token_id = fields.Str(data_key='tokenId')
    token = fields.Nested(TokenSchema, data_key='tokenInfo')


class Account(db.Model):
    address = db.Column('address', db.String(length=64), primary_key=True)
    block_count = db.Column('block_count', db.Integer)
    balances = relationship('Balance')
    vite_balance = db.Column(
        'vite_balance', db.DECIMAL(64, 0), default=0, index=True)

    current_quota = db.Column('current_quota', db.Integer, default=0)
    max_quota = db.Column('max_quota', db.Integer, default=0)
    stake_amount = db.Column('stake_amount', db.DECIMAL(64, 0), default=0)

    last_modified = db.Column('last_modified', db.DateTime, server_default=func.now(
    ), onupdate=func.current_timestamp())

    last_transaction_date = db.Column(
        'last_transaction_date', db.DateTime, nullable=True, index=True)


class AccountSchema(Schema):
    class Meta:
        unknown = EXCLUDE

    address = fields.Str()
    block_count = fields.Integer(data_key='blockCount')
    balances = fields.Nested(BalanceSchema, many=True)
    vite_balance = fields.Integer(data_key='viteBalance', allow_none=True)

    current_quota = fields.Integer(data_key='currentQuota')
    max_quota = fields.Integer(data_key='maxQuota')
    stake_amount = fields.Integer(data_key='stakeAmount')

    last_modified = fields.DateTime(data_key='lastModified')

    last_transaction_date = fields.DateTime(
        data_key='lastTransactionDate', allow_none=True)

    @post_load
    def make_account(self, data, **kwargs):
        return Account(**data)


class AccountSchemaSimple(AccountSchema):
    class Meta:
        exclude = ('balances', 'vite_balance')


class StatisticDaily(db.Model):
    date = db.Column('date', db.Date, primary_key=True)
    transaction_count = db.Column('transaction_count', db.Integer, default=0)


class StatisticDailySchema(Schema):
    class Meta:
        unknown = EXCLUDE
    date = fields.Date(data_key='date')
    transaction_count = fields.Integer(data_key='transactionCount')


class SnapshotBlock(db.Model):
    producer = db.Column('producer', db.String(length=64), index=True)
    hash = db.Column('hash', db.String(length=64), primary_key=True)
    prev_hash = db.Column('prev_hash', db.String(length=64))
    height = db.Column('height', db.Integer, index=True)
    public_key = db.Column('public_key', db.String)
    signature = db.Column('signature', db.String)
    version = db.Column('version', db.Integer)
    timestamp = db.Column('timestamp', db.Integer)
    snapshot_data = relationship('SnapshotData')


class SnapshotData(db.Model):
    __tablename__ = 'snapshot_data'
    __table_args__ = (
        db.PrimaryKeyConstraint('account_address', 'snapshot_block_hash'),
    )
    account_address = db.Column('account_address', db.String(
        length=64), ForeignKey('account.address'), index=True)
    snapshot_block_hash = db.Column(
        'snapshot_block_hash', ForeignKey('snapshot_block.hash'), index=True)
    height = db.Column('height', db.Integer)
    hash = db.Column('hash', db.String(length=64))
    snapshot_block = relationship('SnapshotBlock', viewonly=True)


class SnapshotDataSchema(Schema):
    class Meta:
        unknown = EXCLUDE
    account_address = fields.Str(data_key='accountAddress')
    snapshot_block_hash = fields.Str(data_key='snapshotBlockHash')
    height = fields.Integer(data_key='height')
    hash = fields.Str(data_key='hash')


class SnapshotBlockSchema(Schema):
    class Meta:
        unknown = EXCLUDE
    producer = fields.Str(data_key='producer')
    hash = fields.Str(data_key='hash')
    prev_hash = fields.Str(data_key='prevHash')
    height = fields.Integer(data_key='height')
    public_key = fields.Str(data_key='publicKey')
    signature = fields.Str(data_key='signature')
    version = fields.Integer(data_key='version')
    timestamp = fields.Integer(data_key='timestamp')

    snapshot_data = fields.Nested(
        SnapshotDataSchema, data_key='snapshotData', many=True, allow_none=True)


class ConfigStatus(db.Model):
    key = db.Column('key', db.String(length=64), primary_key=True)
    value = db.Column('value', db.String(length=255))

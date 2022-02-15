from .ledger import bp_ledger
from .contract import bp_contract
from .statistic import bp_statistic
from .cli import bp_cli  # import cli after db to avoid circular dependency
import os

from flask import Flask
from flask_cors import CORS

from .models import db

CONFIG_ENV_VAR = "FLASK_CONFIG"


def create_app(test_config=None):
    # create and configure the app
    app = Flask(__name__, instance_relative_config=True)
    if test_config is None:
        # load config file when not testing
        app.config.from_envvar(CONFIG_ENV_VAR)
    else:
        # load the test config if passed in
        app.config.from_mapping(test_config)

    CORS(app)

    db.init_app(app)

    app.register_blueprint(bp_ledger)
    app.register_blueprint(bp_contract)
    app.register_blueprint(bp_statistic)
    app.register_blueprint(bp_cli)

    # ensure the instance folder exists
    try:
        os.makedirs(app.instance_path)
    except OSError:
        pass

    return app

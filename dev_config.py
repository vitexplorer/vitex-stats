# config for local development
# load by env var "FLASK_CONFIG"

DEBUG = True
TESTING = False
SECRET_KEY = 'secret'
URL_RPC = 'http://127.0.0.1:48132'
SQLALCHEMY_DATABASE_URI = 'postgresql://vitexweb:vitexweb@localhost/vitexweb'
SQLALCHEMY_TRACK_MODIFICATIONS = False
LOGLEVEL = 'INFO'
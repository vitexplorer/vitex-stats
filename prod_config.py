# config for rack server #1
# load by env var "FLASK_CONFIG"

DEBUG = False
TESTING = False
SECRET_KEY = 'rackserver#1'
URL_RPC = 'http://192.168.2.171:48132'
SQLALCHEMY_DATABASE_URI = 'postgresql://vitexweb:vitexweb@192.168.2.166/vitexweb'
SQLALCHEMY_TRACK_MODIFICATIONS = False
LOGLEVEL = 'WARNING'
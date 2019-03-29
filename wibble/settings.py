
SOURCE = {
    'TYPE':None,
    'CONNECTION_STRING':None,
    'TABLESPACE':None,
    'TABLES':(),
}

DEST = {
    'TYPE':None,
    'HOST': None,
    'USER': None,
    'PASSWORD': None,
    'EXCHANGE_PREFIX': None,
}

try:
    from local_settings import *
except ImportError:
    pass

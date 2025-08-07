from datetime import date, datetime
from pathlib import Path

from decouple import config

# Build paths inside the project like this: BASE_DIR / 'subdir'.
BASE_DIR = Path(__file__).resolve().parent.parent

# Database connection parameters
DATABASE = {
    'SERVER': config('DB_SERVER', default='localhost'),
    'DATABASE': config('DB_DATABASE', default=''),
    'SCHEMA': config('DB_SCHEMA', default=''),
    'USERNAME': config('DB_USERNAME', default=''),
    'PASSWORD': config('DB_PASSWORD', default=''),
    'DRIVER': config('DB_DRIVER', default=''),
    'TRUSTED_CONNECTION': config('DB_TRUSTED_CONNECTION', default=False, cast=bool),
}

# Debug mode
DEBUG = config('DEBUG', default=False, cast=bool)

# Logging configuration
LOG_DIR = 'logs'
LOG_ROOT_LEVEL = 'DEBUG'
LOG_CONSOLE_LEVEL = 'INFO'
LOG_INFO_FILE_ENABLED = True
LOG_INFO_FILENAME = 'app_info.log'
LOG_INFO_FILE_LEVEL = 'INFO'
LOG_ERROR_FILE_ENABLED = True
LOG_ERROR_FILENAME = 'app_error.log'
LOG_ERROR_FILE_LEVEL = 'ERROR'

# SFTP settings
SFTP_HOST = config('SFTP_HOST', default='localhost', cast=str)
SFTP_PORT = config('SFTP_PORT', default=22, cast=int)
SFTP_USER = config('SFTP_USER', default='', cast=str)
SFTP_PASSWORD = config('SFTP_PASSWORD', default='', cast=str)
SFTP_HOST_KEY = config('SFTP_HOST_KEY', default='', cast=str)

SFTP_UPLOAD_PATH = config('SFTP_UPLOAD_PATH', default='/', cast=str)
SFTP_DOWNLOAD_PATH = config('SFTP_DOWNLOAD_PATH', default='/', cast=str)

LOCAL_EXPORT_PATH = str(config('LOCAL_EXPORT_PATH', default='./export', cast=str))

# Sage X3 database table settings
DEFAULT_LEGACY_DATE = date(1753, 1, 1)
DEFAULT_LEGACY_DATETIME = datetime(1753, 1, 1)

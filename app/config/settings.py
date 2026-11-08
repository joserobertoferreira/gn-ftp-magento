from datetime import date, datetime
from pathlib import Path

from decouple import config

# Build paths inside the project like this: BASE_DIR / 'subdir'.
BASE_DIR = Path(__file__).resolve().parent.parent

# Database connection parameters
SERVER = str(config('DB_SERVER', default='localhost', cast=str))
DATABASE = str(config('DB_DATABASE', default='', cast=str))
SCHEMA = str(config('DB_SCHEMA', default='', cast=str))
USERNAME = str(config('DB_USERNAME', default='', cast=str))
PASSWORD = str(config('DB_PASSWORD', default='', cast=str))
PORT = int(config('DB_PORT', default=1433, cast=int))

# Debug mode
DEBUG = bool(config('DEBUG', default=False, cast=bool))

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

SFTP_SYNC_BASE_PATH = str(config('SFTP_SYNC_BASE_PATH', default='/', cast=str))

_template = str(config('LOCAL_EXPORT_PATH', cast=str))
LOCAL_EXPORT_PATH = _template.format(schema=SCHEMA)
_template = str(config('LOCAL_IMPORT_PATH', cast=str))
LOCAL_IMPORT_PATH = _template.format(schema=SCHEMA)
_template = str(config('LOCAL_ARCHIVE_PATH', cast=str))
LOCAL_ARCHIVE_PATH = _template.format(schema=SCHEMA)

# Network storage where a copy of the generated files is kept
STORAGE_ARCHIVE_PATH = str(config('STORAGE_ARCHIVE_PATH', cast=str))

# Network folder where a copy of the customer satisfaction email files is kept
EMAIL_ARCHIVE_PATH = str(config('EMAIL_ARCHIVE_PATH', cast=str))

# Sage X3 database table settings
DEFAULT_LEGACY_DATE = date(1753, 1, 1)
DEFAULT_LEGACY_DATETIME = datetime(1753, 1, 1)


def _task_schedule(prefix: str, start: str, end: str, interval_minutes: int) -> dict:
    """Lê do .env a janela e o intervalo de uma tarefa (variáveis <prefix>_START_TIME, etc.)."""
    return {
        'start': config(f'{prefix}_START_TIME', default=start, cast=str),
        'end': config(f'{prefix}_END_TIME', default=end, cast=str),
        'interval_minutes': config(f'{prefix}_INTERVAL_MINUTES', default=interval_minutes, cast=int),
    }


_customer_emails_time = config('CUSTOMER_EMAILS_SCHEDULE_TIME', default='06:00', cast=str)

# Schedule settings
SCHEDULING = {
    'SCHEDULE_ENABLED': config('SCHEDULE_ENABLED', default=True, cast=bool),
    # Allowed months (comma-separated list)
    # This will allow the script to run only during these months
    'SCHEDULE_MONTHS': config(
        'SCHEDULE_MONTHS', default='1,2,3,4,5,6,7,8,9,10,11,12', cast=lambda x: [int(m) for m in x.split(',')]
    ),
    # Execute immediately if the script is started within the allowed time
    'SCHEDULE_RUN_IMMEDIATELY': config('SCHEDULE_RUN_IMMEDIATELY', default=True, cast=bool),
    # Timetable of each task
    'TASKS': {
        'transfers': _task_schedule('SCHEDULE', '08:00', '18:00', 60),
        'stock': _task_schedule('STOCK_SCHEDULE', '08:00', '23:00', 60),
        'barcodes': _task_schedule('BARCODES_SCHEDULE', '08:00', '16:00', 240),
        'customer_emails': {'start': _customer_emails_time, 'end': _customer_emails_time, 'interval_minutes': 1440},
    },
}

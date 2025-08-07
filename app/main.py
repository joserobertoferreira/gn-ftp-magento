import logging

from app.config.logging import setup_logging

setup_logging()

logger = logging.getLogger(__name__)


def main():
    logger.info('Hello from magento!')


if __name__ == '__main__':
    main()

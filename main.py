import logging

from app.config.logging import setup_logging
from app.config.settings import SCHEDULING
from app.scheduler.scheduler import ScheduledTask, Scheduler, TaskSchedule
from app.services.file_handler import (
    sync_barcodes,
    sync_customer_emails,
    sync_local_folder_to_sftp,
    sync_sftp_to_local_folder,
    sync_stocks,
)


def run_file_transfers():
    """Transfere os ficheiros pendentes de e para o Magento"""
    main_logger = logging.getLogger(__name__)

    # Verifica se existem ficheiros para serem transferidos para o Magento
    if not sync_local_folder_to_sftp():
        main_logger.info('Problemas com a transferência de ficheiros para o Magento.')

    # Verifica se existem ficheiros para serem transferidos do Magento
    if not sync_sftp_to_local_folder():
        main_logger.info('Problemas com a transferência de ficheiros do Magento.')


def build_tasks() -> list[ScheduledTask]:
    """Define as rotinas da aplicação e os respetivos horários"""
    schedules = SCHEDULING['TASKS']

    return [
        ScheduledTask('Stock', sync_stocks, TaskSchedule(**schedules['stock'])),
        ScheduledTask('Códigos de barras', sync_barcodes, TaskSchedule(**schedules['barcodes'])),
        ScheduledTask('Emails de satisfação', sync_customer_emails, TaskSchedule(**schedules['customer_emails'])),
        ScheduledTask(
            'Transferência de ficheiros',
            run_file_transfers,
            TaskSchedule(**schedules['transfers']),
            always_in_single_run=True,
        ),
    ]


def main():
    setup_logging()

    main_logger = logging.getLogger(__name__)
    main_logger.info('Aplicação iniciada.')

    scheduler = Scheduler(build_tasks(), SCHEDULING)

    # Verifica se deve rodar em modo agendado ou uma única vez
    if SCHEDULING['SCHEDULE_ENABLED']:
        main_logger.info('Iniciar em modo agendado...')
        scheduler.start()
    else:
        main_logger.info('Modo agendado desativado. Executando uma vez...')
        scheduler.run_once()


if __name__ == '__main__':
    main()

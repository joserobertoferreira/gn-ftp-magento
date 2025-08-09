import logging
from datetime import datetime

from app.config.logging import setup_logging
from app.config.settings import SCHEDULING
from app.scheduler.scheduler import Scheduler
from app.services.file_handler import sync_local_folder_to_sftp, sync_sftp_to_local_folder, sync_stocks


def run_synchronization():
    """Função que executa a sincronização para todos os clientes"""
    main_logger = logging.getLogger(__name__)
    main_logger.info('Execução iniciada.')

    # Sincroniza os dados de stock
    execution_time = datetime.now()

    if execution_time.minute == 0:
        if not sync_stocks():
            main_logger.info('Problemas com a sincronização de dados de stock.')

    # # Verifica se existem ficheiros para serem transferidos para o Magento
    if not sync_local_folder_to_sftp():
        main_logger.info('Problemas com a transferência de ficheiros para o Magento.')

    # Verifica se existem ficheiros para serem transferidos do Magento
    if not sync_sftp_to_local_folder():
        main_logger.info('Problemas com a transferência de ficheiros do Magento.')

    main_logger.info('Execução concluída.')


def main():
    setup_logging()

    main_logger = logging.getLogger(__name__)
    main_logger.info('Aplicação iniciada.')

    # Verifica se deve rodar em modo agendado ou uma única vez
    if SCHEDULING['SCHEDULE_ENABLED']:
        main_logger.info('Iniciar em modo agendado...')
        scheduler = Scheduler(run_synchronization, SCHEDULING)
        scheduler.start()
    else:
        main_logger.info('Modo agendado desativado. Executando uma vez...')
        run_synchronization()


if __name__ == '__main__':
    main()

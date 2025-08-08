import logging

from app.config.logging import setup_logging
from app.config.settings import SCHEDULING
from app.scheduler.scheduler import Scheduler
from app.services.file_handler import sync_local_folder_to_sftp


def run_synchronization():
    """Função que executa a sincronização para todos os clientes"""
    main_logger = logging.getLogger(__name__)
    main_logger.info('Execução iniciada.')

    # Verifica se existem ficheiros para serem transferidos para o Magento
    if sync_local_folder_to_sftp():
        main_logger.info('Ficheiros transferidos com sucesso.')
    else:
        main_logger.warning('Nenhum ficheiro a transferir.')

    main_logger.info('Execução concluída.')


def main():
    setup_logging()

    main_logger = logging.getLogger(__name__)
    main_logger.info('Aplicação iniciada.')

    # Verifica se deve rodar em modo agendado ou uma única vez
    if SCHEDULING['SCHEDULE_ENABLED']:
        main_logger.info('Iniciando em modo agendado...')
        scheduler = Scheduler(run_synchronization, SCHEDULING)
        scheduler.start()
    else:
        main_logger.info('Modo agendado desativado. Executando uma vez...')
        run_synchronization()


if __name__ == '__main__':
    main()

import logging
import re
import shutil
from pathlib import Path
from typing import Optional, Pattern

from app.config import settings
from app.database.manager import DatabaseManager
from app.ftp.manager import SftpManager


def _get_remote_destination(filename: str, rules: dict[Pattern, str]) -> Optional[str]:
    """
    Determina a subpasta remota para um ficheiro com base em um conjunto de regras.
    Retorna a subpasta ou None se nenhuma regra corresponder.
    """
    for pattern, dest_template in rules.items():
        match = pattern.search(filename)
        if match:
            if '{code}' in dest_template:
                code = match.group(1)
                return dest_template.format(code=code)
            else:
                return dest_template
    return None


def _archive_processed_files(filenames: list[str], source_dir: Path, archive_dir: Path) -> None:
    """
    Move uma lista de ficheiros de um diretório de origem para um de arquivamento.
    """
    if not filenames:
        logging.info('Nenhum ficheiro para arquivar.')
        return

    logging.info('Arquivar ficheiros processados com sucesso...')

    for filename in filenames:
        source_path = source_dir / filename
        destination_path = archive_dir / filename
        try:
            # Verifica se o ficheiro ainda existe antes de mover
            if source_path.exists():
                shutil.move(str(source_path), str(destination_path))
                logging.info(f"Ficheiro '{filename}' movido para '{archive_dir}'.")
            else:
                logging.warning(f"Ficheiro '{filename}' não encontrado em '{source_dir}' para arquivamento.")
        except (shutil.Error, OSError) as e:
            logging.error(f"Erro ao mover o ficheiro '{filename}': {e}")


def _process_folder(
    sftp_client, source_dir: Path, archive_dir: Path, routing_rules: dict[Pattern, str], base_remote_path: str
) -> None:
    """
    Processa todos os ficheiros em um único diretório de origem, aplica as regras de roteamento,
    faz o upload e arquiva os ficheiros bem-sucedidos.
    """
    if not source_dir.is_dir():
        logging.warning(f'Diretório de origem não encontrado, pular: {source_dir}')
        return

    logging.info(f'Verificar a pasta: {source_dir}')
    try:
        all_files = [f for f in source_dir.iterdir() if f.is_file()]
    except OSError as e:
        logging.error(f"Não foi possível ler o diretório '{source_dir}': {e}")
        return

    if not all_files:
        logging.info(f'Nenhum ficheiro encontrado em {source_dir}.')
        return

    files_to_archive = []
    for local_filepath in all_files:
        filename = local_filepath.name
        remote_subfolder = _get_remote_destination(filename, routing_rules)

        if not remote_subfolder:
            continue

        remote_path = f'{base_remote_path}/{remote_subfolder}/out/{filename}'

        logging.info(f"Enviar '{filename}' de '{source_dir}' para '{remote_path}'...")
        success = sftp_client.upload_file(str(local_filepath), remote_path)

        if success:
            logging.info(f"Envio de '{filename}' bem-sucedido.")
            files_to_archive.append(filename)
        else:
            logging.error(f"Falha ao enviar o ficheiro '{filename}'.")

    # Arquiva os ficheiros desta pasta que foram processados com sucesso
    _archive_processed_files(files_to_archive, source_dir, archive_dir)


def _download_files_from_remote_folder(sftp_client, remote_folder_path: Path, local_download_dir: Path):
    """
    Baixa todos os ficheiros de uma pasta remota específica e os remove após o sucesso.
    """
    remote_folder_str = str(remote_folder_path).replace('\\', '/')

    files_in_folder = sftp_client.list_files(remote_folder_str)

    if not files_in_folder:
        logging.info(f"Nenhum ficheiro encontrado em '{remote_folder_str}'.")
        return

    logging.info(f"Encontrados {len(files_in_folder)} ficheiros em '{remote_folder_str}'.")

    for filename in files_in_folder:
        remote_filepath = f'{remote_folder_str}/{filename}'
        local_filepath = local_download_dir / filename

        logging.info(f"Baixar '{remote_filepath}' para '{local_filepath}'...")

        # Tenta baixar o ficheiro
        download_success = sftp_client.download_file(remote_filepath, str(local_filepath))

        # Se o download for bem-sucedido, remove o ficheiro remoto
        if download_success:
            logging.info(f"Download de '{filename}' bem-sucedido. Remover ficheiro remoto.")
            sftp_client.delete_file(remote_filepath)
        else:
            logging.error(f"Falha no download de '{filename}'. O ficheiro não será removido do SFTP.")


def sync_local_folder_to_sftp() -> bool:
    """
    Orquestra a sincronização de ficheiros de uma pasta local para o SFTP,
    seguindo regras de roteamento e arquivar os ficheiros processados.
    """
    logging.info('Envio de ficheiros para o SFTP iniciado.')

    local_sync_path = Path(settings.LOCAL_EXPORT_PATH)
    local_archive_path = Path(settings.LOCAL_ARCHIVE_PATH)
    base_remote_path = settings.SFTP_SYNC_BASE_PATH

    local_sync_path.mkdir(parents=True, exist_ok=True)
    local_archive_path.mkdir(parents=True, exist_ok=True)

    try:
        all_files = [f for f in local_sync_path.iterdir() if f.is_file()]
    except OSError as e:
        logging.error(f"Não foi possível ler o diretório '{local_sync_path}': {e}")
        return False

    if not all_files:
        logging.info('Nenhum ficheiro encontrado na pasta de sincronização.')
        return False

    # Regras para a pasta principal
    main_folder_rules = {
        re.compile(r'EDIEE(\d{2})', re.IGNORECASE): 'E{code}',
        re.compile(r'EDISE(\d{2})', re.IGNORECASE): 'E{code}',
        re.compile(r'PRODE(\d{2})', re.IGNORECASE): 'E{code}',
    }

    # Regras para a pasta de devolução
    devolucao_folder_path = local_sync_path / 'devolucao'
    devolucao_folder_rules = {re.compile(r'RECE(\d{2})', re.IGNORECASE): 'E{code}'}

    try:
        with SftpManager() as sftp:
            _process_folder(sftp, local_sync_path, local_archive_path, main_folder_rules, base_remote_path)
            _process_folder(sftp, devolucao_folder_path, local_archive_path, devolucao_folder_rules, base_remote_path)
    except Exception as e:
        logging.error(f'Falha crítica na conexão ou operação SFTP: {e}')
        return False

    logging.info('Envio de ficheiros para o SFTP concluído.')

    return True


def sync_sftp_to_local_folder() -> bool:
    """
    Busca ficheiros de pastas específicas no SFTP, baixando-os localmente.
    """
    logging.info('Iniciar tarefa de download de ficheiros do SFTP.')

    # 1. Buscar a lista de lojas do banco de dados
    logging.info('Buscar lista de lojas no banco de dados...')

    base_sql = f'SELECT SALFCY_0 FROM {settings.SCHEMA}.ZLOJAS'

    folder_codes = []
    try:
        with DatabaseManager() as db:
            results = db.fetch_data(query_base=base_sql, order_by_fields=['SALFCY_0'])
            if results:
                # Extrai apenas o valor da coluna 'SALFCY_0' de cada dicionário
                folder_codes = [row['SALFCY_0'] for row in results]
    except Exception as e:
        logging.error(f'Falha ao buscar lista de lojas do banco de dados: {e}')
        return False

    if not folder_codes:
        logging.warning('Nenhuma loja ativa encontrada no banco de dados. Tarefa encerrada.')
        return False

    logging.info(f'Lojas a serem verificadas: {folder_codes}')

    # 2. Processar cada loja no SFTP
    base_remote_path = Path(settings.SFTP_SYNC_BASE_PATH)

    # Diretório local de destino para os downloads
    local_download_dir = Path(settings.LOCAL_IMPORT_PATH)
    local_download_dir.mkdir(parents=True, exist_ok=True)

    try:
        with SftpManager() as sftp:
            for code in folder_codes:
                # Define as subpastas a serem verificadas para este código
                subfolders_to_check = [base_remote_path / code / 'in', base_remote_path / code / 'recolhas' / 'in']

                for remote_folder in subfolders_to_check:
                    _download_files_from_remote_folder(sftp, remote_folder, local_download_dir)
    except Exception as e:
        logging.error(f'Falha crítica durante a tarefa de download: {e}')
        return False

    logging.info('Tarefa de download de ficheiros do SFTP concluída.')

    return True

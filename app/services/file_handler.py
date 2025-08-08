import logging
import re
import shutil
from pathlib import Path
from typing import Dict, List, Optional, Pattern

from app.config import settings
from app.ftp.manager import SftpManager


def _get_remote_destination(filename: str, rules: Dict[Pattern, str]) -> Optional[str]:
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


def _archive_processed_files(filenames: List[str], source_dir: Path, archive_dir: Path) -> None:
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
    sftp_client, source_dir: Path, archive_dir: Path, routing_rules: Dict[Pattern, str], base_remote_path: str
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


def sync_local_folder_to_sftp() -> bool:
    """
    Orquestra a sincronização de ficheiros de uma pasta local para o SFTP,
    seguindo regras de roteamento e arquivar os ficheiros processados.
    """
    logging.info('Iniciar a tarefa de sincronização com roteamento e arquivamento.')

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

    logging.info('Tarefa de sincronização concluída.')

    return True

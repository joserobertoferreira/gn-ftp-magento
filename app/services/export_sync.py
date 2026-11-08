import logging
import shutil
from pathlib import Path
from typing import Optional

from app.config import settings
from app.database.manager import DatabaseManager
from app.ftp.manager import SftpManager


class ExportSync:
    """
    Base das rotinas que extraem dados do X3, geram ficheiros e os enviam via SFTP.
    """

    def __init__(self):
        """
        Inicializa os gerenciadores que serão usados. A conexão em si ainda não é aberta aqui.
        """
        self.db_manager: Optional[DatabaseManager] = None
        self.sftp_manager: Optional[SftpManager] = None
        self.export_dir = Path(settings.LOCAL_EXPORT_PATH)
        self.archive_dir = Path(settings.LOCAL_ARCHIVE_PATH)
        self.storage_dir = Path(settings.STORAGE_ARCHIVE_PATH)
        self.base_remote_path = settings.SFTP_SYNC_BASE_PATH

    def __enter__(self):
        """
        Gerenciador de contexto: Abre as conexões necessárias (DB e SFTP).
        """

        # Instancia e entra no contexto do DatabaseManager
        self.db_manager = DatabaseManager()
        self.db_manager.__enter__()  # Abre a conexão com o banco

        # Instancia e entra no contexto do SftpManager
        self.sftp_manager = SftpManager()
        self.sftp_manager.__enter__()  # Abre a conexão SFTP

        # Garante que os diretórios locais existem
        self.export_dir.mkdir(parents=True, exist_ok=True)
        self.archive_dir.mkdir(parents=True, exist_ok=True)

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Gerenciador de contexto: Fecha as conexões.
        """
        if self.sftp_manager:
            self.sftp_manager.__exit__(exc_type, exc_val, exc_tb)

        if self.db_manager:
            self.db_manager.__exit__(exc_type, exc_val, exc_tb)

    def _upload_and_archive(
        self, sftp_manager: SftpManager, file_path: Path, remote_folder: str, storage_subfolder: Path
    ) -> bool:
        """
        Envia um ficheiro para o SFTP e, se o envio tiver sucesso, copia-o para o storage e arquiva-o.
        """
        remote_path = f'{self.base_remote_path}/{remote_folder}/{file_path.name}'

        if not sftp_manager.upload_file(str(file_path), remote_path):
            return False

        self._copy_to_storage(file_path, storage_subfolder)

        shutil.move(str(file_path), str(self.archive_dir / file_path.name))
        logging.info(f'Ficheiro {file_path.name} enviado e arquivado com sucesso.')

        return True

    def _copy_to_storage(self, file_path: Path, storage_subfolder: Path) -> None:
        """
        Copia um ficheiro para o storage. Uma falha é registada mas não interrompe o arquivo,
        pois o ficheiro já foi enviado e fica disponível em LOCAL_ARCHIVE_PATH.
        """
        destination_dir = self.storage_dir / storage_subfolder

        try:
            destination_dir.mkdir(parents=True, exist_ok=True)
            shutil.copy2(file_path, destination_dir / file_path.name)
        except OSError as e:
            logging.error(f"Falha ao copiar '{file_path.name}' para o storage '{destination_dir}': {e}")

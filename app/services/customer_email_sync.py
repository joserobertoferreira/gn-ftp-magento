import logging
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import pandas as pd
from sqlalchemy import text

from app.config import settings
from app.services.export_sync import ExportSync

# Valor de LOJA_0 -> (prefixo do ficheiro, subpasta em EMAIL_ARCHIVE_PATH)
FILE_TARGETS = {0: ('MS', 'Site'), 1: ('ML', 'Lojas')}

FILE_COLUMNS = ['NUM_0', 'INVDAT_0', 'BPCINV_0', 'BPCNAM_0', 'WEB_0']


class CustomerEmailSync(ExportSync):
    """
    Orquestra a extração dos emails para satisfação de clientes, a geração dos ficheiros e o seu envio via SFTP.
    """

    def __init__(self):
        super().__init__()
        self.storage_dir = Path(settings.EMAIL_ARCHIVE_PATH)

    def fetch_email_data(self, reference_date: date) -> Optional[pd.DataFrame]:
        """
        Busca os registos criados na data indicada. Devolve None em caso de falha.
        """
        if not self.db_manager or not self.db_manager.connection:
            logging.error('Conexão com o banco de dados não está disponível.')
            return None

        logging.info(f'Buscar emails de satisfação de {reference_date:%d/%m/%Y}...')

        query = text(f'SELECT * FROM {settings.SCHEMA}.YVWMAIRES WHERE CREDAT_0 = :CREDAT_0')

        try:
            df = pd.read_sql_query(query, self.db_manager.connection, params={'CREDAT_0': f'{reference_date:%Y%m%d}'})
        except Exception as e:
            logging.error(f'Falha ao buscar emails de satisfação do X3: {e}')
            return None

        logging.info(f'Dados carregados com sucesso. {len(df)} linhas encontradas.')

        for column in ('BPCNAM_0', 'WEB_0'):
            df[column] = df[column].str.strip()

        return df

    def generate_files(self, df: pd.DataFrame, reference_date: date) -> list[tuple[Path, str]]:
        """
        Gera um ficheiro por valor de LOJA_0. Devolve os ficheiros e a respetiva subpasta de arquivo.
        """
        generated_files = []

        # Data da extração com a hora do processamento
        timestamp = f'{reference_date:%y%m%d}{datetime.now():%H%M%S}'

        for store_flag, (prefix, storage_subfolder) in FILE_TARGETS.items():
            group = df[df['LOJA_0'] == store_flag]

            if group.empty:
                logging.info(f'Sem registos para o ficheiro {prefix}. Nenhum ficheiro será gerado.')
                continue

            filepath = self.archive_dir / f'{prefix}_{timestamp}.txt'
            group.to_csv(
                filepath,
                columns=FILE_COLUMNS,
                sep=';',
                index=False,
                encoding='utf-8',
                header=False,
                date_format='%Y%m%d',
            )

            logging.info(f'Ficheiro de emails de satisfação gerado: {filepath} ({len(group)} linhas)')
            generated_files.append((filepath, storage_subfolder))

        return generated_files

    def upload_files(self, files: list[tuple[Path, str]]) -> bool:
        """
        Envia os ficheiros para o SFTP, copia-os para o arquivo de emails e arquiva-os localmente.
        """
        if not self.sftp_manager:
            logging.error('SFTP Manager não está disponível. Abortar upload.')
            return False

        results = [
            self._upload_and_archive(self.sftp_manager, file_path, 'Emails', Path(storage_subfolder))
            for file_path, storage_subfolder in files
        ]

        return all(results)

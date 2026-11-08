import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd

from app.config import settings
from app.services.export_sync import ExportSync


class BarcodeSync(ExportSync):
    """
    Orquestra a extração dos códigos de barras do X3, a geração do ficheiro e o seu envio via SFTP.
    """

    def fetch_barcode_data(self) -> Optional[pd.DataFrame]:
        """
        Busca os códigos de barras do X3. Devolve None em caso de falha.
        """
        if not self.db_manager or not self.db_manager.connection:
            logging.error('Conexão com o banco de dados não está disponível.')
            return None

        logging.info('Buscar códigos de barras do X3...')

        query = f"""
        SELECT i.TSICOD_0,i.TSICOD_1,i.ITMREF_0,i.ZITMDES_0,b.EANCOD_0,b.PCUSTUCOE_0
        FROM {settings.SCHEMA}.ITMMASTER i
        CROSS APPLY (
            VALUES
                (i.EANCOD_0, 1),
                (i.ZEANCOD_0, CONVERT(int,i.PCUSTUCOE_0)),
                (i.ZEANCOD_1, CONVERT(int,i.PCUSTUCOE_1)),
                (i.ZEANCOD_2, CONVERT(int,i.PCUSTUCOE_2)),
                (i.ZEANCOD_3, CONVERT(int,i.PCUSTUCOE_3))
            ) b (EANCOD_0, PCUSTUCOE_0)
        WHERE i.STOMGTCOD_0>1
        AND b.EANCOD_0<>''
        ORDER BY i.TSICOD_0,i.ITMREF_0
        """

        try:
            df = pd.read_sql_query(query, self.db_manager.connection)
        except Exception as e:
            logging.error(f'Falha ao buscar códigos de barras do X3: {e}')
            return None

        logging.info(f'Dados carregados com sucesso. {len(df)} linhas encontradas.')

        df['ZITMDES_0'] = df['ZITMDES_0'].str.strip()

        return df

    def generate_barcode_file(self, df: pd.DataFrame) -> Optional[Path]:
        """
        Gera o ficheiro de texto com os códigos de barras.
        """
        if df.empty:
            logging.warning('DataFrame vazio. Nenhum ficheiro será criado.')
            return None

        filename = f'EANS{datetime.now().strftime("%y%m%d%H%M%S")}.txt'
        filepath = self.export_dir / filename

        columns = ['ITMREF_0', 'ZITMDES_0', 'TSICOD_0', 'TSICOD_1', 'EANCOD_0', 'PCUSTUCOE_0']
        df.to_csv(filepath, columns=columns, sep=';', index=False, encoding='utf-8', header=False)

        logging.info(f'Ficheiro de códigos de barras gerado: {filepath}')

        return filepath

    def upload_file(self, barcode_file: Path) -> bool:
        """
        Envia o ficheiro para o SFTP, copia-o para o storage e arquiva-o.
        """
        if not self.sftp_manager:
            logging.error('SFTP Manager não está disponível. Abortar upload.')
            return False

        return self._upload_and_archive(self.sftp_manager, barcode_file, 'EANS', Path('STOCK') / 'EANS')

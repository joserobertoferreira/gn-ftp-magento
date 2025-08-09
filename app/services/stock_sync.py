import logging
import shutil
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd

from app.config import settings
from app.database.manager import DatabaseManager
from app.ftp.manager import SftpManager


class StockSync:
    """
    Orquestra a sincronização de dados de stock, desde a extração
    do banco até a geração e envio de ficheiros via SFTP.
    """

    def __init__(self):
        """
        Inicializa os gerenciadores que serão usados. A conexão em si ainda não é aberta aqui.
        """
        self.db_manager: Optional[DatabaseManager] = None
        self.sftp_manager: Optional[SftpManager] = None
        self.export_dir = Path(settings.LOCAL_EXPORT_PATH)
        self.archive_dir = Path(settings.LOCAL_ARCHIVE_PATH)
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

    def fetch_stock_data(self) -> Optional[pd.DataFrame]:
        """
        Busca os dados de stock do X3 e os retorna como um DataFrame do Pandas.
        """
        if not self.db_manager or not self.db_manager.connection:
            logging.error('Conexão com o banco de dados não está disponível.')
            return None

        logging.info('Buscar dados de stock do X3...')

        try:
            # Busca o intervalo de armazéns
            stock_range = self._get_stock_range()

            if stock_range:
                query = f"""
                SELECT ITV.STOFCY_0,ITV.ITMREF_0,ITM.ZITMDES_0,
                CONVERT(int,(ITV.PHYSTO_0-ITV.PHYALL_0-ITV.GLOALL_0)) AS AVLSTO_0,
                PRI.P1PVP_0,PRI.P2PVP_0,PRI.P2_0
                FROM {settings.SCHEMA}.ITMMVT ITV
                INNER JOIN {settings.SCHEMA}.ITMMASTER ITM ON ITM.ITMREF_0=ITV.ITMREF_0 AND ITM.ITMSTA_0=1
                INNER JOIN ZITMPRI PRI ON PRI.ITMREF_0=ITV.ITMREF_0
                INNER JOIN FACILITY FCY ON FCY.FCY_0=ITV.STOFCY_0 AND FCY.LEGCPY_0='GN' AND FCY.WRHFLG_0=2
                 AND FCY.FCY_0<>'E30'
                WHERE ITV.STOFCY_0 BETWEEN '{stock_range[0]}' AND '{stock_range[1]}'
                ORDER BY ITV.ITMREF_0,ITV.STOFCY_0
                """

                df = pd.read_sql_query(query, self.db_manager.connection)

                logging.info(f'Dados carregados com sucesso. {len(df)} linhas encontradas.')

                if 'ZITMDES_0' in df.columns:
                    df['ZITMDES_0'] = df['ZITMDES_0'].str.strip()

                return df
        except Exception as e:
            logging.error(f'Falha ao buscar dados de stock do X3: {e}')

        return pd.DataFrame()

    def _get_stock_range(self) -> Optional[tuple[str, str]]:
        """
        Busca o intervalo de armazéns na tabela ZFCYRANGE.
        """
        if not self.db_manager or not self.db_manager.connection:
            logging.error('Conexão com o banco de dados não está disponível.')
            return None

        logging.info('Buscar intervalo de armazéns...')
        stock_range = ('', '')

        try:
            # Busca o intervalo de armazéns
            base_sql = f'SELECT FCY_0,FCY_1 FROM {settings.SCHEMA}.ZFCYRANGE WHERE CPY_0 = :CPY_0'
            results = self.db_manager.fetch_data(query_base=base_sql, params={'CPY_0': 'GN'})

            if results:
                stock_range = (results[0]['FCY_0'], results[0]['FCY_1'])
        except Exception as e:
            logging.error(f'Falha ao buscar intervalo de armazéns: {e}')

        return stock_range

    def generate_total_stock_file(self, df: pd.DataFrame) -> Optional[Path]:
        """
        Gera um arquivo de texto com a soma das quantidades, agrupado por produto.
        """
        if df.empty:
            logging.warning('DataFrame vazio. Nenhum ficheiro será criado.')
            return

        logging.info('Gerar ficheiro de stock total por produto...')

        # Agrupa por produto e soma as quantidades
        total_stock = (
            df.groupby('ITMREF_0')
            .agg(
                ZITMDES_0=('ZITMDES_0', 'first'),
                AVLSTO_0=('AVLSTO_0', 'sum'),
                P1PVP_0=('P1PVP_0', 'first'),
                P2PVP_0=('P2PVP_0', 'first'),
                P2_0=('P2_0', 'first'),
            )
            .reset_index()
        )

        # Define o nome e caminho do ficheiro
        filename = f'STOCKTOTAL{datetime.now().strftime("%y%m%d%H%M%S")}.txt'
        filepath = self.export_dir / filename

        total_stock.to_csv(filepath, sep=';', index=False, encoding='utf-8', header=False, decimal=',')

        logging.info(f'Ficheiro de stock total gerado: {filepath}')

        return filepath

    def generate_store_files(self, df: pd.DataFrame) -> list[Path]:
        """
        Gera um ficheiro de texto para cada loja (STOFCY_0).
        """
        if df.empty:
            logging.warning('DataFrame vazio. Nenhum ficheiro será criado.')
            return []

        logging.info('Gerar ficheiros de stock por loja...')
        generated_files = []

        columns = ['ITMREF_0', 'ZITMDES_0', 'AVLSTO_0', 'P1PVP_0', 'P2PVP_0', 'P2_0', 'STOFCY_0']

        # Verifica se todas as colunas desejadas existem no DataFrame
        for col in columns:
            if col not in df.columns:
                logging.error(
                    f"Coluna '{col}' não encontrada no DataFrame. "
                    'Verifique a query SQL. Abortada a geração de ficheiros de loja.'
                )
                return []

        # Agrupa o DataFrame por loja
        grouped_by_store = df.groupby('STOFCY_0')

        for store_code, store_df in grouped_by_store:
            filename = f'STOCKLOJA_{store_code}{datetime.now().strftime("%y%m%d%H%M%S")}.txt'
            filepath = self.export_dir / filename

            # Filtrar DataFrame para incluir apenas itens com stock disponível
            filtered_df = store_df[store_df['AVLSTO_0'] > 0].copy()

            if filtered_df.empty:
                logging.info(f'Loja {store_code} não possui itens com stock disponível. Nenhum ficheiro será gerado.')
                continue

            # Criar novo DataFrame somente com as colunas necessárias
            output_df = filtered_df[columns]

            # Exporta o sub-DataFrame do loja para um ficheiro
            output_df.to_csv(filepath, sep=';', index=False, encoding='utf-8', header=False, decimal=',')

            logging.info(f'ficheiro gerado para o loja {store_code}: {filepath}')
            generated_files.append(filepath)

        return generated_files

    def upload_files(self, store_files: list[Path], total_stock_file: Optional[Path] = None) -> bool:
        """
        Faz o upload dos ficheiros gerados para o SFTP.
        """
        if not self.sftp_manager:
            logging.error('SFTP Manager não está disponível. Abortar upload.')
            return False

        if not store_files and not total_stock_file:
            return False

        files_to_upload = [f for f in store_files if f is not None]

        logging.info(f'Iniciar upload de {len(files_to_upload) + 1} ficheiros de stock...')

        if total_stock_file:
            remote_path = f'{self.base_remote_path}/Magento/{total_stock_file.name}'
            success = self.sftp_manager.upload_file(str(total_stock_file), remote_path)
            if success:
                archive_path = self.archive_dir / total_stock_file.name
                shutil.move(str(total_stock_file), str(archive_path))
                logging.info(f'Ficheiro {total_stock_file.name} enviado e arquivado com sucesso.')

        for file_path in files_to_upload:
            remote_path = f'{self.base_remote_path}/StockporLoja/{file_path.name}'
            success = self.sftp_manager.upload_file(str(file_path), remote_path)

            if success:
                archive_path = self.archive_dir / file_path.name
                shutil.move(str(file_path), str(archive_path))
                logging.info(f'Ficheiro {file_path.name} enviado e arquivado com sucesso.')

        return True

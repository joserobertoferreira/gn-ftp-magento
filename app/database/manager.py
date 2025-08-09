import logging
from typing import Any, Optional

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import SQLAlchemyError

from app.config import settings

# Configurar logging
logger = logging.getLogger(__name__)


class DatabaseManager:
    """
    Gerencia a conexão e as operações com o banco de dados SQL Server.
    """

    def __init__(self):
        """
        Inicializa o gerenciador, construir a string de conexão.
        """
        self.db_uri = (
            f'mssql+pymssql://'
            f'{settings.USERNAME}:{settings.PASSWORD}@'
            f'{settings.SERVER}:{settings.PORT}/'
            f'{settings.DATABASE}'
        )
        self.engine: Optional[Engine] = None
        self.connection: Optional[Connection] = None

    def __enter__(self):
        """Cria o engine e abre uma conexão."""
        try:
            logging.info('Criar engine e abrir conexão...')
            self.engine = create_engine(self.db_uri)
            self.connection = self.engine.connect()
            logging.info('Conexão com o banco de dados estabelecida.')
            return self
        except SQLAlchemyError as ex:
            logging.error(f'Erro ao conectar ao banco de dados via SQLAlchemy: {ex}')
            raise

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Fecha a conexão e dispõe do engine."""
        if self.connection:
            self.connection.close()
            logging.info('Conexão fechada.')

        if self.engine:
            self.engine.dispose()
            logging.info('Engine disposed.')

    def fetch_data(self, query_base: str, params: Optional[dict[str, Any]] = None) -> Optional[list[dict[str, Any]]]:
        """
        Executa uma query e retorna os resultados como uma lista de dicionários.
        """
        if not self.connection:
            logging.error('Nenhuma conexão com o banco de dados ativa.')
            return None

        try:
            stmt = text(query_base)

            if settings.DEBUG:
                logging.info(f'Executando query: {stmt}')
                logging.info(f'Com parâmetros: {params}')

            result_proxy = self.connection.execute(stmt, params or {})

            results = [dict(row) for row in result_proxy.mappings().all()]

            return results
        except SQLAlchemyError as e:
            logging.error(f'Erro ao executar a query com SQLAlchemy: {e}')
            return None

import logging
from typing import Any, Optional

import pymssql

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
        self.port = str(settings.PORT)
        self.user = settings.USERNAME
        self.server = settings.SERVER
        self.schema = settings.SCHEMA
        self.password = settings.PASSWORD
        self.database = settings.DATABASE

        self.connection: Optional[pymssql.Connection] = None

    def __enter__(self):
        """Abre a conexão com o banco de dados."""
        try:
            logging.info('Abrir conexão com o banco de dados via pymssql...')
            self.connection = pymssql.connect(
                server=self.server,
                user=self.user,
                password=self.password,
                database=self.database,
                # port=self.port,
                # as_dict=True,
            )
            logging.info('Conexão com o banco de dados estabelecida com sucesso.')
            return self
        except pymssql.Error as ex:
            logging.error(f'Erro ao conectar ao banco de dados via pymssql: {ex}')
            raise

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Método para usar a classe com o statement 'with'.
        Fecha a conexão com o banco de dados.
        """
        if self.connection:
            self.connection.close()
            logging.info('Conexão com o banco de dados fechada.')

    @staticmethod
    def _escape_name(name):
        """Escapa o nome de uma coluna ou alias com colchetes."""
        if '.' in name:
            parts = name.split('.')
            return f'[{parts[0]}].[{parts[1]}]'
        return f'[{name}]'

    @staticmethod
    def _build_where_clause(where_clauses: Optional[dict[str, Any]]):
        """
        Constrói a cláusula WHERE para pymssql (usando %s).
        """
        if not where_clauses:
            return '', []

        conditions = []
        params = []
        for key, value in where_clauses.items():
            field_name = f'[{key.replace(".", "].[")}]'  # Escapa nomes como antes

            if isinstance(value, (list, tuple)):
                safe_values = []
                for v in value:
                    if isinstance(v, (int, float)):
                        safe_values.append(str(v))
                    else:
                        # Escapa aspas simples em strings
                        safe_values.append(f"'{str(v).replace("'", "''")}'")

                conditions.append(f'{field_name} IN ({", ".join(safe_values)})')
            else:
                conditions.append(f'{field_name} = %s')
                params.append(value)

        return ' WHERE ' + ' AND '.join(conditions), params

    def fetch_data(
        self,
        query_base: str,
        where_clauses: Optional[dict[str, Any]] = None,
        group_by_fields: Optional[list[str]] = None,
        order_by_fields: Optional[list[str]] = None,
    ) -> Optional[list[dict[str, Any]]]:
        """
        Busca dados do banco de dados com filtros, agrupamentos e ordenação dinâmicos.
        :param query_base: A parte principal da query, incluindo SELECT, FROM e JOINs.
                           Ex: "SELECT t1.id, t2.nome FROM [dbo].[Tabela1] t1
                                LEFT JOIN [dbo].[Tabela2] t2 ON t1.id = t2.t1_id"
        :param where_clauses: Dicionário com as cláusulas WHERE.
                               Ex: {'t1.status': 'A', 't2.categoria': ('X', 'Y')}
        :param group_by_fields: Lista de campos para a cláusula GROUP BY.
                                Ex: ['t2.categoria']
        :param order_by_fields: Lista de campos para a cláusula ORDER BY.
                                 Pode incluir a direção. Ex: ['t1.data_criacao DESC']
        :return: Lista de tuplas com os resultados ou None em caso de erro.
        """
        if not self.connection:
            logging.error('Nenhuma conexão com o banco de dados ativa.')
            return None

        cursor = self.connection.cursor()
        query = query_base

        # Construção da query: Chamando os métodos auxiliares
        where_sql, params = self._build_where_clause(where_clauses)

        if where_sql:
            if ' where ' in query.lower():
                query += ' AND ' + where_sql[len(' WHERE ') :]
            else:
                query += where_sql

        if group_by_fields:
            escaped_fields = [f'[{f.replace(".", "].[")}]' for f in group_by_fields]
            query += ' GROUP BY ' + ', '.join(escaped_fields)

        if order_by_fields:
            safe_order_by = []
            for field in order_by_fields:
                parts = field.split()
                col = f'[{parts[0].replace(".", "].[")}]'
                direction = f' {parts[1].upper()}' if len(parts) > 1 and parts[1].upper() in {'ASC', 'DESC'} else ''
                safe_order_by.append(f'{col}{direction}')
            query += ' ORDER BY ' + ', '.join(safe_order_by)

        try:
            logging.info(f'Executar a query: {query}')
            logging.info(f'Com os parâmetros: {params}')

            cursor.execute(query, tuple(params))

            # 1. Pega os nomes das colunas da descrição do cursor.
            if cursor.description is None:
                return []  # Retorna lista vazia se a query não produzir colunas (ex: UPDATE)

            column_names = [desc[0] for desc in cursor.description]

            # 2. Pega todas as linhas de resultado (que são tuplas).
            rows_as_tuples = cursor.fetchall()

            # 3. Constrói a lista de dicionários.
            if rows_as_tuples is None:
                results = []
            else:
                results = [dict(zip(column_names, row)) for row in rows_as_tuples]

            return results
        except pymssql.Error as ex:
            logging.error(f'Erro ao executar a query: {ex}')
            return []
        finally:
            cursor.close()

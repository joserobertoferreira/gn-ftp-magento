import logging
from typing import Optional

import pyodbc

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
        self.connection_string = (
            # f'DRIVER={{{settings.DB_DRIVER}}};'
            f'SERVER={settings.DATABASE.get("SERVER")};'
            f'DATABASE={settings.DATABASE.get("DATABASE")};'
            f'UID={settings.DATABASE.get("USERNAME")};'
            f'PWD={settings.DATABASE.get("PASSWORD")};'
            f'TrustServerCertificate={settings.DATABASE.get("TRUSTED_CONNECTION")};'
        )
        self.schema = settings.DATABASE.get('SCHEMA', 'dbo')
        self.connection = None

    def __enter__(self):
        """
        Método para usar a classe com o statement 'with'.
        Abre a conexão com o banco de dados.
        """
        try:
            logging.info('Abrir conexão com o banco de dados...')
            self.connection = pyodbc.connect(self.connection_string)
            logging.info('Conexão com o banco de dados estabelecida com sucesso.')
            return self
        except pyodbc.Error as ex:
            sqlstate = ex.args[0]
            logging.error(f'Erro ao conectar ao banco de dados: {sqlstate} - {ex}')
            raise  # Re-levanta a exceção para que a aplicação pare se não conseguir conectar

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

    def _build_where_clause(self, where_clauses):
        """Constrói a cláusula WHERE e retorna a string e os parâmetros."""
        if not where_clauses:
            return '', []

        conditions = []
        params = []

        for key, value in where_clauses.items():
            field_name = self._escape_name(key)

            if isinstance(value, (list, tuple)):
                # Cláusula IN
                placeholders = ', '.join(['?' for _ in value])
                conditions.append(f'{field_name} IN ({placeholders})')
                params.extend(value)
            else:
                # Cláusula =
                conditions.append(f'{field_name} = ?')
                params.append(value)

        return ' WHERE ' + ' AND '.join(conditions), params

    def _build_group_by_clause(self, group_by_fields):
        """Constrói a cláusula GROUP BY."""
        if not group_by_fields:
            return ''

        # Escapar todos os nomes antes de juntar
        escaped_fields = [self._escape_name(f) for f in group_by_fields]
        return ' GROUP BY ' + ', '.join(escaped_fields)

    def _build_order_by_clause(self, order_by_fields):
        """Constrói a cláusula ORDER BY."""
        if not order_by_fields:
            return ''

        escaped_order_by = []
        for field in order_by_fields:
            parts = field.split()
            col = parts[0]
            direction = ''
            if len(parts) > 1 and parts[1].upper() in {'ASC', 'DESC'}:
                direction = ' ' + parts[1].upper()

            escaped_col = self._escape_name(col)
            escaped_order_by.append(f'{escaped_col}{direction}')

        return ' ORDER BY ' + ', '.join(escaped_order_by)

    def fetch_data(
        self,
        query_base: str,
        where_clauses: Optional[dict] = None,
        group_by_fields: Optional[list] = None,
        order_by_fields: Optional[list] = None,
    ) -> Optional[list[dict]]:
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
        params = []

        # Construção da query: Chamando os métodos auxiliares
        where_sql, where_params = self._build_where_clause(where_clauses)

        # Verifica se a query base já contém um WHERE
        if ' where ' in query.lower() and where_sql:
            # Se sim, usamos AND para juntar as cláusulas
            query += ' AND ' + where_sql[len(' WHERE ') :]
        else:
            # Senão, adicionamos o WHERE
            query += where_sql

        params.extend(where_params)

        query += self._build_group_by_clause(group_by_fields)
        query += self._build_order_by_clause(order_by_fields)

        try:
            logging.info(f'Executar a query: {query}')
            logging.info(f'Com os parâmetros: {params}')

            cursor.execute(query, tuple(params))

            # Formatação dos resultados para lista de dicionários
            if cursor.description is None:
                return []

            column_names = [column[0] for column in cursor.description]
            results = [dict(zip(column_names, row)) for row in cursor.fetchall()]

            return results
        except pyodbc.Error as ex:
            logging.error(f'Erro ao executar a query: {ex}')
            return None
        finally:
            cursor.close()

import logging
from pathlib import Path
from typing import Optional

import paramiko
from paramiko.ssh_exception import AuthenticationException, BadHostKeyException, SSHException

from app.config import settings

# Desativando o logging excessivo do paramiko para não poluir nosso console.
# Manteremos apenas os logs de erro.
logging.getLogger('paramiko').setLevel(logging.ERROR)


class SftpManager:
    """
    Gerencia a conexão e as operações com o servidor SFTP.
    """

    def __init__(self):
        """
        Inicializa o gerenciador com as configurações de SFTP.
        """
        self.hostname = str(settings.SFTP_HOST)
        self.port = int(settings.SFTP_PORT)
        self.username = str(settings.SFTP_USER)
        self.password = str(settings.SFTP_PASSWORD)

        self.ssh_client: Optional[paramiko.SSHClient] = None
        self.sftp_client: Optional[paramiko.SFTPClient] = None

        self.transport = None
        self.sftp_client = None

    def __enter__(self):
        """
        Abre a conexão com o servidor SFTP usando um ficheiro known_hosts.
        """
        try:
            logging.info(f'Conectar ao servidor SFTP em {self.hostname}:{self.port}...')

            # 1. Cria uma instância do cliente SSH
            self.ssh_client = paramiko.SSHClient()

            # 2. Carrega as chaves do nosso ficheiro known_hosts local
            # O getcwd() pega o diretório atual onde o script está rodando
            known_hosts_file = Path.cwd() / 'known_hosts'
            if not known_hosts_file.exists():
                raise FileNotFoundError(
                    f"Ficheiro 'known_hosts' não encontrado no diretório do projeto: {known_hosts_file}"
                )

            self.ssh_client.load_host_keys(known_hosts_file)

            # 3. Define a política para rejeitar chaves que não estão no nosso ficheiro
            # Isso mantém a segurança alta.
            self.ssh_client.set_missing_host_key_policy(paramiko.RejectPolicy())

            # 4. Conecta ao servidor
            # O cliente SSH fará a verificação da chave do host automaticamente contra o ficheiro carregado.
            self.ssh_client.connect(
                hostname=self.hostname,
                port=self.port,
                username=self.username,
                password=self.password,
                timeout=10,  # Adiciona um timeout para a conexão
            )

            # 5. Abre o canal SFTP sobre a conexão SSH
            self.sftp_client = self.ssh_client.open_sftp()

            logging.info('Conexão SFTP estabelecida com sucesso.')
            return self
        except BadHostKeyException as e:
            logging.error(f'ERRO DE CHAVE DO HOST: A chave do servidor mudou ou é um impostor! Detalhes: {e}')
            self.__exit__(None, None, None)  # Garante que tudo seja fechado
            raise
        except AuthenticationException:
            logging.error('Falha na autenticação SFTP: Utilizador ou senha inválidos.')
            self.__exit__(None, None, None)
            raise
        except (SSHException, TimeoutError, FileNotFoundError) as e:
            logging.error(f'Falha na conexão SFTP: {e}')
            self.__exit__(None, None, None)
            raise

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Fecha a conexão SFTP e SSH.
        """
        if self.sftp_client:
            self.sftp_client.close()
        if self.ssh_client:
            self.ssh_client.close()
        logging.info('Conexão SFTP fechada.')

    def upload_file(self, local_path: str, remote_path: str):
        """
        Faz o upload de um ficheiro local para o servidor SFTP.

        :param local_path: Caminho do ficheiro na máquina local.
        :param remote_path: Caminho completo (incluindo nome do ficheiro) no servidor remoto.
        :return: True se o upload for bem-sucedido, False caso contrário.
        """
        if not self.sftp_client:
            logging.error('Cliente SFTP não está conectado. O upload foi abortado.')
            return False

        try:
            logging.info(f"Iniciar upload de '{local_path}' para '{remote_path}'...")
            self.sftp_client.put(local_path, remote_path)
            logging.info('Upload concluído com sucesso.')
            return True
        except Exception as e:
            logging.error(f'Falha no upload do ficheiro: {e}')
            return False

    def download_file(self, remote_path: str, local_path: str):
        """
        Faz o download de um ficheiro do servidor SFTP para a máquina local.

        :param remote_path: Caminho completo do ficheiro no servidor remoto.
        :param local_path: Caminho onde o ficheiro será salvo localmente.
        :return: True se o download for bem-sucedido, False caso contrário.
        """
        if not self.sftp_client:
            logging.error('Cliente SFTP não está conectado. O download foi abortado.')
            return False

        try:
            logging.info(f"Iniciar download de '{remote_path}' para '{local_path}'...")
            self.sftp_client.get(remote_path, local_path)
            logging.info('Download concluído com sucesso.')
            return True
        except Exception as e:
            logging.error(f'Falha no download do ficheiro: {e}')
            return False

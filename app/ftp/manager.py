import base64
import logging

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
        self.host_key_string = str(settings.SFTP_HOST_KEY)

        self.transport = None
        self.sftp_client = None

    def _verify_host_key(self):
        """
        Carrega a chave do host esperada a partir da string de configuração.
        """
        try:
            # A chave do host é do formato "algoritmo tamanho fingerprint_base64"
            key_type, key_string = self.host_key_string.split(' ')[0], self.host_key_string.split(' ')[2]

            # Carrega a chave pública do tipo correto
            decoded_key = base64.b64decode(key_string)

            if key_type == 'ssh-rsa':
                host_key = paramiko.RSAKey(data=decoded_key)
            elif key_type == 'ssh-dss':
                host_key = paramiko.DSSKey(data=decoded_key)
            elif key_type == 'ssh-ed25519':
                host_key = paramiko.Ed25519Key(data=decoded_key)
            else:
                logging.error(f'Tipo de chave do host desconhecido: {key_type}')
                return None
            return host_key
        except Exception as e:
            logging.error(f'Erro ao processar a chave do host: {e}')
            return None

    def __enter__(self):
        """
        Método para usar a classe com o statement 'with'.
        Abre a conexão com o servidor SFTP.
        """
        try:
            # Primeiro, validamos a nossa própria chave de configuração.
            # Se ela for inválida, falhamos antes mesmo de tocar na rede.
            expected_key = self._verify_host_key()
            if expected_key is None:
                # Lança uma exceção clara sobre o problema de configuração local.
                raise ValueError('A chave do host (SFTP_HOST_KEY) configurada é inválida ou não pôde ser processada.')

            logging.info(f'Conectando ao servidor SFTP em {self.hostname}:{self.port}...')

            # 1. Estabelecer a conexão de transporte SSH
            self.transport = paramiko.Transport((self.hostname, self.port))

            # Conecta antes de verificar a chave, pois a verificação é feita durante a conexão
            self.transport.connect(username=self.username, password=self.password, hostkey=expected_key)

            # 2. Iniciar o cliente SFTP
            self.sftp_client = paramiko.SFTPClient.from_transport(self.transport)

            logging.info('Conexão SFTP estabelecida com sucesso.')
            return self

        except (SSHException, AuthenticationException, BadHostKeyException, TimeoutError) as e:
            logging.error(f'Falha na conexão SFTP: {e}')
            # Garante que, em caso de erro na conexão, o 'exit' seja chamado para limpar
            if self.transport and self.transport.is_active():
                self.transport.close()
            raise  # Re-levanta a exceção

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Método para usar a classe com o statement 'with'.
        Fecha a conexão SFTP.
        """
        if self.sftp_client:
            self.sftp_client.close()
        if self.transport and self.transport.is_active():
            self.transport.close()
        logging.info('Conexão SFTP fechada.')

    def upload_file(self, local_path: str, remote_path: str):
        """
        Faz o upload de um arquivo local para o servidor SFTP.

        :param local_path: Caminho do arquivo na máquina local.
        :param remote_path: Caminho completo (incluindo nome do arquivo) no servidor remoto.
        :return: True se o upload for bem-sucedido, False caso contrário.
        """
        if not self.sftp_client:
            logging.error('Cliente SFTP não está conectado. O upload foi abortado.')
            return False

        try:
            logging.info(f"Iniciando upload de '{local_path}' para '{remote_path}'...")
            self.sftp_client.put(local_path, remote_path)
            logging.info('Upload concluído com sucesso.')
            return True
        except Exception as e:
            logging.error(f'Falha no upload do arquivo: {e}')
            return False

    def download_file(self, remote_path: str, local_path: str):
        """
        Faz o download de um arquivo do servidor SFTP para a máquina local.

        :param remote_path: Caminho completo do arquivo no servidor remoto.
        :param local_path: Caminho onde o arquivo será salvo localmente.
        :return: True se o download for bem-sucedido, False caso contrário.
        """
        if not self.sftp_client:
            logging.error('Cliente SFTP não está conectado. O download foi abortado.')
            return False

        try:
            logging.info(f"Iniciando download de '{remote_path}' para '{local_path}'...")
            self.sftp_client.get(remote_path, local_path)
            logging.info('Download concluído com sucesso.')
            return True
        except Exception as e:
            logging.error(f'Falha no download do arquivo: {e}')
            return False

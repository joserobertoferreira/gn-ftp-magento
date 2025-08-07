import logging
import os
import re

from app.config import settings
from app.ftp.manager import SftpManager


def generate_and_upload_client_file():
    """
    Orquestra o processo completo:
    1. Busca dados de clientes no banco de dados.
    2. Gera um arquivo de texto (TXT) com os dados.
    3. Faz o upload do arquivo para o servidor SFTP.
    4. Remove o arquivo local após o upload.
    """
    # logging.info('Iniciando a tarefa de geração e envio do arquivo de clientes.')

    # # 1. Buscar os dados do banco de dados
    # # ----------------------------------------
    # logging.info('Buscando dados dos clientes no banco de dados...')

    # # Exemplo de uma query complexa. Adapte para sua necessidade.
    # base_sql = """
    #     SELECT
    #         c.IDCliente,
    #         c.Nome,
    #         c.Email,
    #         e.Rua,
    #         e.Cidade,
    #         e.Estado
    #     FROM
    #         dbo.Clientes c
    #     LEFT JOIN
    #         dbo.Enderecos e ON c.IDCliente = e.IDCliente
    # """

    # try:
    #     with DatabaseManager() as db:
    #         client_data = db.fetch_data(
    #             query_base=base_sql, where_clauses={'c.Status': 'ATIVO'}, order_by_fields=['c.Nome ASC']
    #         )

    #     if not client_data:
    #         logging.warning('Nenhum dado de cliente encontrado para exportar. Tarefa concluída.')
    #         return

    #     logging.info(f'Foram encontrados {len(client_data)} registros de clientes.')

    # except Exception as e:
    #     logging.error(f'Falha ao buscar dados no banco de dados: {e}')
    #     # Se não conseguirmos buscar os dados, não há como continuar.
    #     return

    # # 2. Gerar o arquivo de texto (TXT)
    # # ---------------------------------
    # # Cria o diretório de exportação se ele não existir.
    # os.makedirs(settings.LOCAL_EXPORT_PATH, exist_ok=True)

    # # Monta um nome de arquivo único com data e hora.
    # timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    # filename = f'clientes_{timestamp}.txt'
    # local_filepath = os.path.join(settings.LOCAL_EXPORT_PATH, filename)

    # logging.info(f'Gerando arquivo de texto em: {local_filepath}')

    # try:
    #     with open(local_filepath, 'w', encoding='utf-8') as f:
    #         # Escreve o cabeçalho (opcional, mas recomendado)
    #         header = ';'.join(client_data[0].keys())
    #         f.write(header + '\n')

    #         # Escreve cada linha de dados
    #         for row in client_data:
    #             # Converte todos os valores para string e os une com um delimitador
    #             line_values = [str(value) for value in row.values()]
    #             f.write(';'.join(line_values) + '\n')

    #     logging.info('Arquivo de texto gerado com sucesso.')

    # except Exception as e:
    #     logging.error(f'Falha ao gerar o arquivo de texto: {e}')
    #     return

    # # 3. Fazer o upload do arquivo via SFTP
    # # --------------------------------------
    # remote_filepath = os.path.join(settings.SFTP_UPLOAD_PATH, filename).replace('\\', '/')

    # try:
    #     with SftpManager() as sftp:
    #         success = sftp.upload_file(local_filepath, remote_filepath)

    #     if not success:
    #         logging.error('O upload do arquivo para o servidor SFTP falhou. O arquivo local será mantido.')
    #         return  # Não apaga o arquivo local se o upload falhou

    # except Exception as e:
    #     logging.error(f'Uma exceção ocorreu durante o processo de upload SFTP: {e}')
    #     return

    # # 4. Limpeza: Remover o arquivo local
    # # -------------------------------------
    # try:
    #     os.remove(local_filepath)
    #     logging.info(f"Arquivo local '{local_filepath}' removido com sucesso.")
    # except OSError as e:
    #     logging.error(f'Erro ao remover o arquivo local: {e}')

    # logging.info('Tarefa de geração e envio do arquivo de clientes concluída com sucesso.')
    pass


def sync_local_folder_to_sftp():
    """
    Monitora uma pasta local, identifica arquivos com um padrão específico,
    envia-os para uma subpasta dinâmica no SFTP e os remove localmente.

    Exemplo:
    - Arquivo local: 'EDIEE01_data.csv' -> Pasta SFTP: 'exportx3/automation/E01'
    - Arquivo local: 'EDIEE03_report.txt' -> Pasta SFTP: 'exportx3/automation/E03'
    """
    logging.info('Iniciando a tarefa de sincronização da pasta local para o SFTP.')

    local_sync_path = settings.LOCAL_EXPORT_PATH

    if not os.path.isdir(local_sync_path):
        logging.warning(f"O diretório de sincronização local '{local_sync_path}' não existe. Tarefa abortada.")
        return

    # Lista todos os arquivos no diretório local
    try:
        files_to_process = [f for f in os.listdir(local_sync_path) if os.path.isfile(os.path.join(local_sync_path, f))]
    except OSError as e:
        logging.error(f"Não foi possível ler o diretório '{local_sync_path}': {e}")
        return

    if not files_to_process:
        logging.info('Nenhum arquivo encontrado na pasta de sincronização. Nenhuma ação necessária.')
        return

    logging.info(f'Encontrados {len(files_to_process)} arquivos para processar.')

    # Padrão de expressão regular para encontrar 'EDIEE<XX>'
    # Ele captura os dois dígitos após 'EDIEE'
    pattern = re.compile(r'EDIEE(\d{2})', re.IGNORECASE)

    files_processed_successfully = []

    # Abrimos a conexão SFTP uma vez para processar todos os arquivos
    try:
        with SftpManager() as sftp:
            for filename in files_to_process:
                match = pattern.search(filename)

                if not match:
                    logging.info(f"Arquivo '{filename}' não corresponde ao padrão 'EDIEE<XX>' e será ignorado.")
                    continue

                # O código da pasta é o grupo capturado (ex: '01', '03')
                folder_code = match.group(1)
                remote_subfolder = f'E{folder_code}'  # Monta o nome da subpasta (ex: 'E01')

                # Monta o caminho remoto completo
                base_remote_path = 'exportx3/automation'
                remote_path = os.path.join(base_remote_path, remote_subfolder, filename).replace('\\', '/')

                local_filepath = os.path.join(local_sync_path, filename)

                logging.info(f"Processando '{filename}': enviando para '{remote_path}'")

                # Tenta fazer o upload do arquivo
                success = sftp.upload_file(local_filepath, remote_path)

                if success:
                    logging.info(f"Arquivo '{filename}' enviado com sucesso.")
                    files_processed_successfully.append(local_filepath)
                else:
                    logging.error(
                        (
                            f"Falha ao enviar o arquivo '{filename}'. "
                            'Ele será mantido localmente e tentado novamente na próxima execução.'
                        )
                    )

    except Exception as e:
        logging.error(f'Ocorreu um erro durante a conexão SFTP. O processamento dos arquivos foi interrompido: {e}')
        # Se a conexão falhar, não tentamos apagar nenhum arquivo
        return

    # 4. Limpeza: Remover os arquivos locais que foram enviados com sucesso
    # -------------------------------------------------------------------
    if not files_processed_successfully:
        logging.info('Nenhum arquivo foi processado com sucesso.')
    else:
        logging.info('Removendo arquivos locais que foram enviados com sucesso...')
        for filepath in files_processed_successfully:
            try:
                os.remove(filepath)
                logging.info(f"Arquivo local '{filepath}' removido.")
            except OSError as e:
                logging.error(f"Erro ao remover o arquivo local '{filepath}': {e}")

    logging.info('Tarefa de sincronização da pasta local concluída.')

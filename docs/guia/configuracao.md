# Configuração

Toda a configuração é lida em `app/config/settings.py` através do
[python-decouple](https://github.com/HBNetwork/python-decouple), que procura as variáveis no
ambiente do sistema e no ficheiro `.env` na raiz do projeto.

!!! danger "Variáveis obrigatórias"
    `LOCAL_EXPORT_PATH`, `LOCAL_IMPORT_PATH` e `LOCAL_ARCHIVE_PATH` **não têm valor por omissão**.
    Se faltarem, a aplicação falha logo no arranque com `decouple.UndefinedValueError`.

## Base de dados (Sage X3)

| Variável      | Omissão     | Descrição                                                     |
| ------------- | ----------- | ------------------------------------------------------------- |
| `DB_SERVER`   | `localhost` | Servidor SQL Server.                                          |
| `DB_PORT`     | `1433`      | Porta do SQL Server.                                          |
| `DB_DATABASE` | —           | Nome da base de dados.                                        |
| `DB_SCHEMA`   | —           | Schema (pasta) do X3, ex.: `GNPROD`. Usado nas queries e nos caminhos locais. |
| `DB_USERNAME` | —           | Utilizador SQL.                                               |
| `DB_PASSWORD` | —           | Palavra-passe SQL.                                            |

!!! warning "Caracteres especiais na palavra-passe"
    A connection string é montada por concatenação (`mssql+pymssql://user:pass@host:port/db`).
    Palavras-passe com `@`, `:`, `/` ou `#` quebram a ligação. Ver
    [Análise técnica](../analise-tecnica.md#base-de-dados).

## SFTP

| Variável              | Omissão     | Descrição                                               |
| --------------------- | ----------- | ------------------------------------------------------- |
| `SFTP_HOST`           | `localhost` | Servidor SFTP.                                          |
| `SFTP_PORT`           | `22`        | Porta SSH.                                              |
| `SFTP_USER`           | —           | Utilizador.                                             |
| `SFTP_PASSWORD`       | —           | Palavra-passe.                                          |
| `SFTP_SYNC_BASE_PATH` | `/`         | Diretório base remoto onde estão as pastas das lojas, `Magento/` e `StockporLoja/`. |

## Pastas

Os caminhos `LOCAL_*` aceitam o marcador `{schema}`, substituído pelo valor de `DB_SCHEMA`.
Todas as variáveis desta secção são obrigatórias.

| Variável             | Descrição                                                                 |
| -------------------- | ------------------------------------------------------------------------- |
| `LOCAL_EXPORT_PATH`  | Pasta de saída: ficheiros a enviar para o SFTP (EDI do X3 e ficheiros gerados pelas rotinas de stock, códigos de barras e emails). Pode conter a subpasta `devolucao/`. |
| `LOCAL_IMPORT_PATH`  | Pasta de entrada: ficheiros descarregados do SFTP, a importar no X3.      |
| `LOCAL_ARCHIVE_PATH` | Ficheiros enviados com sucesso são movidos para aqui.                     |
| `STORAGE_ARCHIVE_PATH` | Partilha de rede com uma cópia dos ficheiros de stock (`STOCK\STOCKTOTAL`, `STOCK\STOCKPORLOJA`) e de códigos de barras (`STOCK\EANS`). |
| `EMAIL_ARCHIVE_PATH` | Partilha de rede com uma cópia dos ficheiros de emails de satisfação (`Site` e `Lojas`). |

```dotenv title="Exemplo"
LOCAL_EXPORT_PATH=D:\Sage\X3\dossiers\{schema}\MAGENTO\OUT
LOCAL_IMPORT_PATH=D:\Sage\X3\dossiers\{schema}\MAGENTO\IN
LOCAL_ARCHIVE_PATH=D:\Sage\X3\dossiers\{schema}\MAGENTO\ARCHIVE
STORAGE_ARCHIVE_PATH=\\servidor\partilha\DADOS
EMAIL_ARCHIVE_PATH=\\servidor\partilha\REPORTS
```

As pastas são criadas automaticamente se não existirem. A conta que executa o serviço tem de ter
permissão de escrita nas partilhas de rede (ver
[Executar como serviço](operacao.md#executar-como-servico-no-windows)).

## Agendamento

| Variável                    | Omissão                     | Descrição                                                  |
| --------------------------- | --------------------------- | ---------------------------------------------------------- |
| `SCHEDULE_ENABLED`          | `True`                      | `True` = processo contínuo; `False` = executa uma vez e termina. |
| `SCHEDULE_MONTHS`           | `1,2,3,4,5,6,7,8,9,10,11,12`| Meses em que a execução é permitida (lista separada por vírgulas). |
| `SCHEDULE_RUN_IMMEDIATELY`  | `True`                      | Executa no arranque as tarefas que estiverem dentro da sua janela. |

Horário de cada tarefa:

| Variável                             | Omissão | Descrição                                                  |
| ------------------------------------ | ------- | ---------------------------------------------------------- |
| `SCHEDULE_START_TIME`                | `08:00` | Transferência de ficheiros: início da janela (`HH:MM`).    |
| `SCHEDULE_END_TIME`                  | `18:00` | Transferência de ficheiros: fim da janela (`HH:MM`). Pode ser inferior ao início (janela que atravessa a meia-noite). |
| `SCHEDULE_INTERVAL_MINUTES`          | `60`    | Transferência de ficheiros: intervalo, em minutos.         |
| `STOCK_SCHEDULE_START_TIME`          | `08:00` | Stock: início da janela.                                   |
| `STOCK_SCHEDULE_END_TIME`            | `23:00` | Stock: fim da janela.                                      |
| `STOCK_SCHEDULE_INTERVAL_MINUTES`    | `60`    | Stock: intervalo, em minutos.                              |
| `BARCODES_SCHEDULE_START_TIME`       | `08:00` | Códigos de barras: início da janela.                       |
| `BARCODES_SCHEDULE_END_TIME`         | `16:00` | Códigos de barras: fim da janela.                          |
| `BARCODES_SCHEDULE_INTERVAL_MINUTES` | `240`   | Códigos de barras: intervalo, em minutos.                  |
| `CUSTOMER_EMAILS_SCHEDULE_TIME`      | `06:00` | Emails de satisfação: hora da execução diária.             |

Ver [Agendamento](../funcionamento/agendamento.md) para o comportamento detalhado.

## Outros

| Variável | Omissão | Descrição                                                         |
| -------- | ------- | ----------------------------------------------------------------- |
| `DEBUG`  | `False` | Quando `True`, regista no log cada query SQL executada e os seus parâmetros. |

## Logging

A configuração de logging está definida como constantes em `settings.py` (não vem do `.env`):

| Constante                | Valor           |
| ------------------------ | --------------- |
| `LOG_DIR`                | `logs`          |
| `LOG_ROOT_LEVEL`         | `DEBUG`         |
| `LOG_CONSOLE_LEVEL`      | `INFO`          |
| `LOG_INFO_FILENAME`      | `app_info.log` (nível `INFO`)  |
| `LOG_ERROR_FILENAME`     | `app_error.log` (nível `ERROR`) |

## Ficheiro `known_hosts`

A ligação SFTP usa uma política **estrita** de verificação de chave de host
(`paramiko.RejectPolicy`): o servidor só é aceite se a sua chave constar do ficheiro
`known_hosts` no **diretório de trabalho atual** (`Path.cwd()`).

Para o obter:

```bash
ssh-keyscan -p 22 sftp.exemplo.pt > known_hosts
```

!!! tip
    Valide a impressão digital da chave com o fornecedor do SFTP antes de a confiar.
    Se a chave do servidor mudar, a ligação falha com `BadHostKeyException` até o ficheiro ser atualizado.

## Ficheiro `.env` completo

```dotenv
# Base de dados
DB_SERVER=srv-x3
DB_DATABASE=x3
DB_USERNAME=magento
DB_PASSWORD=********
DB_PORT=1433
DB_SCHEMA=GNPROD

DEBUG=False

# SFTP
SFTP_HOST=sftp.exemplo.pt
SFTP_USER=gn
SFTP_PASSWORD=********
SFTP_PORT=22
SFTP_SYNC_BASE_PATH=/home/gn

# Pastas locais
LOCAL_EXPORT_PATH=D:\Sage\X3\dossiers\{schema}\MAGENTO\OUT
LOCAL_IMPORT_PATH=D:\Sage\X3\dossiers\{schema}\MAGENTO\IN
LOCAL_ARCHIVE_PATH=D:\Sage\X3\dossiers\{schema}\MAGENTO\ARCHIVE
STORAGE_ARCHIVE_PATH=\\servidor\partilha\DADOS
EMAIL_ARCHIVE_PATH=\\servidor\partilha\REPORTS

# Agendamento
SCHEDULE_ENABLED=True
SCHEDULE_MONTHS=1,2,3,4,5,6,7,8,9,10,11,12
SCHEDULE_START_TIME=06:00
SCHEDULE_END_TIME=20:00
SCHEDULE_INTERVAL_MINUTES=5
SCHEDULE_RUN_IMMEDIATELY=True
STOCK_SCHEDULE_START_TIME=08:00
STOCK_SCHEDULE_END_TIME=23:00
STOCK_SCHEDULE_INTERVAL_MINUTES=60
BARCODES_SCHEDULE_START_TIME=08:00
BARCODES_SCHEDULE_END_TIME=16:00
BARCODES_SCHEDULE_INTERVAL_MINUTES=240
CUSTOMER_EMAILS_SCHEDULE_TIME=06:00
```

# Instalação

## Pré-requisitos

- **Python 3.12** ou superior (ver `.python-version`).
- **[uv](https://docs.astral.sh/uv/)** (recomendado) ou `pip`.
- Acesso de rede ao **SQL Server** do Sage X3 (porta por omissão `1433`).
- Acesso de rede ao **servidor SFTP** (porta por omissão `22`) e a respetiva chave de host
  (ver [`known_hosts`](configuracao.md#ficheiro-known_hosts)).

## Instalar dependências

=== "uv (recomendado)"

    ```bash
    uv sync
    ```

    Para incluir também as ferramentas de documentação:

    ```bash
    uv sync --group docs
    ```

=== "pip"

    ```bash
    python -m venv .venv
    .venv\Scripts\activate          # Windows
    # source .venv/bin/activate     # Linux / macOS
    pip install -r requirements.txt
    ```

    Para incluir também as ferramentas de documentação:

    ```bash
    pip install -r requirements-docs.txt
    ```

    O `requirements-docs.txt` é independente: para construir apenas a documentação não é preciso
    instalar o `requirements.txt`.

    !!! warning "Os ficheiros requirements podem estar desatualizados"
        A fonte de verdade das dependências é o `pyproject.toml` / `uv.lock`.
        O `requirements.txt` é mantido manualmente. O `requirements-docs.txt` é gerado com:

        ```bash
        uv export --only-group docs --no-hashes --no-annotate --no-header --no-emit-project -o requirements-docs.txt
        ```

## Preparar a configuração

1. Copiar o ficheiro de exemplo:

    ```bash
    cp .env.exemple .env
    ```

2. Preencher as variáveis — ver [Configuração](configuracao.md).
3. Colocar o ficheiro `known_hosts` na **raiz do projeto** (diretório de onde a aplicação é executada).

## Estrutura do projeto

```text
magento/
├── main.py                     # Ponto de entrada
├── app/
│   ├── config/
│   │   ├── settings.py         # Leitura do .env e constantes
│   │   └── logging.py          # Configuração de logging
│   ├── database/
│   │   └── manager.py          # DatabaseManager (SQL Server)
│   ├── ftp/
│   │   └── manager.py          # SftpManager (paramiko)
│   ├── scheduler/
│   │   └── scheduler.py        # Scheduler (várias tarefas, janela horária por tarefa)
│   └── services/
│       ├── file_handler.py     # Orquestração dos fluxos
│       ├── export_sync.py      # ExportSync (base: ligações, envio, cópia e arquivo)
│       ├── stock_sync.py       # StockSync (extração e ficheiros de stock)
│       ├── barcode_sync.py     # BarcodeSync (extração e ficheiro de códigos de barras)
│       └── customer_email_sync.py  # CustomerEmailSync (ficheiros de emails de satisfação)
├── docs/                       # Esta documentação (MkDocs)
├── mkdocs.yml
├── known_hosts                 # Chave(s) do servidor SFTP (não versionado)
├── .env                        # Configuração (não versionado)
├── logs/                       # Criado automaticamente
├── pyproject.toml
└── uv.lock
```

## Documentação

Servir localmente com recarregamento automático:

```bash
uv run --group docs mkdocs serve
```

Gerar o site estático (pasta `site/`, já ignorada pelo Git):

```bash
uv run --group docs mkdocs build --strict
```

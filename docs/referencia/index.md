# Referência da API

Documentação gerada automaticamente a partir do código-fonte e das *docstrings* com
[mkdocstrings](https://mkdocstrings.github.io/).

| Página                          | Conteúdo                                                        |
| ------------------------------- | --------------------------------------------------------------- |
| [main](main.md)                 | Ponto de entrada e definição das tarefas.                       |
| [app.config](config.md)         | Configuração (`settings`) e logging.                            |
| [app.database](database.md)     | `DatabaseManager` — acesso ao SQL Server do Sage X3.            |
| [app.ftp](ftp.md)               | `SftpManager` — operações no servidor SFTP.                     |
| [app.scheduler](scheduler.md)   | `Scheduler` — agendamento de tarefas com horário próprio.                   |
| [app.services](services.md)     | `file_handler`, `ExportSync`, `StockSync`, `BarcodeSync` e `CustomerEmailSync` — fluxos de negócio. |

!!! note "Análise estática"
    O mkdocstrings usa o [Griffe](https://mkdocstrings.github.io/griffe/) para ler o código
    **sem o importar**, por isso a documentação pode ser gerada sem `.env` nem acesso à base de dados.

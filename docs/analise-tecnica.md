# Análise técnica

Resultado da revisão do código (novembro de 2026). Os pontos estão agrupados por área e
classificados por prioridade.

| Prioridade | Significado                                                        |
| ---------- | ------------------------------------------------------------------ |
| :red_circle: **Alta**   | Pode causar comportamento incorreto ou perda de dados.  |
| :orange_circle: **Média** | Comportamento frágil, confuso ou difícil de diagnosticar. |
| :green_circle: **Baixa**  | Qualidade de código / manutenção.                       |

## Pontos fortes

- Separação clara entre configuração, infraestrutura (BD, SFTP) e serviços.
- Ligações geridas por *context managers*, com fecho garantido.
- **Verificação estrita da chave de host SFTP** (`RejectPolicy` + `known_hosts`) — protege contra
  *man-in-the-middle*.
- Ficheiros **só são removidos do SFTP após download bem-sucedido** e só são arquivados após
  upload bem-sucedido.
- Configuração externa via `.env`, com `{schema}` nos caminhos para suportar várias pastas X3.
- Queries parametrizadas onde há valores fixos (`ZFCYRANGE … WHERE CPY_0 = :CPY_0`).
- Lint e formatação configurados com `ruff`.

---

## Orquestração

### Execução dos stocks depende do minuto 00

:white_check_mark: **Resolvido** — o stock passou a ser uma tarefa com horário próprio, alinhado
ao relógio. Ver [Agendamento](funcionamento/agendamento.md).

### Valores de retorno ambíguos

:orange_circle: **Média** — `app/services/file_handler.py`

- `sync_local_folder_to_sftp()` devolve `False` quando **não há ficheiros**, o que faz `main`
  registar *"Problemas com a transferência de ficheiros para o Magento"* em execuções normais.
- `sync_stocks()` devolve `True` mesmo quando nada foi enviado (último `return True`).
- `sync_sftp_to_local_folder()` devolve `False` quando `ZLOJAS` está vazia, misturando
  "sem trabalho" com "erro".

**Recomendação:** distinguir "nada a fazer" de "erro" (ex.: devolver `True` quando não há
ficheiros, ou usar um `enum`/contagem de ficheiros processados).

---

## Stocks (`StockSync`)

### Intervalo de armazéns vazio não é detetado

:red_circle: **Alta** — `StockSync._get_stock_range` / `fetch_stock_data`

`_get_stock_range()` devolve `('', '')` quando não encontra registo, e o teste
`if stock_range:` é sempre verdadeiro para um tuplo de 2 elementos. A query corre então com
`BETWEEN '' AND ''` e devolve 0 linhas, sem qualquer aviso explícito.

**Recomendação:** devolver `None` quando não há registo e testar
`if stock_range and all(stock_range)`.

### Fuga de ligação se o SFTP falhar no arranque

:orange_circle: **Média** — `ExportSync.__enter__` (afeta as rotinas de stock, códigos de barras e emails)

A BD é aberta primeiro; se a ligação SFTP lançar exceção, `__enter__` não termina e o
`__exit__` **não é chamado**, deixando a ligação SQL aberta.

**Recomendação:** usar `contextlib.ExitStack` ou envolver a abertura do SFTP num
`try/except` que feche a BD.

### Ficheiros exportados não reenviados

:orange_circle: **Média**

Se o upload de um `STOCKTOTAL*`/`STOCKLOJA_*`, `EANS*`, `MS_*` ou `ML_*` falhar, o ficheiro fica em
`LOCAL_EXPORT_PATH` para sempre — não corresponde às regras de roteamento do fluxo de envio, e a
próxima execução gera ficheiros novos. A pasta vai acumulando ficheiros órfãos. No caso dos
emails, a execução seguinte processa outro dia, por isso os dados do dia falhado têm de ser
enviados manualmente.

### Outros

- :green_circle: Tabelas `ZITMPRI` e `FACILITY` não estão qualificadas com `{schema}`, ao
  contrário das restantes — dependem do schema por omissão do utilizador SQL.
- :green_circle: Valores do intervalo são interpolados com f-string na query. O risco de
  injeção é baixo (vêm da BD), mas é preferível usar parâmetros `:fcy_from`/`:fcy_to`.
- :green_circle: Em `upload_files`, o log `Iniciar upload de N+1 ficheiros` soma 1 mesmo
  sem ficheiro total.
- :green_circle: `generate_total_stock_file` tem anotação `Optional[Path]` mas faz `return`
  vazio; a sociedade `'GN'` e o armazém `'E30'` estão fixos no código.

---

## Base de dados

:orange_circle: **Média** — `DatabaseManager.__init__`

A URI é construída por concatenação; credenciais com `@`, `:`, `/`, `#` ou `%` quebram a
ligação. **Recomendação:**

```python
from sqlalchemy import URL

self.db_uri = URL.create(
    'mssql+pymssql',
    username=settings.USERNAME,
    password=settings.PASSWORD,
    host=settings.SERVER,
    port=settings.PORT,
    database=settings.DATABASE,
)
```

:green_circle: Um novo *engine* é criado e destruído em cada `with DatabaseManager()`.
Para a frequência atual é aceitável.

---

## SFTP

- :orange_circle: `known_hosts` é procurado em `Path.cwd()`: se o processo for lançado a partir
  de outro diretório (serviço, tarefa agendada) a ligação falha. Considerar `BASE_DIR` ou uma
  variável `SFTP_KNOWN_HOSTS`.
- :green_circle: `list_files` devolve também subdiretórios; usar `listdir_attr` e filtrar por
  `stat.S_ISREG` evita tentativas de download de pastas.
- :green_circle: `__init__` inicializa `sftp_client` duas vezes e `transport` não é usado.

---

## Receção

:orange_circle: **Média** — todos os ficheiros de todas as lojas são descarregados para a mesma
pasta; ficheiros com o mesmo nome em lojas diferentes **sobrescrevem-se** (e o original já foi
apagado do SFTP). Considerar prefixar com o código da loja ou verificar existência antes.

---

## Agendamento

:white_check_mark: **Resolvido** — o `Scheduler` foi reescrito sem a biblioteca `schedule`: as
execuções estão alinhadas ao relógio, sem deriva, e todas as tarefas têm as falhas isoladas.

- :green_circle: `SCHEDULE_MONTHS` não consta do `.env.exemple`.
- :orange_circle: Execuções previstas enquanto o processo está parado não são recuperadas. É
  mais relevante nos emails de satisfação, que só correm uma vez por dia.

---

## Logging

- :orange_circle: Sem rotação de ficheiros (`FileHandler`). Recomendado
  `logging.handlers.TimedRotatingFileHandler` (ex.: rotação diária, 30 dias).
- :green_circle: Os módulos usam `logging.info(...)` (logger *root*) em vez de
  `logging.getLogger(__name__)`; em `database/manager.py` o `logger` é criado mas não usado.
  Perde-se a identificação do módulo de origem no log.
- :green_circle: O formatador `detailed` está definido mas não é usado.

---

## Projeto e manutenção

- :green_circle: `README.md` contém apenas o título — apontar para esta documentação.
- :green_circle: `pyproject.toml` tem `description = "Add your description here"`.
- :green_circle: `requirements.txt` duplica o `uv.lock` e pode desatualizar-se; gerar com
  `uv export --no-dev -o requirements.txt` ou removê-lo.
- :green_circle: `.env.exemple` → nome habitual é `.env.example`.
- :green_circle: Sem testes automatizados. As funções puras (`_get_remote_destination`,
  `TaskSchedule.next_after`, `generate_*_file`) são boas candidatas a testes com
  `pytest`.
- :green_circle: *Docstrings* heterogéneas (algumas em estilo Sphinx `:param:`, a maioria só
  com descrição, e `setup_logging`/`Scheduler.__init__` sem nenhuma). Uniformizar num estilo
  (Google ou Sphinx) enriquece a [Referência da API](referencia/index.md).

## Resumo de ações recomendadas

- [x] Corrigir a condição `minute == 0` da sincronização de stocks.
- [x] Reduzir o `sleep` do ciclo do `Scheduler`.
- [ ] Validar o intervalo de armazéns vazio em `StockSync`.
- [ ] Construir a URI da BD com `URL.create`.
- [ ] Fechar a BD se o SFTP falhar em `ExportSync.__enter__`.
- [ ] Evitar sobrescrita de ficheiros descarregados com o mesmo nome.
- [ ] Rotação de logs e `getLogger(__name__)` em todos os módulos.
- [ ] Clarificar valores de retorno ("sem trabalho" ≠ "erro").
- [ ] Testes unitários para as funções puras.

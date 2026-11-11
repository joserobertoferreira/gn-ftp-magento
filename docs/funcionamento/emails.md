# Emails de satisfação

Implementada por [`sync_customer_emails()`][app.services.file_handler.sync_customer_emails] com a
classe [`CustomerEmailSync`][app.services.customer_email_sync.CustomerEmailSync].

!!! info "Frequência"
    Por omissão, diariamente às 06:00. Ver [Agendamento](agendamento.md).

## Extração

```sql
SELECT * FROM {schema}.YVWMAIRES WHERE CREDAT_0 = {ontem, AAAAMMDD}
```

A data é sempre a do dia anterior ao processamento. `BPCNAM_0` e `WEB_0` são gravados sem espaços
nas pontas.

## Ficheiros gerados

Um ficheiro por valor de `LOJA_0`, em CSV **sem cabeçalho**, separador `;`, codificação UTF-8.
O nome usa a data da extração (`AAMMDD`) seguida da hora do processamento (`hhmmss`). Se não
houver registos para um dos valores, esse ficheiro não é gerado.

| `LOJA_0` | Ficheiro               | Subpasta em `EMAIL_ARCHIVE_PATH` |
| -------- | ---------------------- | -------------------------------- |
| `0`      | `MS_AAMMDDhhmmss.txt`  | `Site`                           |
| `1`      | `ML_AAMMDDhhmmss.txt`  | `Lojas`                          |

| # | Coluna     | Formato    |
| - | ---------- | ---------- |
| 1 | `NUM_0`    |            |
| 2 | `INVDAT_0` | `AAAAMMDD` |
| 3 | `BPCINV_0` |            |
| 4 | `BPCNAM_0` | Sem espaços nas pontas |
| 5 | `WEB_0`    | Sem espaços nas pontas |

## Envio e arquivo

Cada ficheiro é enviado para `{SFTP_SYNC_BASE_PATH}/Emails/`. Se o envio tiver sucesso, é copiado
para a subpasta respetiva de `EMAIL_ARCHIVE_PATH`. Os ficheiros são gerados diretamente em
`LOCAL_ARCHIVE_PATH` e lá ficam, mesmo que o envio falhe.
A pasta `Emails` tem de existir no SFTP, porque a aplicação não cria pastas remotas.

!!! warning "Execuções manuais"
    Como o nome inclui a hora do processamento, voltar a processar o mesmo dia gera e envia
    ficheiros novos, sem substituir os anteriores.

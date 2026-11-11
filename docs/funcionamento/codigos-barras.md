# Códigos de barras

Implementada por [`sync_barcodes()`][app.services.file_handler.sync_barcodes] com a classe
[`BarcodeSync`][app.services.barcode_sync.BarcodeSync].

!!! info "Frequência"
    Por omissão, diariamente às 08:00, 12:00 e 16:00. Ver [Agendamento](agendamento.md).

## Extração

Uma linha por código de barras preenchido dos artigos com `STOMGTCOD_0 > 1`, ordenada por
`TSICOD_0` e `ITMREF_0`.

| Código de barras | Coeficiente (`PCUSTUCOE_0` no ficheiro) |
| ---------------- | --------------------------------------- |
| `EANCOD_0`       | `1`                                     |
| `ZEANCOD_0`      | `PCUSTUCOE_0` (inteiro)                 |
| `ZEANCOD_1`      | `PCUSTUCOE_1` (inteiro)                 |
| `ZEANCOD_2`      | `PCUSTUCOE_2` (inteiro)                 |
| `ZEANCOD_3`      | `PCUSTUCOE_3` (inteiro)                 |

!!! note "Códigos repetidos"
    A extração não remove repetições: um EAN preenchido em mais do que um campo do artigo gera
    uma linha por campo.

## Ficheiro gerado — `EANSAAMMDDhhmmss.txt`

CSV **sem cabeçalho**, separador `;`, codificação UTF-8, gravado diretamente em `LOCAL_ARCHIVE_PATH`.

| # | Coluna      | Descrição                        |
| - | ----------- | -------------------------------- |
| 1 | `ITMREF_0`  | Referência do artigo             |
| 2 | `ZITMDES_0` | Descrição (sem espaços extremos) |
| 3 | `TSICOD_0`  | Grupo estatístico 1              |
| 4 | `TSICOD_1`  | Grupo estatístico 2              |
| 5 | `EANCOD_0`  | Código de barras                 |
| 6 | `PCUSTUCOE_0` | Coeficiente da unidade do código (`1` para `EANCOD_0`) |

## Envio e arquivo

Tal como nos [stocks](stocks.md#4-envio-e-arquivo), o ficheiro enviado com sucesso é copiado para
o storage; o ficheiro local fica em `LOCAL_ARCHIVE_PATH`.

| Destino | Pasta                                  |
| ------- | -------------------------------------- |
| SFTP    | `{SFTP_SYNC_BASE_PATH}/EANS/`          |
| Storage | `{STORAGE_ARCHIVE_PATH}\STOCK\EANS`    |

A pasta `EANS` tem de existir no SFTP, porque a aplicação não cria pastas remotas.

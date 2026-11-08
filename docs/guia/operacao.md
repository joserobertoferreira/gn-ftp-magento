# Execução e operação

## Executar

```bash
uv run main.py
```

O comportamento depende de `SCHEDULE_ENABLED`:

- **`True`** — processo contínuo; cada tarefa corre no seu horário. Termina com ++ctrl+c++.
- **`False`** — executa **uma única vez** e termina (útil para testes ou para agendar via
  Agendador de Tarefas do Windows / cron). Ver
  [Execução única](../funcionamento/agendamento.md#execucao-unica).

## Tarefas

As tarefas estão definidas em [`build_tasks()`][main.build_tasks], por esta ordem:

1. **Stock**. Ver [Sincronização de stocks](../funcionamento/stocks.md).
2. **Códigos de barras**. Ver [Códigos de barras](../funcionamento/codigos-barras.md).
3. **Emails de satisfação**. Ver [Emails de satisfação](../funcionamento/emails.md).
4. **Transferência de ficheiros**: [envio](../funcionamento/envio.md) da pasta
   `LOCAL_EXPORT_PATH` para o SFTP e [receção](../funcionamento/rececao.md) do SFTP para
   `LOCAL_IMPORT_PATH`.

!!! note "Falhas são isoladas"
    Uma falha numa tarefa é registada em log mas não impede as restantes.

## Executar como serviço no Windows

Como o modo agendado é um ciclo infinito, a aplicação pode ser instalada como serviço,
por exemplo com o [NSSM](https://nssm.cc/):

```powershell
nssm install GN-FTP-Magento "D:\Python\Sage\GN\magento\.venv\Scripts\python.exe" "main.py"
nssm set GN-FTP-Magento AppDirectory "D:\Python\Sage\GN\magento"
nssm start GN-FTP-Magento
```

!!! important "Diretório de trabalho"
    `AppDirectory` tem de ser a raiz do projeto: o ficheiro `known_hosts`, o `.env` e a pasta
    `logs/` são resolvidos relativamente ao diretório de trabalho.

!!! warning "Acesso às partilhas de rede"
    A conta do serviço tem de ter permissão de escrita em `STORAGE_ARCHIVE_PATH` e
    `EMAIL_ARCHIVE_PATH`. A conta `LocalSystem`, usada por omissão, normalmente não tem acesso a
    partilhas de outras máquinas. Use caminhos UNC (`\\servidor\partilha`), não unidades mapeadas,
    e configure uma conta com acesso:

    ```powershell
    nssm set GN-FTP-Magento ObjectName "DOMINIO\utilizador" "palavra-passe"
    ```

## Logs

| Destino              | Nível mínimo | Conteúdo                                     |
| -------------------- | ------------ | -------------------------------------------- |
| Consola              | `INFO`       | Progresso geral.                             |
| `logs/app_info.log`  | `INFO`       | Histórico completo das execuções.            |
| `logs/app_error.log` | `ERROR`      | Apenas erros — o primeiro sítio a consultar. |

Formato: `AAAA-MM-DD HH:MM:SS,mmm - NÍVEL - logger - mensagem`.

!!! warning "Sem rotação"
    Os ficheiros são abertos em modo *append* e **não têm rotação**; crescem indefinidamente.
    Ver [Análise técnica](../analise-tecnica.md#logging).

## Resolução de problemas

| Sintoma no log                                                      | Causa provável / ação                                                          |
| ------------------------------------------------------------------- | ------------------------------------------------------------------------------ |
| `UndefinedValueError: LOCAL_EXPORT_PATH not found`                  | Falta a variável no `.env`.                                                    |
| `Ficheiro 'known_hosts' não encontrado no diretório do projeto`     | Executar a partir da raiz do projeto ou criar o ficheiro (ver [Configuração](configuracao.md#ficheiro-known_hosts)). |
| `ERRO DE CHAVE DO HOST`                                             | A chave do servidor SFTP mudou. Confirmar com o fornecedor e atualizar `known_hosts`. |
| `Server '...' not found in known_hosts`                             | O host/porta não constam do `known_hosts` (atenção ao formato `[host]:porta` para portas ≠ 22). |
| `Falha na autenticação SFTP`                                        | `SFTP_USER` / `SFTP_PASSWORD` incorretos.                                      |
| `Erro ao conectar ao banco de dados via SQLAlchemy`                 | Servidor/porta/credenciais SQL; firewall; caracteres especiais na palavra-passe. |
| `Nenhuma loja ativa encontrada no banco de dados`                   | Tabela `ZLOJAS` vazia no schema configurado.                                   |
| `Problemas com a transferência de ficheiros para o Magento.`        | Frequentemente apenas significa que **não havia ficheiros** a enviar — ver [Análise técnica](../analise-tecnica.md#valores-de-retorno-ambiguos). |
| Ficheiros ficam esquecidos em `LOCAL_EXPORT_PATH`                   | O nome não corresponde a nenhuma [regra de roteamento](../funcionamento/envio.md#regras-de-roteamento), ou o upload falhou. Ficheiros de stock, `EANS*`, `MS_*` e `ML_*` não são reenviados automaticamente; confirmar que a pasta remota existe. |
| `Falha ao copiar '...' para o storage`                              | Partilha indisponível ou sem permissão de escrita. O ficheiro já foi enviado para o SFTP e fica em `LOCAL_ARCHIVE_PATH`, de onde pode ser copiado manualmente. |
| `A tarefa '...' terminou sem sucesso.`                              | Consultar os erros anteriores em `logs/app_error.log` (falha na query ou no envio). |

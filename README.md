# GN-FTP-Magento

Integração Sage X3 ↔ Magento via SFTP: sincronização de stocks, códigos de barras, emails de
satisfação de clientes, envio de ficheiros EDI e receção de encomendas/recolhas.

## Início rápido

```bash
uv sync
cp .env.exemple .env   # preencher as variáveis
uv run main.py
```

## Documentação

A documentação completa (instalação, configuração, fluxos, referência da API e análise técnica)
está em `docs/` e é gerada com MkDocs:

```bash
uv run --group docs mkdocs serve    # http://127.0.0.1:8765
uv run --group docs mkdocs build    # gera a pasta site/
```

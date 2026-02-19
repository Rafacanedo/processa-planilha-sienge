# Processa Planilha Sienge

Aplicação Streamlit para processamento e normalização de planilhas de orçamentos, ajustando a hierarquia de itens para o padrão de 4 níveis (XXX.XXX.XXX.XXX) exigido pelo sistema Sienge.

## Funcionalidades
- Upload de arquivos `.xlsx`.
- Mapeamento flexível de colunas (Item, Descrição, Código, Unidade, Preço, Quantidade).
- Normalização automática da estrutura hierárquica.
- Download da planilha processada.

## Como rodar
1. Instale as dependências:
   ```bash
   pip install -r requirements.txt
   ```
2. Execute a aplicação:
   ```bash
   streamlit run app.py
   ```


import streamlit as st
import pandas as pd
from io import BytesIO
import sys
import openpyxl

# Add current directory to path to import pipeline
sys.path.append(".")
import pipeline



st.set_page_config(page_title="Processa planilha para o Sienge", page_icon="📊", layout="wide")

st.title("📊 Processa planilha para o Sienge")
st.markdown("""
Faça o upload de uma planilha para processar a hierarquia de itens.
O pipeline identifica tarefas (itens com código e unidade) em qualquer nível e normaliza para o Nível 4.
""")

# File Uploader first
uploaded_file = st.file_uploader("Escolha um arquivo Excel (.xlsx)", type=["xlsx"])


if uploaded_file is not None:
    st.info(f"Arquivo carregado: {uploaded_file.name}")
    
    # Load workbook to get sheet names
    try:
        # We need to save the stream position to reset it later
        uploaded_file.seek(0)
        wb = openpyxl.load_workbook(uploaded_file, read_only=True, data_only=True)
        sheet_names = wb.sheetnames
        wb.close()
    except Exception as e:
        st.error(f"Erro ao ler abas do arquivo: {e}")
        sheet_names = []

    if sheet_names:
        selected_sheet = st.selectbox("Selecione a Aba", sheet_names)
    else:
        selected_sheet = None


    # Configuration - Column Mapping (Now below uploader)
    st.subheader("Configuração das Colunas")
    st.caption("Insira o MÚMERO da coluna conforme mostrado na pré-visualização (Indices começam em 0).")
    
    col1, col2, col3 = st.columns(3)
    col4, col5, col6 = st.columns(3)
    
    # Defaults based on 0-based index:
    # A=0, B=1, C=2, D=3, E=4, F=5, S=18
    def_item = 1
    def_desc = 2
    def_code = 3
    def_unit = 4
    def_price = 5
    def_qty = 18
    def_start = 7

    with col1:
        item_col = st.number_input("Coluna ITEM (Índice)", min_value=0, value=def_item)
    with col2:
        desc_col = st.number_input("Coluna DESCRIÇÃO (Índice)", min_value=0, value=def_desc)
    with col3:
        code_col = st.number_input("Coluna CÓDIGO (Índice)", min_value=0, value=def_code)
    
    with col4:
        unit_col = st.number_input("Coluna UNID. (Índice)", min_value=0, value=def_unit)
    with col5:
        price_col = st.number_input("Coluna PREÇO (Índice)", min_value=0, value=def_price)
    with col6:
        qty_col = st.number_input("Coluna QUANTIDADE (Índice)", min_value=0, value=def_qty)
        
    start_row = st.number_input("Linha Inicial de Dados", min_value=1, value=def_start)

    # Preview
    if selected_sheet:
        try:
            # Reset pointer for preview
            uploaded_file.seek(0)
            df_preview = pd.read_excel(uploaded_file, sheet_name=selected_sheet, header=None, skiprows=start_row-2, nrows=5)
            st.subheader("Pré-visualização (Topo)")
            st.dataframe(df_preview)
        except Exception as e:
            st.error(f"Erro ao visualizar arquivo: {e}")

    if st.button("Executar Pipeline"):
        with st.spinner("Processando..."):
            try:
                # Reset pointer for processing
                uploaded_file.seek(0)
                
                # Setup mapping
                mapping = pipeline.ColumnMapping(
                    item_col=int(item_col),
                    desc_col=int(desc_col),
                    code_col=int(code_col),
                    unit_col=int(unit_col),
                    price_col=int(price_col),
                    qty_col=int(qty_col),
                    start_row=int(start_row)
                )


                # READ
                items = pipeline.read_input(uploaded_file, mapping, sheet_name=selected_sheet)
                
                # TRANSFORM
                output_rows = pipeline.transform(items)
                
                # WRITE TO BUFFER
                output_buffer = BytesIO()
                pipeline.write_output(output_rows, output_buffer)
                
                st.success("Processamento concluído com sucesso!")
                
                st.download_button(
                    label="Baixar Planilha Processada",
                    data=output_buffer.getvalue(),
                    file_name="planilha_final.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
                
            except Exception as e:
                st.error(f"Ocorreu um erro: {e}")
                # Print stack trace in expander
                import traceback
                with st.expander("Detalhes do Erro"):
                    st.code(traceback.format_exc())


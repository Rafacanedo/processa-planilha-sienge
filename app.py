
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
O pipeline normaliza os itens para o Nível 4 (XXX.XXX.XXX.XXX).
""")

# File Uploader first
uploaded_file = st.file_uploader("Escolha um arquivo Excel (.xlsx)", type=["xlsx"])

if uploaded_file is not None:
    st.info(f"Arquivo carregado: {uploaded_file.name}")
    
    # Configuration - Column Mapping (Now below uploader)
    st.subheader("Configuração das Colunas")
    st.caption("Verifique se as colunas correspondem ao seu arquivo.")
    
    col1, col2, col3 = st.columns(3)
    col4, col5, col6 = st.columns(3)
    
    def_item = "B"
    def_desc = "C"
    def_code = "D"
    def_unit = "E"
    def_price = "F"
    def_qty = "S"
    def_start = 7

    with col1:
        item_col = st.text_input("Coluna ITEM", def_item)
    with col2:
        desc_col = st.text_input("Coluna DESCRIÇÃO", def_desc)
    with col3:
        code_col = st.text_input("Coluna CÓDIGO", def_code)
    
    with col4:
        unit_col = st.text_input("Coluna UNID.", def_unit)
    with col5:
        price_col = st.text_input("Coluna PREÇO", def_price)
    with col6:
        qty_col = st.text_input("Coluna QUANTIDADE", def_qty)
        
    start_row = st.number_input("Linha Inicial de Dados", min_value=1, value=def_start)

    # Preview
    try:
        # Reset pointer for preview
        uploaded_file.seek(0)
        df_preview = pd.read_excel(uploaded_file, header=None, skiprows=start_row-2, nrows=5)
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
                    item_col=item_col,
                    desc_col=desc_col,
                    code_col=code_col,
                    unit_col=unit_col,
                    price_col=price_col,
                    qty_col=qty_col,
                    start_row=int(start_row)
                )

                # READ
                items = pipeline.read_input(uploaded_file, mapping)
                
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


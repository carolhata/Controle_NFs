# Seu código Streamlit_app.py

import streamlit as st
import os
import time
import pandas as pd
import pytesseract
from PIL import Image
import re
# Importar cv2 se você planeja usar o OpenCV. 
# Se não for usar, pode remover a menção a ele no código para evitar a mensagem de "não encontrado".
# import cv2 

# >>> ESTA DEVE SER A PRIMEIRA LINHA STREAMLIT <<<
st.set_page_config(page_title="Controle NF - Folder Watcher", layout="wide")

# O restante do seu código Streamlit viria aqui:

st.title("Monitoramento de Pastas para NFS")

# Exemplo de como você pode verificar se o pytesseract está configurado
# e lidar com a ausência do OpenCV
try:
    # Verifique se o tesseract está no PATH ou especifique o caminho completo
    # Ex: pytesseract.pytesseract.tesseract_cmd = r'C:\Program Files\Tesseract-OCR\tesseract.exe' (Windows)
    # Ex: pytesseract.pytesseract.tesseract_cmd = '/usr/bin/tesseract' (Linux)
    # Se você está usando Streamlit Cloud, provavelmente precisará instalar o tesseract via apt-get na configuração do seu ambiente.
    pytesseract.get_tesseract_version()
except pytesseract.TesseractNotFoundError:
    st.warning("Tesseract-OCR não encontrado. O OCR não funcionará. Por favor, instale o Tesseract-OCR e configure o PATH, se necessário.")

# Função para processar imagem (se você usa OpenCV aqui, é onde ele precisaria estar importado)
def process_image_for_ocr(image_path):
    # Exemplo sem OpenCV - apenas PIL
    img = Image.open(image_path)
    # Você pode adicionar algum pré-processamento básico da PIL aqui, se necessário
    # Por exemplo: img = img.convert('L') # Converter para escala de cinza
    return img

def extract_cnpj(text):
    # Sua lógica de extração de CNPJ
    cnpj_pattern = r'\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2}'
    match = re.search(cnpj_pattern, text)
    if match:
        return match.group(0)
    return "Não encontrado"

def extract_value(text):
    # Sua lógica de extração de valor
    value_pattern = r'VALOR TOTAL\s*R\$\s*(\d{1,3}(?:\.\d{3})*,\d{2})'
    match = re.search(value_pattern, text, re.IGNORECASE)
    if match:
        return match.group(1)
    return "Não encontrado"


# Resto da sua aplicação Streamlit
folder_path = st.text_input("Insira o caminho da pasta para monitorar:", "./nf_folder")

if st.button("Iniciar Monitoramento"):
    if not os.path.exists(folder_path):
        st.error(f"A pasta '{folder_path}' não existe. Por favor, crie-a ou insira um caminho válido.")
    else:
        st.write(f"Monitorando a pasta: {folder_path}")
        placeholder = st.empty()
        
        while True:
            new_files = []
            for filename in os.listdir(folder_path):
                if filename.lower().endswith(('.png', '.jpg', '.jpeg', '.pdf')):
                    file_path = os.path.join(folder_path, filename)
                    if file_path not in st.session_state.get('processed_files', set()):
                        new_files.append(file_path)
            
            if new_files:
                for file_path in new_files:
                    try:
                        st.write(f"Processando novo arquivo: {os.path.basename(file_path)}")
                        
                        # Use a função de processamento de imagem
                        processed_image = process_image_for_ocr(file_path)
                        text = pytesseract.image_to_string(processed_image, lang='por') # Assumindo português

                        cnpj = extract_cnpj(text)
                        value = extract_value(text)

                        # Exibir informações (você pode querer armazenar isso em um DataFrame)
                        placeholder.write(f"Arquivo: {os.path.basename(file_path)}, CNPJ: {cnpj}, Valor: {value}")
                        
                        # Adicione o arquivo aos processados
                        if 'processed_files' not in st.session_state:
                            st.session_state.processed_files = set()
                        st.session_state.processed_files.add(file_path)
                        
                    except Exception as e:
                        st.error(f"Erro ao processar {os.path.basename(file_path)}: {e}")
            
            time.sleep(5) # Verifica a cada 5 segundos

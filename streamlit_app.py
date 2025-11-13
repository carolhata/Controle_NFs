import streamlit as st
import pandas as pd
import io
import os
import re
import tempfile
from datetime import datetime
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import pdfplumber
import magic
import pytesseract
from PIL import Image

# Para o pré-processamento de imagens com OpenCV (se instalado)
try:
    import cv2
except ImportError:
    st.warning("OpenCV não encontrado. O pré-processamento de imagens para OCR será limitado.")
    cv2 = None

from gspread import authorize
from gspread_dataframe import set_with_dataframe

st.set_page_config(page_title="Controle NF - Folder Watcher", layout="wide")
st.title("📁 Monitor de pasta Drive → 🧾 → 📊 Google Sheets")

# ---------- SCOPES ----------
SCOPES = [
    "https://www.googleapis.com/auth/drive.readonly",
    "https://www.googleapis.com/auth/spreadsheets"
]

# ---------- Autenticação ----------
@st.cache_resource
def get_google_creds():
    if "GOOGLE_SERVICE_ACCOUNT" not in st.secrets:
        st.error("Coloque o JSON da Service Account em Settings → Secrets como GOOGLE_SERVICE_ACCOUNT.")
        st.stop()
    sa = st.secrets["GOOGLE_SERVICE_ACCOUNT"]
    import json
    if isinstance(sa, str):
        sa_info = json.loads(sa)
    else:
        sa_info = sa
    creds = Credentials.from_service_account_info(sa_info, scopes=SCOPES)
    return creds

def build_drive_service(creds):
    return build("drive", "v3", credentials=creds, cache_discovery=False)

def build_sheets_client(creds):
    return authorize(creds)

# ---------- Drive helpers ----------
def list_files_in_folder(service, folder_id, page_size=1000):
    """
    Lista arquivos em uma pasta Drive. Retorna lista de dicts {id, name, mimeType, modifiedTime}
    """
    files = []
    q = f"'{folder_id}' in parents and trashed=false"
    page_token = None
    while True:
        resp = service.files().list(q=q,
                                    spaces='drive',
                                    fields="nextPageToken, files(id, name, mimeType, modifiedTime, size)",
                                    pageToken=page_token,
                                    pageSize=page_size).execute()
        files.extend(resp.get("files", []))
        page_token = resp.get('nextPageToken', None)
        if not page_token:
            break
    return files

def download_drive_file(service, file_id, dest_path, mime=None):
    meta = service.files().get(fileId=file_id, fields="id,name,mimeType").execute()
    mime = meta.get("mimeType")
    name = meta.get("name")
    
    # Tratamento para Google Docs (exportar como texto simples)
    if mime == "application/vnd.google-apps.document":
        request = service.files().export_media(fileId=file_id, mimeType="text/plain")
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
        fh.seek(0)
        with open(dest_path, "wb") as f:
            f.write(fh.read())
        return dest_path, mime # Retorna o mime original para registro
    else:
        request = service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
        fh.seek(0)
        with open(dest_path, "wb") as f:
            f.write(fh.read())
        return dest_path, mime # Retorna o mime original para registro

# ---------- Extração de texto ----------
def extract_text_from_pdf(path):
    text = ""
    try:
        with pdfplumber.open(path) as pdf:
            for p in pdf.pages:
                pt = p.extract_text()
                if pt:
                    text += pt + "\n"
    except Exception as e:
        st.warning(f"pdfplumber falhou ao extrair texto direto: {e}")
    return text

def preprocess_image_for_ocr(image):
    """
    Aplica pré-processamento básico à imagem para melhorar o OCR.
    Requer OpenCV.
    """
    if cv2 is None:
        return image # Retorna a imagem original se OpenCV não estiver disponível

    img_np = cv2.cvtColor(np.array(image), cv2.COLOR_RGB2BGR)
    
    # Converter para tons de cinza
    gray = cv2.cvtColor(img_np, cv2.COLOR_BGR2GRAY)
    
    # Binarização (thresholding adaptativo para diferentes iluminações)
    # Tenta melhorar o contraste entre texto e fundo
    thresh = cv2.adaptiveThreshold(gray, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY, 11, 2)
    
    # Opcional: Remover ruído (comentado, pois nem sempre é benéfico)
    # denoised = cv2.medianBlur(thresh, 3) 
    
    return Image.fromarray(thresh) # Retorna como objeto PIL Image

def extract_text_via_ocr(path):
    text = ""
    try:
        if path.lower().endswith(".pdf"):
            with pdfplumber.open(path) as pdf:
                for i, p in enumerate(pdf.pages):
                    st.info(f"Aplicando OCR na página {i+1} do PDF...")
                    im = p.to_image(resolution=300).original # Aumentar resolução para melhor OCR
                    
                    # Pré-processamento se OpenCV estiver disponível
                    if cv2:
                        import numpy as np # Importar numpy aqui, pois cv2 o utiliza
                        processed_im = preprocess_image_for_ocr(im)
                        # Opcional: mostrar a imagem pré-processada para depuração
                        # st.image(processed_im, caption=f"Página {i+1} pré-processada para OCR")
                    else:
                        processed_im = im

                    # Usar o modo de segmentação de página (PSM) para tentar melhorar a detecção de blocos de texto
                    # psm 3: Default, assume uma página de texto. Bom para layout geral.
                    # psm 6: Assume um único bloco de texto uniforme. Pode ser bom para recibos simples.
                    # psm 1: Auto OSD (Orientação e detecção de script)
                    # Vamos tentar com 3 e se o texto for muito curto, tentar com 6
                    
                    # Tentar com PSM 3 primeiro (padrão para maioria dos documentos)
                    txt = pytesseract.image_to_string(processed_im, lang='por+eng', config='--psm 3')
                    
                    if not txt.strip() or len(txt.strip()) < 50: # Se o resultado for muito curto, tentar com PSM 6
                        st.info("Texto curto com PSM 3, tentando PSM 6 (single block)...")
                        txt = pytesseract.image_to_string(processed_im, lang='por+eng', config='--psm 6')

                    text += txt + "\n"
        else: # Para imagens diretas (JPG, PNG, etc.)
            st.info("Aplicando OCR em arquivo de imagem...")
            im = Image.open(path)
            if cv2:
                import numpy as np
                processed_im = preprocess_image_for_ocr(im)
            else:
                processed_im = im
            
            txt = pytesseract.image_to_string(processed_im, lang='por+eng', config='--psm 3')
            if not txt.strip() or len(txt.strip()) < 50:
                st.info("Texto curto com PSM 3, tentando PSM 6 (single block)...")
                txt = pytesseract.image_to_string(processed_im, lang='por+eng', config='--psm 6')
            text += txt
    except Exception as e:
        st.warning(f"OCR falhou: {e}")
    return text

# ---------- Parsers ----------
CNPJ_RE = re.compile(r"(?:CNPJ[:\s]|C.?NPJ[:\s]|CNPJ\s*)?([0-9]{2}[./-]?[0-9]{3}[./-]?[0-9]{3}[/-]?[0-9]{4}[-]?[0-9]{2})")
CPF_RE = re.compile(r"(?:CPF[:\s]|CPF\s)?([0-9]{3}[./-]?[0-9]{3}[./-]?[0-9]{3}[-]?[0-9]{2})")
VAL_RE = re.compile(r"R\$?\s*(\d{1,3}(?:\.\d{3})*(?:,\d{2})|\d+,\d{2})")
# Melhorando DATE_RE para capturar formatos mais variados, incluindo com hora
DATE_RE = re.compile(r"(?:DATA(?:\s*DE\s*EMISSÃO)?[:\s]*|EMISSÃO[:\s]*|DATA/HORA[:\s]*|DATA[:\s]*|DATE[:\s]*)?(\d{1,2}[/-]\d{1,2}[/-]\d{2,4}(?:\s+\d{1,2}:\d{1,2}(?::\d{1,2})?)?|\d{4}-\d{1,2}-\d{1,2})")
# Expressão para número da nota fiscal, mais abrangente
NF_RE = re.compile(r"(?:N(?:.|º|o)?\sF(?:iscal)?[:\s]|Nota\s+Fiscal[:\s]|N[:º\s]|SAT\sNo.?\s|\sNFC-e\s|\sNF-e\s|Nr\s+Documento[:\s]*)([0-9.-]+\s*[A-Z]?\s*[0-9]+(?:\/[0-9]+)?|\d{6,})") # Inclui números longos e formatos como 123.456/7890

def normalize_money(s):
    if not s:
        return None
    s = s.strip()
    s = re.sub(r"[Rr]\$?\s*", "", s) # Remove "R$"
    s = s.replace(".", "") # Remove separador de milhares
    s = s.replace(",", ".") # Troca vírgula por ponto para decimais
    try:
        return float(s)
    except ValueError:
        return None

def extract_fields_from_text(text):
    out = {}
    st.write("--- Texto Bruto do OCR (primeiros 1000 caracteres) ---")
    st.text(text[:1000])
    st.write("-----------------------------------------------------")

    lines = [l.strip() for l in text.splitlines() if l.strip()]

    # 1. Extrair CNPJ (prioritário para identificar a empresa)
    m = CNPJ_RE.search(text)
    out["cnpj"] = m.group(1) if m else None
    if out["cnpj"]: st.write(f"CNPJ encontrado: {out['cnpj']}")

    # 2. Extrair Empresa (Razão Social) - Lógica aprimorada
    company = None
    if out["cnpj"]:
        # Tenta encontrar a empresa na(s) linha(s) acima do CNPJ
        cnpj_pos = text.find(out["cnpj"])
        if cnpj_pos != -1:
            snippet_before_cnpj = text[:cnpj_pos]
            lines_before_cnpj = [l.strip() for l in snippet_before_cnpj.splitlines() if l.strip()]
            
            if lines_before_cnpj:
                # Procura por uma linha que se pareça com o nome de uma empresa
                for i in range(len(lines_before_cnpj) - 1, -1, -1): # Começa da linha imediatamente acima do CNPJ
                    line_candidate = lines_before_cnpj[i]
                    # Heurística: linha com mais de 5 caracteres, predominantemente maiúscula (ou título-case),
                    # e que não seja um endereço ou CPF. Prioriza termos como LTDA, SA, ME, EPP.
                    if (len(line_candidate) > 5 and
                        (line_candidate.isupper() or line_candidate.istitle() or 
                         re.search(r'\b(LTDA|SA|S\.A\.|ME|EPP|EIRELI|COMERCIO|SERVICOS|PREFEITURA)\b', line_candidate.upper())) and
                        not re.search(r'\d{1,2}/\d{1,2}/\d{2,4}|\d{3}\.\d{3}\.\d{3}-\d{2}|RUA|AVENIDA|BAIRRO|CEP|CPF', line_candidate.upper())):
                        company = line_candidate
                        break
    
    # Se ainda não encontrou, tenta buscar em blocos no início do documento
    if not company and lines:
        search_area = "\n".join(lines[:10]) # Primeiras 10 linhas
        company_keywords_strong = ["LTDA", "MEI", "EIRELI", "S.A.", "SA", "COMERCIO", "SERVICOS", "MATERIAIS", "INDUSTRIA", "PREFEITURA", "MUNICÍPIO"]
        for kw in company_keywords_strong:
            for l in search_area.splitlines():
                if kw in l.upper() and len(l) > 10 and \
                   not re.search(r'\d{1,2}/\d{1,2}/\d{2,4}|\d{3}-\d{3}|\d+\,\d+|\d+\.\d+|RUA|AVENIDA|BAIRRO|CEP|CPF|TELEFONE|EMAIL|WWW', l.upper()):
                    company = l.strip()
                    break
            if company:
                break
    
    # Último recurso: a primeira ou segunda linha significativa (muitas vezes é o nome da empresa)
    if not company and lines:
        for l in lines[:3]: # Tenta as 3 primeiras linhas
            if len(l) > 10 and (l.isupper() or l.istitle()) and \
               not re.search(r'\d{1,2}/\d{1,2}/\d{2,4}|CNPJ|CPF|RUA|AVENIDA|BAIRRO|CEP', l.upper()):
                company = l.strip()
                break

    out["empresa"] = company
    if out["empresa"]: st.write(f"Empresa encontrada: {out['empresa']}")
    
    # 3. Extrair CPF
    m = CPF_RE.search(text)
    out["cpf"] = m.group(1) if m else None
    if out["cpf"]: st.write(f"CPF encontrado: {out['cpf']}")

    # 4. Extrair Data de Compra - Priorizando por palavras-chave
    out["data_compra"] = None
    date_keywords = ["DATA DE EMISSÃO", "EMISSÃO", "DATA/HORA", "DATA", "DATE"]
    for kw in date_keywords:
        idx = text.upper().find(kw)
        if idx != -1:
            snippet = text[idx:idx + 60] # Aumenta o snippet
            m = DATE_RE.search(snippet)
            if m:
                out["data_compra"] = m.group(1)
                break
    if not out["data_compra"]: # Se não encontrou com palavras-chave, tenta no texto todo
        m = DATE_RE.search(text)
        out["data_compra"] = m.group(1) if m else None

    if out["data_compra"]: st.write(f"Data de Compra encontrada: {out['data_compra']}")

    # 5. Extrair Número da Nota - Priorizando por palavras-chave
    out["numero_nota"] = None
    nf_keywords = ["NÚMERO:", "NÚMERO DA NOTA", "NOTA FISCAL Nº", "SAT NO.", "Nr Documento", "NF-E", "NFC-E", "DOC:", "NFE:"]
    for kw in nf_keywords:
        idx = text.upper().find(kw)
        if idx != -1:
            snippet = text[idx:idx + 60] # Aumenta o snippet
            m = NF_RE.search(snippet)
            if m:
                out["numero_nota"] = m.group(1)
                break
    if not out["numero_nota"]: # Fallback para busca geral
        m = NF_RE.search(text)
        out["numero_nota"] = m.group(1) if m else None
    if out["numero_nota"]: st.write(f"Número da Nota encontrado: {out['numero_nota']}")

    # 6. Extrair Valor Total
    total = None
    # Prioriza keywords de valor total
    for keyword in ["VALOR TOTAL", "TOTAL DA NOTA", "TOTAL GERAL", "TOTAL A PAGAR", "TOTAL A RECEBER", "SUBTOTAL"]:
        idx = text.upper().find(keyword)
        if idx != -1:
            # Busca o valor numérico após a palavra-chave
            snippet = text[idx:idx + 150]
            mval = VAL_RE.search(snippet)
            if mval:
                total = normalize_money(mval.group(1))
                if total is not None:
                    break
    
    # Se não encontrou por palavra-chave, pega o maior valor numérico no documento
    if total is None:
        all_vals = VAL_RE.findall(text)
        if all_vals:
            nums_parsed = [normalize_money(v) for v in all_vals if normalize_money(v) is not None]
            if nums_parsed:
                total = max(nums_parsed) # Assume que o maior valor é o total

    out["valor_total"] = total
    if out["valor_total"]: st.write(f"Valor Total encontrado: {out['valor_total']}")

    # 7. Extrair Endereço - Lógica aprimorada para blocos de endereço
    address = None
    address_keywords = ["ENDEREÇO", "ENDEREÇO:", "RUA ", "R. ", "AV ", "AVENIDA", "LOGRADOURO", "BAIRRO", "CIDADE", "CEP", "Nº", "NUMERO"]
    
    # Busca por uma palavra-chave de endereço e tenta capturar um bloco de linhas subsequentes
    for keyword in address_keywords:
        idx = text.upper().find(keyword)
        if idx != -1:
            # Pega um snippet maior após a palavra-chave
            snippet_raw = text[idx: idx + 300]
            snippet_lines = [l.strip() for l in snippet_raw.splitlines() if l.strip()]

            potential_address_parts = []
            for al in snippet_lines[:8]: # Limita a busca às próximas 8 linhas
                # Critérios para incluir a linha como parte do endereço
                # Deve conter indicadores de endereço, e não ser outra coisa (CNPJ, telefone, etc.)
                if (any(sub_keyword in al.upper() for sub_keyword in ["RUA", "AV", "BAIRRO", "CEP", "CIDADE", "ESTADO", "Nº", "NUMERO", ",", "EDF", "APTO"]) or 
                    re.search(r'\d{5}-\d{3}|\d+\,\d+', al)) and # Contém CEP ou número com vírgula
                   len(al) > 5 and \
                   not re.search(r'CNPJ|CPF|INSCRIÇÃO|IE|TELEFONE|CELULAR|E-MAIL|HTTP|VALOR|TOTAL|IMPOSTO|IEST|IMPOSTO', al.upper()):
                    
                    potential_address_parts.append(al)
                elif len(potential_address_parts) > 0 and \
                     len(al) > 5 and \
                     not re.search(r'CNPJ|CPF|INSCRIÇÃO|IE|TELEFONE|CELULAR|E-MAIL|HTTP|VALOR|TOTAL|IMPOSTO|IEST|IMPOSTO', al.upper()):
                    # Se já encontrou partes de endereço, inclui linhas subsequentes que pareçam continuar o endereço
                    potential_address_parts.append(al)
                else:
                    # Quebra se a linha não for um endereço e não for uma continuação
                    if len(potential_address_parts) > 0:
                        break

            if potential_address_parts:
                address = " ".join(potential_address_parts)
                # Tentar limpar o endereço de "CEP" se ele estiver no início
                address = re.sub(r'CEP\s*[:-]?\s*', '', address, flags=re.IGNORECASE).strip()
                break
    out["endereco"] = address
    if out["endereco"]: st.write(f"Endereço encontrado: {out['endereco']}")

    # 8. Extrair Descrição de Itens - Lógica aprimorada
    items=[]
    # Palavras-chave para ignorar linhas que não são itens de descrição (mais abrangente)
    ignore_keywords_items = [
        "SUBTOTAL", "TOTAL", "IMPOSTOS", "ICMS", "ISS", "DESCONTO", "VALOR TOTAL", "TOTAL DA NOTA",
        "FORMA DE PAGAMENTO", "TROCO", "CRÉDITO", "DÉBITO", "DINHEIRO", "VALOR", 
        "CPF DO CONSUMIDOR", "CNPJ", "IE", "CUPOM FISCAL", "SAT NO", "NOTA FISCAL", 
        "LANÇAMENTO", "DATA", "CÓDIGO", "UNIDADE", "QTD", "PRODUTO", "DESCRIÇÃO", 
        "VALOR UNITÁRIO", "VALOR TOTAL", "CLIENTE", "CONSUMIDOR", "ENDEREÇO", "BAIRRO", 
        "CIDADE", "CEP", "CONTATO", "TELEFONE", "CNPJ/CPF", "CPF/CNPJ", "IEST",
        "DISCRIMINAÇÃO DOS SERVIÇOS", "TOTAL DO SERVIÇO", "PRESTADOR DE SERVIÇOS", "TOMADOR DE SERVIÇOS"
    ]
    
    start_keywords = ["ITENS", "DESCRIÇÃO", "PRODUTOS E SERVIÇOS", "DETALHES DA VENDA", "CUPOM FISCAL", 
                      "NOTA FISCAL", "DISCRIMINAÇÃO", "VALOR TOTAL DO SERVIÇO", "SERVIÇOS PRESTADOS", 
                      "ITEM","QTD","UN","VALOR"]
    end_keywords = ["VALOR TOTAL", "SUBTOTAL", "TOTAL A PAGAR", "FORMAS DE PAGAMENTO", "OBSERVAÇÕES", 
                    "INFORMAÇÕES ADICIONAIS", "PAGAMENTO", "TOTAL GERAL", "DADOS BANCÁRIOS", 
                    "IMPOSTOS", "TOTAL LIQUIDO"]

    in_items_section = False
    
    for line in text.splitlines():
        line_clean = line.strip()
        line_upper = line_clean.upper()

        # Verifica o início da seção de itens
        if not in_items_section:
            if any(kw in line_upper for kw in start_keywords) and len(line_clean) < 100: # Evitar capturar linhas muito longas como "start"
                in_items_section = True
                continue
        
        # Se estamos na seção de itens
        if in_items_section:
            # Verifica o fim da seção de itens
            if any(kw in line_upper for kw in end_keywords):
                break 

            # Critérios para uma linha ser considerada um item:
            # 1. Contém um valor monetário (ex: 10,00 ou 10.00) E tem algum texto
            # 2. Contém um padrão de quantidade (ex: "1 X", "2 UN") e um valor
            # 3. É uma linha de texto razoavelmente longa com alguns números
            is_potential_item = (
                (re.search(r"\d+,\d{2}|\d+\.\d{2}", line_clean) and re.search(r"[a-zA-Z]", line_clean) and len(line_clean) > 5) or
                (re.search(r"^\d+(\s*[X\*x]\s*)?\d*\s*(?:UN|QTDE|PÇ|KG|LT|M)\s*[a-zA-Z0-9\s]+?\s+\d+,\d{2}|\d+\.\d{2}", line_clean)) or
                (re.search(r"[a-zA-Z]{5,}", line_clean) and re.search(r"\d+", line_clean) and len(line_clean) > 10)
            )

            if is_potential_item:
                # Verifica se a linha NÃO contém palavras-chave de ignorar (case-insensitive)
                if not any(keyword in line_upper for keyword in ignore_keywords_items):
                    items.append(line_clean)
                    
    out["itens_descricoes"] = "\n".join(items[:40]) if items else None
    if out["itens_descricoes"]: st.write(f"Itens encontrados:\n{out['itens_descricoes']}")

    return out

# ---------- UI inputs ----------
st.sidebar.header("Parâmetros")
creds_check = get_google_creds()

drive_folder_id = st.sidebar.text_input("ID da pasta no Google Drive (folderId)", value=st.secrets.get("DRIVE_FOLDER_ID", ""))
spreadsheet_id = st.sidebar.text_input("ID do Google Sheets (spreadsheetId)", value=st.secrets.get("GOOGLE_SHEET_ID", ""))
sheet_tab = st.sidebar.text_input("Nome da aba para dados", value=st.secrets.get("DATA_SHEET_NAME", "NF_Import"))
processed_tab = st.sidebar.text_input("Aba para arquivos processados", value=st.secrets.get("PROCESSED_FILES_SHEET_NAME", "Processed_Files"))

# Nova opção para reprocessar todos os arquivos
reprocess_all = st.sidebar.checkbox("Reprocessar todos os arquivos?", value=False)

button_label = "Processar Todos os Arquivos" if reprocess_all else "Verificar nova(s) NF(s)"
st.sidebar.markdown("---")
st.sidebar.markdown("Opções de processamento:")

# Botão para limpar a planilha
if 'confirm_clear_clicked' not in st.session_state:
    st.session_state.confirm_clear_clicked = False

if st.sidebar.button("🚨 Limpar Dados da Planilha"):
    st.session_state.confirm_clear_clicked = True

if st.session_state.confirm_clear_clicked:
    st.sidebar.warning("ATENÇÃO: Esta ação apagará TODOS os dados das abas de 'Dados' e 'Processados'!")
    confirm_clear_final = st.sidebar.checkbox("Confirmar limpeza agora? (Irreversível!)")
    if confirm_clear_final:
        if not spreadsheet_id:
            st.error("Forneça o spreadsheetId do Sheets antes de tentar limpar.")
        else:
            try:
                gs_client = build_sheets_client(creds_check)
                sh = gs_client.open_by_key(spreadsheet_id)
                
                # Colunas esperadas para cada aba
                initial_columns_data = ["timestamp_import", "drive_file_id", "file_name", "drive_mime", "empresa", "cnpj", "descricao_itens", "data_compra", "valor_total", "numero_nota", "cpf", "endereco"]
                initial_columns_proc = ["fileId","name","mimeType","processed_at","modifiedTime", "note"]

                # Limpa e recria cabeçalhos na aba de dados
                ws_data = sh.worksheet(sheet_tab)
                ws_data.clear()
                ws_data.append_row(initial_columns_data)
                st.success(f"Aba '{sheet_tab}' limpa e cabeçalho restaurado.")
                
                # Limpa e recria cabeçalhos na aba de arquivos processados
                ws_proc = sh.worksheet(processed_tab)
                ws_proc.clear()
                ws_proc.append_row(initial_columns_proc)
                st.success(f"Aba '{processed_tab}' limpa e cabeçalho restaurado.")
                
                st.success("Planilhas limpas com sucesso! Por favor, prossiga com o reprocessamento.")
                st.session_state.confirm_clear_clicked = False # Reseta o estado de confirmação
                st.experimental_rerun() # Força uma nova execução para atualizar o UI
            except Exception as e:
                st.error(f"Erro ao limpar planilha: {e}")
                st.session_state.confirm_clear_clicked = False # Reseta o estado em caso de erro
    else:
        if st.sidebar.button("Cancelar Limpeza"):
            st.session_state.confirm_clear_clicked = False
            st.experimental_rerun()


st.sidebar.markdown("---")
st.sidebar.markdown("Clique no botão abaixo para iniciar o processamento:")

# ---------- Main processing ----------
if st.sidebar.button(button_label, key="main_process_button"):
    if not drive_folder_id or not spreadsheet_id:
        st.error("Forneça folderId do Drive e spreadsheetId do Sheets nas configurações de Secrets ou nos campos acima.")
        st.stop()

    creds = get_google_creds()
    drive = build_drive_service(creds)
    gs_client = build_sheets_client(creds)

    with st.spinner("Listando arquivos na pasta..."):
        files = list_files_in_folder(drive, drive_folder_id)
    st.success(f"{len(files)} arquivo(s) encontrados na pasta.")

    # Colunas esperadas para cada aba
    initial_columns_data = ["timestamp_import", "drive_file_id", "file_name", "drive_mime", "empresa", "cnpj", "descricao_itens", "data_compra", "valor_total", "numero_nota", "cpf", "endereco"]
    initial_columns_proc = ["fileId","name","mimeType","processed_at","modifiedTime", "note"]

    # garantir a aba NF_Import
    sh = gs_client.open_by_key(spreadsheet_id)
    try:
        ws_data = sh.worksheet(sheet_tab)
        # Verifica se a primeira linha é o cabeçalho. Se não, limpa e recria.
        if ws_data.row_values(1) != initial_columns_data:
            st.warning(f"Cabeçalhos da aba '{sheet_tab}' inconsistentes. Limpando e recriando.")
            ws_data.clear()
            ws_data.append_row(initial_columns_data)
    except Exception: # Se a aba não existe, cria e adiciona cabeçalhos
        ws_data = sh.add_worksheet(title=sheet_tab, rows="1000", cols="30")
        ws_data.append_row(initial_columns_data)

    # garantir a aba Processed_Files
    try:
        ws_proc = sh.worksheet(processed_tab)
        # Verifica se a primeira linha é o cabeçalho. Se não, limpa e recria.
        if ws_proc.row_values(1) != initial_columns_proc:
            st.warning(f"Cabeçalhos da aba '{processed_tab}' inconsistentes. Limpando e recriando.")
            ws_proc.clear()
            ws_proc.append_row(initial_columns_proc)
    except Exception: # Se a aba não existe, cria e adiciona cabeçalhos
        ws_proc = sh.add_worksheet(title=processed_tab, rows="1000", cols="10")
        ws_proc.append_row(initial_columns_proc)


    # ler processados (se não estiver em modo de reprocessamento total)
    proc_records = ws_proc.get_all_records()
    proc_df = pd.DataFrame(proc_records) if proc_records else pd.DataFrame(columns=initial_columns_proc)
    processed_ids = set(proc_df["fileId"].astype(str).tolist()) if not proc_df.empty and "fileId" in proc_df.columns else set()


    # identificar arquivos para processar
    if reprocess_all:
        files_to_process = files
        st.write(f"Reprocessando TODOS os arquivos: {len(files_to_process)}")
    else:
        files_to_process = [f for f in files if str(f.get("id")) not in processed_ids]
        st.write(f"Novos arquivos a processar: {len(files_to_process)}")

    results_rows = []
    processed_rows = []
    
    # Carregar dados existentes para atualização (se houver)
    # Sempre leia do sheets para ter os dados mais atualizados
    data_records_from_sheet = ws_data.get_all_records()
    existing_data_df = pd.DataFrame(data_records_from_sheet) if data_records_from_sheet else pd.DataFrame(columns=initial_columns_data)


    for f in files_to_process:
        fid = f.get("id")
        name = f.get("name")
        mime = f.get("mimeType")
        modified = f.get("modifiedTime", "")
        st.write(f"Processando: {name} ({fid}) — {mime}")
        
        tmpf = tempfile.NamedTemporaryFile(delete=False, suffix=".bin")
        try:
            path, actual_mime = download_drive_file(drive, fid, tmpf.name)
            tmpf.close()
            
            # Usar `magic.from_file` para uma detecção mais robusta do tipo de arquivo
            # Em alguns ambientes, `file` (usado por `python-magic`) pode não estar disponível ou configurado corretamente.
            # Se `magic` falhar, podemos tentar inferir pela extensão ou pelo MIME do Drive.
            try:
                kind = magic.from_file(path, mime=True)
            except Exception as e:
                st.warning(f"Falha na detecção de MIME local com `python-magic`: {e}. Usando MIME do Drive: {actual_mime}")
                kind = actual_mime
            
            st.write(f"Download OK. MIME detectado (ou inferido): {kind}")
            
            extracted = ""
            if actual_mime == "text/plain" or "text" in kind: # Tratamento para texto puro
                 with open(path, "r", encoding="utf-8", errors="ignore") as fh:
                    extracted = fh.read()
            elif "pdf" in kind or path.lower().endswith(".pdf"):
                extracted = extract_text_from_pdf(path)
                if not extracted or len(extracted.strip()) < 30: # Se pouco texto ou falha, tenta OCR
                    st.info("PDF com pouco texto direto ou erro — tentando OCR.")
                    extracted = extract_text_via_ocr(path)
            elif "image" in kind or path.lower().endswith((".png", ".jpg", ".jpeg", ".tiff", ".bmp")):
                st.info("Arquivo de imagem detectado — aplicando OCR.")
                extracted = extract_text_via_ocr(path)
            else:
                st.warning(f"Tipo de arquivo '{kind}' não suportado para extração direta. Tentando OCR como fallback.")
                extracted = extract_text_via_ocr(path)

            if not extracted or len(extracted.strip())==0:
                st.warning(f"Nenhum texto extraído do arquivo {name}. Pulei.")
                processed_rows.append({
                    "fileId": fid, "name": name, "mimeType": mime, "processed_at": datetime.now().isoformat(), "modifiedTime": modified, "note": "no_text_extracted"
                })
                continue

            fields = extract_fields_from_text(extracted)
            row = {
                "timestamp_import": datetime.now().isoformat(),
                "drive_file_id": fid,
                "file_name": name,
                "drive_mime": mime,
                "empresa": fields.get("empresa"),
                "cnpj": fields.get("cnpj"),
                "descricao_itens": fields.get("itens_descricoes"),
                "data_compra": fields.get("data_compra"),
                "valor_total": fields.get("valor_total"),
                "numero_nota": fields.get("numero_nota"),
                "cpf": fields.get("cpf"),
                "endereco": fields.get("endereco")
            }
            results_rows.append(row)
            processed_rows.append({
                "fileId": fid, "name": name, "mimeType": mime, "processed_at": datetime.now().isoformat(), "modifiedTime": modified, "note": "processed_ok"
            })
            st.success(f"Processado: {name}")

        except Exception as e:
            st.error(f"Erro ao processar {name}: {e}")
            processed_rows.append({
                "fileId": fid, "name": name, "mimeType": mime, "processed_at": datetime.now().isoformat(), "modifiedTime": modified, "note": f"error: {e}"
            })
        finally:
            try:
                os.unlink(tmpf.name)
            except:
                pass

    # gravar resultados na aba NF_Import (com lógica de upsert)
    if results_rows:
        try:
            new_data_df = pd.DataFrame(results_rows)
            
            # Assegurar que 'drive_file_id' é string para comparações
            existing_data_df["drive_file_id"] = existing_data_df["drive_file_id"].astype(str)
            new_data_df["drive_file_id"] = new_data_df["drive_file_id"].astype(str)

            # Combinar dados existentes com novos, priorizando os novos em caso de conflito no 'drive_file_id'
            # Remove as linhas existentes que estão sendo atualizadas pelos novos dados
            combined_df = pd.concat([existing_data_df[~existing_data_df['drive_file_id'].isin(new_data_df['drive_file_id'])], new_data_df], ignore_index=True)
            
            combined_df = combined_df[initial_columns_data] # Garante a ordem das colunas
            # Limpa a aba e escreve tudo de novo, garantindo o cabeçalho
            ws_data.clear()
            set_with_dataframe(ws_data, combined_df, include_index=False, include_column_header=True) 
            st.success(f"{len(results_rows)} linha(s) gravadas/atualizadas em '{sheet_tab}'.")
        except Exception as e:
            st.error(f"Erro ao gravar dados: {e}")

    # gravar Processed_Files (com lógica de upsert)
    if processed_rows:
        try:
            # Ler a aba de processados mais uma vez para garantir que está atualizada
            proc_records_latest = ws_proc.get_all_records()
            existing_proc_df = pd.DataFrame(proc_records_latest) if proc_records_latest else pd.DataFrame(columns=initial_columns_proc)
            
            new_proc_df = pd.DataFrame(processed_rows)
            
            # Assegurar que 'fileId' é string para comparações
            existing_proc_df["fileId"] = existing_proc_df["fileId"].astype(str)
            new_proc_df["fileId"] = new_proc_df["fileId"].astype(str)

            # Combina os dataframes, removendo duplicatas de fileId e mantendo o registro mais recente
            combined_proc = pd.concat([existing_proc_df, new_proc_df], ignore_index=True)
            combined_proc = combined_proc.sort_values("processed_at").drop_duplicates(subset=["fileId"], keep="last")
            
            combined_proc = combined_proc[initial_columns_proc] # Garante a ordem das colunas
            # Limpa a aba e escreve tudo de novo, garantindo o cabeçalho
            ws_proc.clear()
            set_with_dataframe(ws_proc, combined_proc, include_index=False, include_column_header=True) 
            st.success(f"{len(processed_rows)} arquivo(s) marcados como processados.")
        except Exception as e:
            st.error(f"Erro ao gravar Processed_Files: {e}")

    # mostrar resumo
    st.subheader("Resumo de execução")
    st.write(f"Arquivos encontrados na pasta: {len(files)}")
    st.write(f"Arquivos processados (novos ou reprocessados): {len(results_rows)} (gravados/atualizados em '{sheet_tab}')")
    st.write(f"Arquivos marcados como processados: {len(processed_rows)} (gravados em '{processed_tab}')")

    # exibir primeiras linhas gravadas
    if not combined_df.empty: # Use o dataframe combinado final
        st.subheader("Amostra dos dados gravados/atualizados")
        st.dataframe(combined_df.head(20))

    if not combined_proc.empty: # Use o dataframe combinado final
        st.subheader("Amostra dos arquivos marcados como processados")
        st.dataframe(combined_proc.head(20))
        
st.markdown("---")
st.markdown("Notas / recomendações")
st.markdown(
"""
- A Service Account precisa ser Viewer na pasta (ou nos arquivos) e Editor na planilha.
- O app registra os fileId processados na aba Processed_Files para não reprocessar.
- Para arquivos escaneados, OCR depende do binário Tesseract disponível no ambiente.
- Podemos adaptar para:
    * Processar somente arquivos com extensão específica (.pdf, .docx, .xml)
    * Rodar em lote (bulk) e enviar relatório por e-mail
    * Usar Google Vision API se precisar de OCR de alta qualidade
"""
)

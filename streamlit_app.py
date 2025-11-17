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
import numpy as np # Importado no topo

# Para o pré-processamento de imagens com OpenCV
try:
    import cv2
except ImportError:
    st.warning("OpenCV não encontrado. O pré-processamento de imagens para OCR será limitado.")
    cv2 = None

from gspread import authorize
from gspread_dataframe import set_with_dataframe
from google.auth.exceptions import RefreshError # Importar exceção para tratamento

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
    try:
        creds = Credentials.from_service_account_info(sa_info, scopes=SCOPES)
        return creds
    except RefreshError as e:
        st.error(f"Erro de autenticação Google. Verifique o JSON da Service Account: {e}")
        st.stop()
    except Exception as e:
        st.error(f"Erro inesperado na autenticação Google: {e}")
        st.stop()

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

def download_drive_file(service, file_id, dest_path): # Removido 'mime' pois não era usado e 'actual_mime' é retornado
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
def extract_text_from_pdf(path, status_container=st):
    text = ""
    try:
        with pdfplumber.open(path) as pdf:
            for i, p in enumerate(pdf.pages):
                # Usar st.status.update ou similar para feedback
                status_container.info(f"Extraindo texto da página {i+1} do PDF...")
                pt = p.extract_text()
                if pt:
                    text += pt + "\n"
    except Exception as e:
        status_container.warning(f"pdfplumber falhou ao extrair texto direto: {e}")
    return text

def preprocess_image_for_ocr(image):
    """
    Aplica pré-processamento básico à imagem para melhorar o OCR.
    Requer OpenCV.
    """
    if cv2 is None:
        return image # Retorna a imagem original se OpenCV não estiver disponível

    img_np = np.array(image) # Já é um array NumPy
    
    # Converter para tons de cinza
    gray = cv2.cvtColor(img_np, cv2.COLOR_RGB2GRAY) # PIL Image.open() lê como RGB
    
    # Binarização (thresholding adaptativo para diferentes iluminações)
    thresh = cv2.adaptiveThreshold(gray, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY, 11, 2)
    
    return Image.fromarray(thresh) # Retorna como objeto PIL Image

def extract_text_via_ocr(path, status_container=st):
    text = ""
    try:
        if path.lower().endswith(".pdf"):
            with pdfplumber.open(path) as pdf:
                for i, p in enumerate(pdf.pages):
                    status_container.info(f"Aplicando OCR na página {i+1} do PDF...")
                    im = p.to_image(resolution=300).original # Aumentar resolução para melhor OCR
                    
                    if cv2:
                        processed_im = preprocess_image_for_ocr(im)
                    else:
                        processed_im = im

                    txt = pytesseract.image_to_string(processed_im, lang='por+eng', config='--psm 3')
                    
                    if not txt.strip() or len(txt.strip()) < 50:
                        status_container.info("Texto curto com PSM 3, tentando PSM 6 (single block)...")
                        txt = pytesseract.image_to_string(processed_im, lang='por+eng', config='--psm 6')

                    text += txt + "\n"
        else: # Para imagens diretas (JPG, PNG, etc.)
            status_container.info("Aplicando OCR em arquivo de imagem...")
            im = Image.open(path)
            if cv2:
                processed_im = preprocess_image_for_ocr(im)
            else:
                processed_im = im
            
            txt = pytesseract.image_to_string(processed_im, lang='por+eng', config='--psm 3')
            if not txt.strip() or len(txt.strip()) < 50:
                status_container.info("Texto curto com PSM 3, tentando PSM 6 (single block)...")
                txt = pytesseract.image_to_string(processed_im, lang='por+eng', config='--psm 6')
            text += txt
    except Exception as e:
        status_container.warning(f"OCR falhou: {e}")
    return text

# ---------- Parsers ----------
CNPJ_RE = re.compile(r"(?:CNPJ[:\s]|C.?NPJ[:\s]|CNPJ\s*)?([0-9]{2}[./-]?[0-9]{3}[./-]?[0-9]{3}[/-]?[0-9]{4}[-]?[0-9]{2})")
CPF_RE = re.compile(r"(?:CPF[:\s]|CPF\s)?([0-9]{3}[./-]?[0-9]{3}[./-]?[0-9]{3}[-]?[0-9]{2})")
VAL_RE = re.compile(r"R\$?\s*(\d{1,3}(?:\.\d{3})*(?:,\d{2})|\d+,\d{2})")
DATE_RE = re.compile(r"(?:DATA(?:\s*DE\s*EMISSÃO)?[:\s]*|EMISSÃO[:\s]*|DATA/HORA[:\s]*|DATA[:\s]*|DATE[:\s]*)?(\d{1,2}[/-]\d{1,2}[/-]\d{2,4}(?:\s+\d{1,2}:\d{1,2}(?::\d{1,2})?)?|\d{4}-\d{1,2}-\d{1,2})")
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

def extract_fields_from_text(text, status_container=st):
    out = {}
    
    # st.write("--- Texto Bruto do OCR (primeiros 1000 caracteres) ---")
    # st.text(text[:1000])
    # st.write("-----------------------------------------------------")

    lines = [l.strip() for l in text.splitlines() if l.strip()]

    # 1. Extrair CNPJ (prioritário para identificar a empresa)
    m = CNPJ_RE.search(text)
    out["cnpj"] = m.group(1) if m else None
    if out["cnpj"]: status_container.caption(f"CNPJ encontrado: {out['cnpj']}")

    # 2. Extrair Empresa (Razão Social) - Lógica aprimorada
    company = None
    if out["cnpj"]:
        cnpj_pos = text.find(out["cnpj"])
        if cnpj_pos != -1:
            snippet_before_cnpj = text[:cnpj_pos]
            lines_before_cnpj = [l.strip() for l in snippet_before_cnpj.splitlines() if l.strip()]
            
            if lines_before_cnpj:
                for i in range(len(lines_before_cnpj) - 1, -1, -1):
                    line_candidate = lines_before_cnpj[i]
                    if (len(line_candidate) > 5 and
                        (line_candidate.isupper() or line_candidate.istitle() or 
                         re.search(r'\b(LTDA|SA|S\.A\.|ME|EPP|EIRELI|COMERCIO|SERVICOS|PREFEITURA)\b', line_candidate.upper())) and
                        not re.search(r'\d{1,2}/\d{1,2}/\d{2,4}|\d{3}\.\d{3}\.\d{3}-\d{2}|RUA|AVENIDA|BAIRRO|CEP|CPF', line_candidate.upper())):
                        company = line_candidate
                        break
    
    if not company and lines:
        search_area = "\n".join(lines[:10])
        company_keywords_strong = ["LTDA", "MEI", "EIRELI", "S.A.", "SA", "COMERCIO", "SERVICOS", "MATERIAIS", "INDUSTRIA", "PREFEITURA", "MUNICÍPIO"]
        for kw in company_keywords_strong:
            for l in search_area.splitlines():
                if kw in l.upper() and len(l) > 10 and \
                   not re.search(r'\d{1,2}/\d{1,2}/\d{2,4}|\d{3}-\d{3}|\d+\,\d+|\d+\.\d+|RUA|AVENIDA|BAIRRO|CEP|CPF|TELEFONE|EMAIL|WWW', l.upper()):
                    company = l.strip()
                    break
            if company:
                break
    
    if not company and lines:
        for l in lines[:3]:
            if len(l) > 10 and (l.isupper() or l.istitle()) and \
               not re.search(r'\d{1,2}/\d{1,2}/\d{2,4}|CNPJ|CPF|RUA|AVENIDA|BAIRRO|CEP', l.upper()):
                company = l.strip()
                break

    out["empresa"] = company
    if out["empresa"]: status_container.caption(f"Empresa encontrada: {out['empresa']}")
    
    # 3. Extrair CPF
    m = CPF_RE.search(text)
    out["cpf"] = m.group(1) if m else None
    if out["cpf"]: status_container.caption(f"CPF encontrado: {out['cpf']}")

    # 4. Extrair Data de Compra - Priorizando por palavras-chave
    out["data_compra"] = None
    date_keywords = ["DATA DE EMISSÃO", "EMISSÃO", "DATA/HORA", "DATA", "DATE"]
    for kw in date_keywords:
        idx = text.upper().find(kw)
        if idx != -1:
            snippet = text[idx:idx + 60]
            m = DATE_RE.search(snippet)
            if m:
                out["data_compra"] = m.group(1)
                break
    if not out["data_compra"]:
        m = DATE_RE.search(text)
        out["data_compra"] = m.group(1) if m else None

    if out["data_compra"]: status_container.caption(f"Data de Compra encontrada: {out['data_compra']}")

    # 5. Extrair Número da Nota - Priorizando por palavras-chave
    out["numero_nota"] = None
    nf_keywords = ["NÚMERO:", "NÚMERO DA NOTA", "NOTA FISCAL Nº", "SAT NO.", "Nr Documento", "NF-E", "NFC-E", "DOC:", "NFE:"]
    for kw in nf_keywords:
        idx = text.upper().find(kw)
        if idx != -1:
            snippet = text[idx:idx + 60]
            m = NF_RE.search(snippet)
            if m:
                out["numero_nota"] = m.group(1)
                break
    if not out["numero_nota"]:
        m = NF_RE.search(text)
        out["numero_nota"] = m.group(1) if m else None
    if out["numero_nota"]: status_container.caption(f"Número da Nota encontrado: {out['numero_nota']}")

    # 6. Extrair Valor Total
    total = None
    for keyword in ["VALOR TOTAL", "TOTAL DA NOTA", "TOTAL GERAL", "TOTAL A PAGAR", "TOTAL A RECEBER", "SUBTOTAL"]:
        idx = text.upper().find(keyword)
        if idx != -1:
            snippet = text[idx:idx + 150]
            mval = VAL_RE.search(snippet)
            if mval:
                total = normalize_money(mval.group(1))
                if total is not None:
                    break
    
    if total is None:
        all_vals = VAL_RE.findall(text)
        if all_vals:
            nums_parsed = [normalize_money(v) for v in all_vals if normalize_money(v) is not None]
            if nums_parsed:
                total = max(nums_parsed)

    out["valor_total"] = total
    if out["valor_total"]: status_container.caption(f"Valor Total encontrado: {out['valor_total']}")

    # 7. Extrair Endereço - Lógica aprimorada para blocos de endereço
    address = None
    address_keywords = ["ENDEREÇO", "ENDEREÇO:", "RUA ", "R. ", "AV ", "AVENIDA", "LOGRADOURO", "BAIRRO", "CIDADE", "CEP", "Nº", "NUMERO"]
    
    for keyword in address_keywords:
        idx = text.upper().find(keyword)
        if idx != -1:
            snippet_raw = text[idx: idx + 300]
            snippet_lines = [l.strip() for l in snippet_raw.splitlines() if l.strip()]

            potential_address_parts = []
            for al in snippet_lines[:8]:
                # CORREÇÃO: Removido o parêntese extra que causava o SyntaxError
                if (any(sub_keyword in al.upper() for sub_keyword in ["RUA", "AV", "BAIRRO", "CEP", "CIDADE", "ESTADO", "Nº", "NUMERO", ",", "EDF", "APTO"]) or 
                    re.search(r'\d{5}-\d{3}|\d+\,\d+', al)) and \
                   len(al) > 5 and \
                   not re.search(r'CNPJ|CPF|INSCRIÇÃO|IE|TELEFONE|CELULAR|E-MAIL|HTTP|VALOR|TOTAL|IMPOSTO|IEST|IMPOSTO', al.upper()):
                    
                    potential_address_parts.append(al)
                elif len(potential_address_parts) > 0 and \
                     len(al) > 5 and \
                     not re.search(r'CNPJ|CPF|INSCRIÇÃO|IE|TELEFONE|CELULAR|E-MAIL|HTTP|VALOR|TOTAL|IMPOSTO|IEST|IMPOSTO', al.upper()):
                    potential_address_parts.append(al)
                else:
                    if len(potential_address_parts) > 0:
                        break

            if potential_address_parts:
                address = " ".join(potential_address_parts)
                address = re.sub(r'CEP\s*[:-]?\s*', '', address, flags=re.IGNORECASE).strip()
                break
    out["endereco"] = address
    if out["endereco"]: status_container.caption(f"Endereço encontrado: {out['endereco']}")

    # 8. Extrair Descrição de Itens - Lógica aprimorada
    items=[]
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

        if not in_items_section:
            if any(kw in line_upper for kw in start_keywords) and len(line_clean) < 100:
                in_items_section = True
                continue
        
        if in_items_section:
            if any(kw in line_upper for kw in end_keywords):
                break 

            is_potential_item = (
                (re.search(r"\d+,\d{2}|\d+\.\d{2}", line_clean) and re.search(r"[a-zA-Z]", line_clean) and len(line_clean) > 5) or
                (re.search(r"^\d+(\s*[X\*x]\s*)?\d*\s*(?:UN|QTDE|PÇ|KG|LT|M)\s*[a-zA-Z0-9\s]+?\s+\d+,\d{2}|\d+\.\d{2}", line_clean)) or
                (re.search(r"[a-zA-Z]{5,}", line_clean) and re.search(r"\d+", line_clean) and len(line_clean) > 10)
            )

            if is_potential_item:
                if not any(keyword in line_upper for keyword in ignore_keywords_items):
                    items.append(line_clean)
                    
    out["itens_descricoes"] = "\n".join(items[:40]) if items else None
    if out["itens_descricoes"]: status_container.caption(f"Itens encontrados:\n{out['itens_descricoes']}")

    return out

# ---------- UI inputs ----------
st.sidebar.header("Parâmetros")
creds_check = get_google_creds()

drive_folder_id = st.sidebar.text_input("ID da pasta no Google Drive (folderId)", value=st.secrets.get("DRIVE_FOLDER_ID", ""))
spreadsheet_id = st.sidebar.text_input("ID do Google Sheets (spreadsheetId)", value=st.secrets.get("GOOGLE_SHEET_ID", ""))
sheet_tab = st.sidebar.text_input("Nome da aba para dados", value=st.secrets.get("DATA_SHEET_NAME", "NF_Import"))
processed_tab = st.sidebar.text_input("Aba para arquivos processados", value=st.secrets.get("PROCESSED_FILES_SHEET_NAME", "Processed_Files"))

reprocess_all = st.sidebar.checkbox("Reprocessar todos os arquivos?", value=False)

button_label = "Processar Todos os Arquivos" if reprocess_all else "Verificar nova(s) NF(s)"
st.sidebar.markdown("---")
st.sidebar.markdown("Opções de processamento:")

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
                
                initial_columns_data = ["timestamp_import", "drive_file_id", "file_name", "drive_mime", "empresa", "cnpj", "descricao_itens", "data_compra", "valor_total", "numero_nota", "cpf", "endereco"]
                initial_columns_proc = ["fileId","name","mimeType","processed_at","modifiedTime", "note"]

                ws_data = sh.worksheet(sheet_tab)
                ws_data.clear()
                ws_data.append_row(initial_columns_data)
                st.success(f"Aba '{sheet_tab}' limpa e cabeçalho restaurado.")
                
                ws_proc = sh.worksheet(processed_tab)
                ws_proc.clear()
                ws_proc.append_row(initial_columns_proc)
                st.success(f"Aba '{processed_tab}' limpa e cabeçalho restaurado.")
                
                st.success("Planilhas limpas com sucesso! Por favor, prossiga com o reprocessamento.")
                st.session_state.confirm_clear_clicked = False
                st.rerun() # Usar st.rerun() no lugar de experimental_rerun()
            except Exception as e:
                st.error(f"Erro ao limpar planilha: {e}")
                st.session_state.confirm_clear_clicked = False
    else:
        if st.sidebar.button("Cancelar Limpeza"):
            st.session_state.confirm_clear_clicked = False
            st.rerun() # Usar st.rerun()

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

    with st.status("Listando arquivos na pasta...", expanded=True) as status_list:
        files = list_files_in_folder(drive, drive_folder_id)
        status_list.success(f"{len(files)} arquivo(s) encontrados na pasta.")

    initial_columns_data = ["timestamp_import", "drive_file_id", "file_name", "drive_mime", "empresa", "cnpj", "descricao_itens", "data_compra", "valor_total", "numero_nota", "cpf", "endereco"]
    initial_columns_proc = ["fileId","name","mimeType","processed_at","modifiedTime", "note"]

    sh = gs_client.open_by_key(spreadsheet_id)
    with st.status("Verificando abas do Google Sheets...", expanded=True) as status_sheets:
        try:
            ws_data = sh.worksheet(sheet_tab)
            if ws_data.row_values(1) != initial_columns_data:
                status_sheets.warning(f"Cabeçalhos da aba '{sheet_tab}' inconsistentes. Limpando e recriando.")
                ws_data.clear()
                ws_data.append_row(initial_columns_data)
                status_sheets.success(f"Aba '{sheet_tab}' verificada e cabeçalho restaurado.")
            else:
                status_sheets.info(f"Aba '{sheet_tab}' OK.")
        except Exception:
            ws_data = sh.add_worksheet(title=sheet_tab, rows="1000", cols="30")
            ws_data.append_row(initial_columns_data)
            status_sheets.success(f"Aba '{sheet_tab}' criada e cabeçalho adicionado.")

        try:
            ws_proc = sh.worksheet(processed_tab)
            if ws_proc.row_values(1) != initial_columns_proc:
                status_sheets.warning(f"Cabeçalhos da aba '{processed_tab}' inconsistentes. Limpando e recriando.")
                ws_proc.clear()
                ws_proc.append_row(initial_columns_proc)
                status_sheets.success(f"Aba '{processed_tab}' verificada e cabeçalho restaurado.")
            else:
                status_sheets.info(f"Aba '{processed_tab}' OK.")
        except Exception:
            ws_proc = sh.add_worksheet(title=processed_tab, rows="1000", cols="10")
            ws_proc.append_row(initial_columns_proc)
            status_sheets.success(f"Aba '{processed_tab}' criada e cabeçalho adicionado.")
        status_sheets.update(state="complete")


    # ler processados (se não estiver em modo de reprocessamento total)
    with st.status("Carregando arquivos processados...", expanded=True) as status_proc_load:
        proc_records = ws_proc.get_all_records()
        proc_df = pd.DataFrame(proc_records) if proc_records else pd.DataFrame(columns=initial_columns_proc)
        processed_ids = set(proc_df["fileId"].astype(str).tolist()) if not proc_df.empty and "fileId" in proc_df.columns else set()
        status_proc_load.success(f"{len(processed_ids)} arquivos processados previamente carregados.")


    # identificar arquivos para processar
    if reprocess_all:
        files_to_process = files
        st.info(f"Reprocessando TODOS os arquivos: {len(files_to_process)}")
    else:
        files_to_process = [f for f in files if str(f.get("id")) not in processed_ids]
        st.info(f"Novos arquivos a processar: {len(files_to_process)}")

    results_rows = []
    processed_rows = []
    
    # Carregar dados existentes para atualização
    with st.status("Carregando dados existentes da planilha de destino...", expanded=False) as status_data_load:
        data_records_from_sheet = ws_data.get_all_records()
        existing_data_df = pd.DataFrame(data_records_from_sheet) if data_records_from_sheet else pd.DataFrame(columns=initial_columns_data)
        status_data_load.success(f"{len(existing_data_df)} registros existentes carregados.")


    for i, f in enumerate(files_to_process):
        fid = f.get("id")
        name = f.get("name")
        mime = f.get("mimeType")
        modified = f.get("modifiedTime", "")
        
        with st.status(f"Processando arquivo {i+1}/{len(files_to_process)}: **{name}** ({fid})", expanded=True) as file_status:
            try:
                tmpf = tempfile.NamedTemporaryFile(delete=False, suffix=".bin")
                path, actual_mime = download_drive_file(drive, fid, tmpf.name)
                tmpf.close()
                
                try:
                    kind = magic.from_file(path, mime=True)
                except Exception as e:
                    file_status.warning(f"Falha na detecção de MIME local com `python-magic`: {e}. Usando MIME do Drive: {actual_mime}")
                    kind = actual_mime
                
                file_status.info(f"Download OK. MIME detectado (ou inferido): {kind}")
                
                extracted = ""
                if actual_mime == "text/plain" or "text" in kind:
                    with open(path, "r", encoding="utf-8", errors="ignore") as fh:
                        extracted = fh.read()
                elif "pdf" in kind or path.lower().endswith(".pdf"):
                    extracted = extract_text_from_pdf(path, file_status)
                    if not extracted or len(extracted.strip()) < 30:
                        file_status.info("PDF com pouco texto direto ou erro — tentando OCR.")
                        extracted = extract_text_via_ocr(path, file_status)
                elif "image" in kind or path.lower().endswith((".png", ".jpg", ".jpeg", ".tiff", ".bmp")):
                    file_status.info("Arquivo de imagem detectado — aplicando OCR.")
                    extracted = extract_text_via_ocr(path, file_status)
                else:
                    file_status.warning(f"Tipo de arquivo '{kind}' não suportado para extração direta. Tentando OCR como fallback.")
                    extracted = extract_text_via_ocr(path, file_status)

                if not extracted or len(extracted.strip())==0:
                    file_status.warning(f"Nenhum texto extraído do arquivo {name}. Pulei.")
                    processed_rows.append({
                        "fileId": fid, "name": name, "mimeType": mime, "processed_at": datetime.now().isoformat(), "modifiedTime": modified, "note": "no_text_extracted"
                    })
                    file_status.update(state="error", expanded=False) # Fecha o status em erro
                    continue

                fields = extract_fields_from_text(extracted, file_status)
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
                file_status.success(f"Processado: {name}", expanded=False) # Fecha o status em sucesso

            except Exception as e:
                file_status.error(f"Erro ao processar {name}: {e}", expanded=True) # Mantém aberto em erro para visualização
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
        with st.status(f"Gravando/Atualizando {len(results_rows)} registros na aba '{sheet_tab}'...", expanded=True) as status_save_data:
            try:
                new_data_df = pd.DataFrame(results_rows)
                
                existing_data_df["drive_file_id"] = existing_data_df["drive_file_id"].astype(str)
                new_data_df["drive_file_id"] = new_data_df["drive_file_id"].astype(str)

                combined_df = pd.concat([existing_data_df[~existing_data_df['drive_file_id'].isin(new_data_df['drive_file_id'])], new_data_df], ignore_index=True)
                
                combined_df = combined_df[initial_columns_data]
                ws_data.clear()
                set_with_dataframe(ws_data, combined_df, include_index=False, include_column_header=True) 
                status_save_data.success(f"{len(results_rows)} linha(s) gravadas/atualizadas em '{sheet_tab}'.")
            except Exception as e:
                status_save_data.error(f"Erro ao gravar dados: {e}")
            finally:
                status_save_data.update(state="complete")


    # gravar Processed_Files (com lógica de upsert)
    if processed_rows:
        with st.status(f"Registrando {len(processed_rows)} arquivos como processados na aba '{processed_tab}'...", expanded=True) as status_save_proc:
            try:
                proc_records_latest = ws_proc.get_all_records()
                existing_proc_df = pd.DataFrame(proc_records_latest) if proc_records_latest else pd.DataFrame(columns=initial_columns_proc)
                
                new_proc_df = pd.DataFrame(processed_rows)
                
                existing_proc_df["fileId"] = existing_proc_df["fileId"].astype(str)
                new_proc_df["fileId"] = new_proc_df["fileId"].astype(str)

                combined_proc = pd.concat([existing_proc_df, new_proc_df], ignore_index=True)
                combined_proc = combined_proc.sort_values("processed_at").drop_duplicates(subset=["fileId"], keep="last")
                
                combined_proc = combined_proc[initial_columns_proc]
                ws_proc.clear()
                set_with_dataframe(ws_proc, combined_proc, include_index=False, include_column_header=True) 
                status_save_proc.success(f"{len(processed_rows)} arquivo(s) marcados como processados.")
            except Exception as e:
                status_save_proc.error(f"Erro ao gravar Processed_Files: {e}")
            finally:
                status_save_proc.update(state="complete")

    st.subheader("Resumo de execução")
    st.write(f"Arquivos encontrados na pasta: {len(files)}")
    st.write(f"Arquivos processados (novos ou reprocessados): {len(results_rows)} (gravados/atualizados em '{sheet_tab}')")
    st.write(f"Arquivos marcados como processados: {len(processed_rows)} (gravados em '{processed_tab}')")

    # exibir primeiras linhas gravadas
    if 'combined_df' in locals() and not combined_df.empty:
        st.subheader("Amostra dos dados gravados/atualizados")
        st.dataframe(combined_df.head(20))
    else:
        st.info("Nenhum dado novo foi gravado ou atualizado.")

    if 'combined_proc' in locals() and not combined_proc.empty:
        st.subheader("Amostra dos arquivos marcados como processados")
        st.dataframe(combined_proc.head(20))
    else:
        st.info("Nenhum arquivo foi marcado como processado nesta execução.")
        
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

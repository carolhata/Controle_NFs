
"""
Streamlit app: varre uma pasta do Google Drive, detecta arquivos novos ou reprocessa todos,
extrai campos de notas fiscais e grava no Google Sheets.
Requer: colocar JSON da Service Account em st.secrets["GOOGLE_SERVICE_ACCOUNT"]
Fluxo: listar pasta -> (opcionalmente ignorar Processed_Files sheet) -> processar -> anotar Processed_Files
"""
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
        st.warning(f"pdfplumber falhou: {e}")
    return text

def extract_text_via_ocr(path):
    text = ""
    try:
        if path.lower().endswith(".pdf"):
            with pdfplumber.open(path) as pdf:
                for p in pdf.pages:
                    im = p.to_image(resolution=200).original
                    txt = pytesseract.image_to_string(im, lang='por+eng')
                    text += txt + "\n"
        else:
            im = Image.open(path)
            txt = pytesseract.image_to_string(im, lang='por+eng')
            text += txt
    except Exception as e:
        st.warning(f"OCR falhou: {e}")
    return text

# ---------- Parsers ----------
CNPJ_RE = re.compile(r"(?:CNPJ[:\s]|C.?NPJ[:\s]|CNPJ\s*)?([0-9]{2}[./-]?[0-9]{3}[./-]?[0-9]{3}[/-]?[0-9]{4}[-]?[0-9]{2})")
CPF_RE = re.compile(r"(?:CPF[:\s]|CPF\s)?([0-9]{3}[./-]?[0-9]{3}[./-]?[0-9]{3}[-]?[0-9]{2})")
VAL_RE = re.compile(r"R\$?\s*(\d{1,3}(?:\.\d{3})*(?:,\d{2})|\d+,\d{2})") # CORRIGIDO: Escapado o ponto
# Melhorando DATE_RE para capturar formatos como DD/MM/AAAA, DD/MM/AA, AAAA-MM-DD e DD/MM/YYYY HH:MM
DATE_RE = re.compile(r"(?:DATA(?:\s*DE\s*EMISSÃO)?[:\s]*)?(\d{1,2}[/-]\d{1,2}[/-]\d{2,4}(?:\s+\d{1,2}:\d{1,2}(?::\d{1,2})?)?|\d{4}-\d{1,2}-\d{1,2})")

NF_RE = re.compile(r"(?:N(?:.|º|o)?\sF(?:iscal)?[:\s]|Nota\s+Fiscal[:\s]|N[:º\s]|SAT\sNo.?\s|\sNFC-e\s|\sNF-e\s|Nr\s+Documento[:\s]*)([0-9-/.]+)")


def normalize_money(s):
    if not s:
        return None
    s = s.strip()
    s = re.sub(r"[Rr]$\s*", "", s)
    s = s.replace(".", "")
    s = s.replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None

def extract_fields_from_text(text):
    out = {}
    st.write("--- Texto Bruto do OCR ---")
    st.text(text[:1000]) # Mostra os primeiros 1000 caracteres do texto do OCR para depuração
    st.write("-------------------------")

    # Extrair CNPJ
    m = CNPJ_RE.search(text)
    out["cnpj"] = m.group(1) if m else None
    if out["cnpj"]: st.write(f"CNPJ encontrado: {out['cnpj']}")

    # Extrair CPF
    m = CPF_RE.search(text)
    out["cpf"] = m.group(1) if m else None
    if out["cpf"]: st.write(f"CPF encontrado: {out['cpf']}")

    # Extrair Data de Compra - Aprimorando a busca
    out["data_compra"] = None
    # Priorizar buscas com palavras-chave
    date_keywords = ["DATA DE EMISSÃO", "EMISSÃO", "DATA/HORA", "DATA", "DATE"]
    for kw in date_keywords:
        idx = text.upper().find(kw)
        if idx != -1:
            snippet = text[idx:idx + 50] # Pega um snippet após a palavra-chave
            m = DATE_RE.search(snippet)
            if m:
                out["data_compra"] = m.group(1)
                break
    # Se não encontrou por palavra-chave, tenta no texto todo
    if not out["data_compra"]:
        m = DATE_RE.search(text)
        out["data_compra"] = m.group(1) if m else None

    if out["data_compra"]: st.write(f"Data de Compra encontrada: {out['data_compra']}")

    # Extrair Número da Nota - Aprimorando a busca
    out["numero_nota"] = None
    nf_keywords = ["NÚMERO:", "NÚMERO DA NOTA", "NOTA FISCAL Nº", "SAT NO.", "Nr Documento", "NF-E", "NFC-E"]
    for kw in nf_keywords:
        idx = text.upper().find(kw)
        if idx != -1:
            snippet = text[idx:idx + 50]
            m = NF_RE.search(snippet)
            if m:
                out["numero_nota"] = m.group(1)
                break
    if not out["numero_nota"]:
        m = NF_RE.search(text)
        out["numero_nota"] = m.group(1) if m else None
    if out["numero_nota"]: st.write(f"Número da Nota encontrado: {out['numero_nota']}")

    # Extrair Valor Total - Lógica existing está boa, mas garatindo que 'VAL_RE' está correto
    total = None
    for keyword in ["valor total", "total da nota", "total", "valor da nota", "total geral", "valor a pagar", "valor a receber"]:
        idx = text.lower().find(keyword)
        if idx != -1:
            snippet = text[idx: idx + 150]
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
    if out["valor_total"]: st.write(f"Valor Total encontrado: {out['valor_total']}")

    # Extrair Empresa (Razão Social) - Aprimorando a lógica
    company = None
    lines = [l.strip() for l in text.splitlines() if l.strip()]

    # Tenta encontrar antes do CNPJ ou com palavras-chave mais fortes no início
    if lines:
        m_cnpj = CNPJ_RE.search(text)
        if m_cnpj:
            cnpj_pos = m_cnpj.start()
            text_before_cnpj_raw = text[:cnpj_pos]
            text_before_cnpj_lines = [l.strip() for l in text_before_cnpj_raw.splitlines() if l.strip()]

            if text_before_cnpj_lines:
                # Prioriza as 2 últimas linhas antes do CNPJ
                for i in range(len(text_before_cnpj_lines) -1, max(-1, len(text_before_cnpj_lines) - 3), -1):
                    candidate_company_line = text_before_cnpj_lines[i]
                    # Heurística: linha com mais de 5 caracteres, predominantemente maiúscula, sem muitas datas/valores/endereços
                    if len(candidate_company_line) > 5 and \
                       (candidate_company_line.isupper() or sum(1 for c in candidate_company_line if c.isupper()) / len(candidate_company_line) > 0.7) and \
                       not re.search(r'\d{1,2}/\d{1,2}/\d{2,4}|\d{3}-\d{3}|\d+\,\d+|\d+\.\d+|RUA|AVENIDA|BAIRRO|CEP|CPF', candidate_company_line.upper()):
                        company = candidate_company_line
                        break
                if company:
                    st.write(f"Empresa encontrada (pré-CNPJ): {company}")

        # Se não encontrou antes do CNPJ, procura por palavras-chave no início do documento
        if not company:
            search_area = "\n".join(lines[:10])
            company_keywords = ["LTDA", "MEI", "EIRELI", "S.A.", "SA", "COMERCIO", "SERVICOS", "MATERIAIS", "INDUSTRIA", "PREFEITURA"]
            for keyword in company_keywords:
                for l in search_area.splitlines():
                    if keyword in l.upper() and len(l) > 10 and \
                       not re.search(r'\d{1,2}/\d{1,2}/\d{2,4}|\d{3}-\d{3}|\d+\,\d+|\d+\.\d+|RUA|AVENIDA|BAIRRO|CEP|CPF', l.upper()): # Evitar endereços e CPF
                        company = l.strip()
                        break
                if company:
                    break
        
        # Último recurso: a primeira linha significativa em maiúsculas (no topo)
        if not company and lines:
            for l in lines[:5]:
                if l.isupper() and len(l) > 10 and not re.search(r'\d{1,2}/\d{1,2}/\d{2,4}|\d{3}-\d{3}|\d+\,\d+|\d+\.\d+|RUA|AVENIDA|BAIRRO|CEP|CPF', l.upper()):
                    company = l
                    break

    out["empresa"] = company
    if out["empresa"]: st.write(f"Empresa encontrada: {out['empresa']}")

    # Extrair Endereço - Aprimorando a lógica para capturar múltiplas linhas
    address = None
    address_keywords = ["ENDEREÇO", "ENDEREÇO:", "RUA ", "R. ", "AV ", "AVENIDA", "LOGRADOURO", "BAIRRO", "CIDADE", "CEP"]
    
    # Busca por palavras-chave e tenta capturar blocos de texto
    for keyword in address_keywords:
        idx = text.upper().find(keyword)
        if idx != -1:
            # Pega um snippet maior após a palavra-chave
            snippet_raw = text[idx: idx + 300]
            snippet_lines = [l.strip() for l in snippet_raw.splitlines() if l.strip()]

            potential_address_parts = []
            # Procura por linhas que contenham indicadores de endereço
            for al in snippet_lines[:7]: # Limita a busca às próximas 7 linhas
                if (any(sub_keyword in al.upper() for sub_keyword in ["RUA", "AV", "BAIRRO", "CEP", "CIDADE", "NUMERO", "Nº", ",", "EDF", "APTO"]) and 
                    len(al) > 5 and # Linha minimamente longa
                    not re.search(r'CNPJ|CPF|INSCRIÇÃO|IE|TELEFONE|CELULAR|E-MAIL|HTTP|VALOR|TOTAL|IMPOSTO', al.upper())): # Evita outras informações
                    potential_address_parts.append(al)
                elif len(potential_address_parts) > 0 and len(al) > 5 and not re.search(r'CNPJ|CPF|INSCRIÇÃO|IE|TELEFONE|CELULAR|E-MAIL|HTTP|VALOR|TOTAL|IMPOSTO', al.upper()):
                    # Se já encontrou partes de endereço, inclui linhas subsequentes que pareçam continuar o endereço
                    potential_address_parts.append(al)
                else:
                    # Quebra se a linha não for um endereço e não for uma continuação
                    if len(potential_address_parts) > 0: # Se já pegou algo, pare de buscar
                        break

            if potential_address_parts:
                address = " ".join(potential_address_parts)
                break
    out["endereco"] = address
    if out["endereco"]: st.write(f"Endereço encontrado: {out['endereco']}")

    # Extrair Descrição de Itens - Lógica aprimorada
    items=[]
    # Palavras-chave para ignorar linhas que não são itens de descrição (mais abrangente)
    ignore_keywords_items = ["SUBTOTAL", "TOTAL", "IMPOSTOS", "ICMS", "ISS", "DESCONTO", 
                             "FORMA DE PAGAMENTO", "TROCO", "CRÉDITO", "DÉBITO", "DINHEIRO", 
                             "VALOR", "CPF DO CONSUMIDOR", "CNPJ", "IE", "CUPOM FISCAL", 
                             "SAT NO", "NOTA FISCAL", "LANÇAMENTO", "DATA", "CÓDIGO", 
                             "UNIDADE", "QTD", "PRODUTO", "DESCRIÇÃO", "VALOR UNITÁRIO", 
                             "VALOR TOTAL", "CLIENTE", "CONSUMIDOR", "ENDEREÇO", "BAIRRO", 
                             "CIDADE", "CEP", "CONTATO", "TELEFONE", "CNPJ/CPF", "CPF/CNPJ",
                             "DISCRIMINAÇÃO DOS SERVIÇOS", "TOTAL DA NOTA"]
    
    # Marcadores de início e fim da seção de itens (para melhor delimitação)
    start_keywords = ["ITENS", "DESCRIÇÃO", "PRODUTOS E SERVIÇOS", "DETALHES DA VENDA", "CUPOM FISCAL", "NOTA FISCAL", "DISCRIMINAÇÃO", "VALOR TOTAL DO SERVIÇO", "SERVIÇOS PRESTADOS"]
    end_keywords = ["VALOR TOTAL", "SUBTOTAL", "TOTAL A PAGAR", "FORMAS DE PAGAMENTO", "OBSERVAÇÕES", "INFORMÇÕES ADICIONAIS", "PAGAMENTO", "TOTAL GERAL"]

    in_items_section = False
    
    for line in text.splitlines():
        line_clean = line.strip()
        line_upper = line_clean.upper()

        if not in_items_section:
            if any(kw in line_upper for kw in start_keywords) and len(line_clean) < 80: # Evitar capturar linhas muito longas como "start"
                in_items_section = True
                continue
        
        if in_items_section:
            if any(kw in line_upper for kw in end_keywords):
                break # Sai da seção de itens

            # Verifica se contém padrão de dinheiro/número (ex: "10,00" ou "10.00")
            # e também procura por um número de quantidade no início (ex: "1 X", "2 UN")
            # Ou se parece uma descrição de produto (letras e números)
            if ((re.search(r"\d+,\d{2}|\d+\.\d{2}", line_clean) and re.search(r"[a-zA-Z]", line_clean) and len(line_clean) > 5) or # linha com valor e texto
               (re.search(r"^\d+\s*(?:X|UN|QTDE|PÇ|KG)\s*(?:[a-zA-Z0-9\s]+?)\s+\d+,\d{2}|\d+\.\d{2}", line_clean)) or # linha com qtd e valor
               (re.search(r"[a-zA-Z]{5,}", line_clean) and re.search(r"\d+", line_clean) and len(line_clean) > 10)) : # linha com texto longo e número
                
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
# Usamos st.session_state para gerenciar o estado da confirmação
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
    try:
        sh = gs_client.open_by_key(spreadsheet_id)
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
    # Ignorar a primeira linha que é o cabeçalho
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
    # Ignorar a primeira linha que é o cabeçalho
    data_records = ws_data.get_all_records()
    existing_data_df = pd.DataFrame(data_records) if data_records else pd.DataFrame(columns=initial_columns_data)


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
            
            kind = magic.from_file(path, mime=True)
            st.write(f"Download OK. MIME detectado: {kind}")
            
            extracted = ""
            if path.lower().endswith(".txt") or actual_mime=="text/plain":
                with open(path, "r", encoding="utf-8", errors="ignore") as fh:
                    extracted = fh.read()
            elif path.lower().endswith(".pdf") or "pdf" in kind:
                extracted = extract_text_from_pdf(path)
                if not extracted or len(extracted.strip())<30:
                    st.info("PDF com pouco texto — tentando OCR.")
                    extracted = extract_text_via_ocr(path)
            else:
                st.info("Tipo de arquivo desconhecido para extração direta — tentando OCR.")
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
            
            if existing_data_df.empty:
                # Se a planilha estava vazia antes do processamento, já garantimos o cabeçalho.
                set_with_dataframe(ws_data, new_data_df, include_index=False, include_column_header=False)
            else:
                # Combinar dados existentes com novos, priorizando os novos em caso de conflito no 'drive_file_id'
                existing_data_df["drive_file_id"] = existing_data_df["drive_file_id"].astype(str)
                new_data_df["drive_file_id"] = new_data_df["drive_file_id"].astype(str)

                combined_df = pd.concat([existing_data_df[~existing_data_df['drive_file_id'].isin(new_data_df['drive_file_id'])], new_data_df], ignore_index=True)
                
                combined_df = combined_df[initial_columns_data] 
                set_with_dataframe(ws_data, combined_df, include_index=False, include_column_header=False) 
            st.success(f"{len(results_rows)} linha(s) gravadas/atualizadas em '{sheet_tab}'.")
        except Exception as e:
            st.error(f"Erro ao gravar dados: {e}")

    # gravar Processed_Files
    if processed_rows:
        try:
            proc_records_latest = ws_proc.get_all_records()
            existing_proc_latest = pd.DataFrame(proc_records_latest) if proc_records_latest else pd.DataFrame(columns=initial_columns_proc)
            
            new_proc_df = pd.DataFrame(processed_rows)
            
            if existing_proc_latest.empty:
                set_with_dataframe(ws_proc, new_proc_df, include_index=False, include_column_header=False) 
            else:
                combined_proc = pd.concat([existing_proc_latest, new_proc_df], ignore_index=True)
                combined_proc = combined_proc.sort_values("processed_at").drop_duplicates(subset=["fileId"], keep="last")
                
                combined_proc = combined_proc[initial_columns_proc]
                set_with_dataframe(ws_proc, combined_proc, include_index=False, include_column_header=False) 
            st.success(f"{len(processed_rows)} arquivo(s) marcados como processados.")
        except Exception as e:
            st.error(f"Erro ao gravar Processed_Files: {e}")

    # mostrar resumo
    st.subheader("Resumo de execução")
    st.write(f"Arquivos encontrados na pasta: {len(files)}")
    st.write(f"Arquivos processados (novos ou reprocessados): {len(results_rows)} (gravados/atualizados em '{sheet_tab}')")
    st.write(f"Arquivos marcados como processados: {len(processed_rows)} (gravados em '{processed_tab}')")

    # exibir primeiras linhas gravadas
    if results_rows:
        st.subheader("Amostra dos dados gravados/atualizados")
        st.dataframe(pd.DataFrame(results_rows).head(20))

    if processed_rows:
        st.subheader("Amostra dos arquivos marcados como processados")
        st.dataframe(pd.DataFrame(processed_rows).head(20))
        
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

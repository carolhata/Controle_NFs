

"""
streamlit_app.py
Versão atualizada:
- Cria planilha automaticamente (se spreadsheet_id vazio)
- Cria abas: DATA (dados extraídos) e LOGS (idempotência)
- Idempotência por Drive file_id (evita duplicados)
- XML-first + Google Vision OCR fallback
- Usa st.secrets["gcp_service_account"] (NUNCA coloque JSON no código)

Como usar:
1) Adicione o JSON da service account em Streamlit Secrets com a chave:
2) Compartilhe a pasta do Drive e/ou a planilha com o email da service account.
3) Abra o app, informe Drive Folder ID (ou use default nos Secrets), clique em 'Listar arquivos', selecione e processe.
"""

import streamlit as st
import json
import os
import io
import tempfile
import re
from datetime import datetime
from decimal import Decimal, InvalidOperation
from dateutil import parser as dateparser
import pandas as pd
import fitz  # PyMuPDF
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.cloud import vision_v1
from lxml import etree

# -------------------------
# CONFIG
# -------------------------
SCOPES = [
    "https://www.googleapis.com/auth/drive.readonly",
    "https://www.googleapis.com/auth/spreadsheets"
]
VISION_SCOPE = "https://www.googleapis.com/auth/cloud-platform"

# Regex patterns
CNPJ_REGEX = re.compile(r'(\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2}|\d{14})')
CPF_REGEX = re.compile(r'(\d{3}\.\d{3}\.\d{3}-\d{2}|\d{11})')
VALUE_REGEX = re.compile(r'\d{1,3}(?:[.,]\d{3})*[.,]\d{2}')
NOTE_NUMBER_REGEX = re.compile(r'(?:N(?:º|o)?\.?\s*|Nota\s*Fiscal\s*[:\-]?\s*)(\d{1,12})', re.IGNORECASE)
DATE_REGEX = re.compile(r'(\d{2}/\d{2}/\d{4}|\d{4}-\d{2}-\d{2})')

DATA_SHEET_NAME = "DATA"
LOGS_SHEET_NAME = "LOGS"

SHEET_HEADER = [
    "source_filename",
    "drive_file_id",
    "fornecedor_razao_social",
    "fornecedor_cnpj",
    "nota_numero",
    "nota_data",
    "item_index",
    "item_descricao",
    "item_quantidade",
    "item_valor_unitario",
    "item_valor_total",
    "nota_valor_total",
    "cpf_associado",
    "metodo_extracao",
    "confidence",
    "processed_at",
    "observacoes"
]

LOGS_HEADER = ["drive_file_id", "filename", "processed_at", "status", "rows", "message"]

# -------------------------
# CREDENTIALS / SERVICES
# -------------------------
@st.cache_resource(show_spinner=False)
def load_service_account_info():
    """
    Carrega o JSON da service account a partir de st.secrets.
    - Procura por 'gcp_service_account' (preferido) e por 'GOOGLE_SERVICE_ACCOUNT' (fallback).
    - Converte sequências '\\n' em quebras de linha reais.
    - Remove aspas triplas/extras se alguém colou de forma errada.
    - Retorna dict pronto para service_account.Credentials.from_service_account_info(...)
    """
    # 1) obter raw string do secrets (tolerante a dois nomes)
    raw = None
    if "gcp_service_account" in st.secrets:
        raw = st.secrets["gcp_service_account"]
    elif "GOOGLE_SERVICE_ACCOUNT" in st.secrets:
        raw = st.secrets["GOOGLE_SERVICE_ACCOUNT"]
    else:
        st.error("Secret 'gcp_service_account' não encontrado em Streamlit Secrets. Verifique Settings → Secrets.")
        st.stop()

    # 2) normalizar tipo
    if not isinstance(raw, str):
        st.error("Formato inesperado do secret da service account (esperado string).")
        st.stop()

    s = raw.strip()

    # 3) Converter barras duplas \\n em quebras de linha reais (se existirem)
    #    Isso não altera uma string que já tem quebras de linha reais.
    if "\\n" in s:
        s = s.replace("\\n", "\n")

    # 4) Remover aspas extras no início/fim se houver (algumas pessoas acidentalmente colocam aspas duplicadas)
    if s.startswith('"""') and s.endswith('"""'):
        s = s[3:-3].strip()
    # se foi colocado entre aspas simples ou duplas extras (caso raro)
    if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
        # Apenas remover uma camada externa de aspas se for o caso
        s = s[1:-1]

    # 5) Tentar fazer json.loads com tratamento
    try:
        info = json.loads(s)
        return info
    except Exception as e:
        st.error("Erro ao decodificar JSON da service account: " + str(e))
        st.error("Verifique: nome do secret = 'gcp_service_account' e se a chave 'private_key' contém quebras de linha reais (sem '\\\\n').")
        st.stop()


@st.cache_resource(show_spinner=False)
def build_services():
    info = load_service_account_info()
    creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    drive_service = build("drive", "v3", credentials=creds, cache_discovery=False)
    sheets_service = build("sheets", "v4", credentials=creds, cache_discovery=False)
    vision_creds = service_account.Credentials.from_service_account_info(info, scopes=[VISION_SCOPE])
    vision_client = vision_v1.ImageAnnotatorClient(credentials=vision_creds)
    return drive_service, sheets_service, vision_client

# -------------------------
# DRIVE FUNCTIONS
# -------------------------
def list_files_in_folder(drive_service, folder_id):
    """Lista arquivos relevantes na pasta do Drive."""
    q = f"'{folder_id}' in parents and trashed=false"
    fields = "nextPageToken, files(id, name, mimeType, modifiedTime, size)"
    page_token = None
    results = []
    while True:
        resp = drive_service.files().list(q=q, spaces='drive', fields=fields, pageToken=page_token, pageSize=200).execute()
        files = resp.get('files', [])
        for f in files:
            name = f.get("name", "").lower()
            if any(name.endswith(ext) for ext in (".pdf", ".xml", ".jpg", ".jpeg", ".png")):
                results.append(f)
        page_token = resp.get('nextPageToken', None)
        if not page_token:
            break
    return results

def download_drive_file(drive_service, file_id, dest_path):
    request = drive_service.files().get_media(fileId=file_id)
    fh = io.FileIO(dest_path, 'wb')
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    fh.close()
    return dest_path

# -------------------------
# XML PARSER
# -------------------------
def parse_nfe_xml(xml_path):
    """Parse simples para NF-e (cada det -> item)."""
    tree = etree.parse(xml_path)
    root = tree.getroot()
    ns = root.nsmap
    def find_text(node, path):
        try:
            el = node.find(path, namespaces=ns)
            return el.text.strip() if el is not None and el.text else None
        except Exception:
            return None

    emit = root.find('.//{*}emit')
    ide = root.find('.//{*}ide')
    total = root.find('.//{*}total')
    items = root.findall('.//{*}det')

    fornecedor = find_text(emit, './/{*}xNome') if emit is not None else None
    cnpj = find_text(emit, './/{*}CNPJ') if emit is not None else None
    nota_num = find_text(ide, './/{*}nNF') if ide is not None else None
    nota_data = find_text(ide, './/{*}dEmi') if ide is not None else None
    nota_valor_total = find_text(total, './/{*}vNF') if total is not None else None

    rows = []
    for idx, det in enumerate(items, start=1):
        prod = det.find('.//{*}prod')
        if prod is None:
            continue
        descricao = find_text(prod, './/{*}xProd')
        qCom = find_text(prod, './/{*}qCom')
        vUnCom = find_text(prod, './/{*}vUnCom')
        vProd = find_text(prod, './/{*}vProd')
        rows.append({
            "fornecedor_razao_social": fornecedor,
            "fornecedor_cnpj": cnpj,
            "nota_numero": nota_num,
            "nota_data": nota_data,
            "item_index": idx,
            "item_descricao": descricao,
            "item_quantidade": qCom,
            "item_valor_unitario": vUnCom,
            "item_valor_total": vProd,
            "nota_valor_total": nota_valor_total,
            "cpf_associado": None,
            "metodo_extracao": "xml",
            "confidence": 1.0,
            "observacoes": ""
        })
    return rows

# -------------------------
# PDF -> imagens
# -------------------------
def pdf_to_images(pdf_path, zoom=2):
    images = []
    doc = fitz.open(pdf_path)
    mat = fitz.Matrix(zoom, zoom)
    for page in doc:
        pix = page.get_pixmap(matrix=mat, alpha=False)
        images.append(pix.tobytes(output="png"))
    doc.close()
    return images

# -------------------------
# VISION OCR
# -------------------------
def vision_document_ocr(vision_client, image_bytes):
    image = vision_v1.Image(content=image_bytes)
    response = vision_client.document_text_detection(image=image)
    if response.error.message:
        raise RuntimeError(response.error.message)
    return response.full_text_annotation.text if response.full_text_annotation else ""

# -------------------------
# HEURÍSTICAS DE EXTRAÇÃO
# -------------------------
def extract_basic_fields_from_text(text):
    cnpj = None
    cpf = None
    nota_num = None
    nota_data = None
    probable_totals = []

    cnpj_m = CNPJ_REGEX.search(text)
    if cnpj_m:
        cnpj = re.sub(r'\D', '', cnpj_m.group(0))
    cpf_m = CPF_REGEX.search(text)
    if cpf_m:
        cpf = re.sub(r'\D', '', cpf_m.group(0))
    nn = NOTE_NUMBER_REGEX.search(text)
    if nn:
        nota_num = nn.group(1)
    dm = DATE_REGEX.search(text)
    if dm:
        try:
            nota_data = dateparser.parse(dm.group(1), dayfirst=True).date().isoformat()
        except Exception:
            nota_data = dm.group(1)

    vals = VALUE_REGEX.findall(text)
    cleaned = []
    for v in vals:
        vv = v.replace('.', '').replace(',', '.')
        try:
            cleaned.append(Decimal(vv))
        except Exception:
            continue
    cleaned.sort(reverse=True)
    nota_valor_total = str(cleaned[0]) if cleaned else None

    return {
        "fornecedor_razao_social": None,
        "fornecedor_cnpj": cnpj,
        "nota_numero": nota_num,
        "nota_data": nota_data,
        "nota_valor_total": nota_valor_total,
        "cpf_associado": cpf,
        "observacoes": ""
    }

def extract_items_from_text_lines(text):
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    items = []
    idx = 0
    for line in lines:
        values = VALUE_REGEX.findall(line)
        if not values:
            continue
        first_val_match = VALUE_REGEX.search(line)
        desc = line[:first_val_match.start()].strip() if first_val_match else line
        last_vals = values[-2:] if len(values) >= 2 else values
        unit = None; total = None
        try:
            if len(last_vals) == 2:
                unit = Decimal(last_vals[-2].replace('.', '').replace(',', '.'))
                total = Decimal(last_vals[-1].replace('.', '').replace(',', '.'))
            else:
                total = Decimal(last_vals[-1].replace('.', '').replace(',', '.'))
        except Exception:
            pass
        idx += 1
        items.append({
            "item_index": idx,
            "item_descricao": desc or None,
            "item_quantidade": None,
            "item_valor_unitario": unit,
            "item_valor_total": total
        })
    return items

# -------------------------
# BUILD ROWS
# -------------------------
def build_rows_from_extraction(filename, file_id, xml_rows=None, ocr_text=None, ocr_items=None, metodo="xml"):
    rows = []
    processed_at = datetime.utcnow().isoformat()
    if metodo == "xml" and xml_rows:
        for r in xml_rows:
            rows.append({
                "source_filename": filename,
                "drive_file_id": file_id,
                "fornecedor_razao_social": r.get("fornecedor_razao_social"),
                "fornecedor_cnpj": re.sub(r'\D', '', (r.get("fornecedor_cnpj") or "")) if r.get("fornecedor_cnpj") else None,
                "nota_numero": r.get("nota_numero"),
                "nota_data": r.get("nota_data"),
                "item_index": r.get("item_index"),
                "item_descricao": r.get("item_descricao"),
                "item_quantidade": r.get("item_quantidade"),
                "item_valor_unitario": r.get("item_valor_unitario"),
                "item_valor_total": r.get("item_valor_total"),
                "nota_valor_total": r.get("nota_valor_total"),
                "cpf_associado": r.get("cpf_associado"),
                "metodo_extracao": "xml",
                "confidence": 1.0,
                "processed_at": processed_at,
                "observacoes": r.get("observacoes", "")
            })
    else:
        base = ocr_text or {}
        for item in (ocr_items or []):
            rows.append({
                "source_filename": filename,
                "drive_file_id": file_id,
                "fornecedor_razao_social": base.get("fornecedor_razao_social"),
                "fornecedor_cnpj": base.get("fornecedor_cnpj"),
                "nota_numero": base.get("nota_numero"),
                "nota_data": base.get("nota_data"),
                "item_index": item.get("item_index"),
                "item_descricao": item.get("item_descricao"),
                "item_quantidade": item.get("item_quantidade"),
                "item_valor_unitario": str(item.get("item_valor_unitario")) if item.get("item_valor_unitario") is not None else None,
                "item_valor_total": str(item.get("item_valor_total")) if item.get("item_valor_total") is not None else None,
                "nota_valor_total": base.get("nota_valor_total"),
                "cpf_associado": base.get("cpf_associado"),
                "metodo_extracao": "vision" if metodo=="vision" else metodo,
                "confidence": 0.6,
                "processed_at": processed_at,
                "observacoes": base.get("observacoes", "")
            })
    return rows

# -------------------------
# SHEETS HELPERS
# -------------------------
def create_spreadsheet_if_missing(sheets_service, spreadsheet_id, title="Notas_Extracao"):
    if spreadsheet_id:
        return spreadsheet_id
    body = {
        "properties": {"title": title},
        "sheets": [
            {"properties": {"title": DATA_SHEET_NAME}},
            {"properties": {"title": LOGS_SHEET_NAME}}
        ]
    }
    resp = sheets_service.spreadsheets().create(body=body).execute()
    new_id = resp.get("spreadsheetId")
    # write headers
    header_range = f"{DATA_SHEET_NAME}!A1"
    sheets_service.spreadsheets().values().update(
        spreadsheetId=new_id, range=header_range, valueInputOption="RAW",
        body={"values":[SHEET_HEADER]}
    ).execute()
    logs_range = f"{LOGS_SHEET_NAME}!A1"
    sheets_service.spreadsheets().values().update(
        spreadsheetId=new_id, range=logs_range, valueInputOption="RAW",
        body={"values":[LOGS_HEADER]}
    ).execute()
    return new_id

def ensure_sheets_and_headers(sheets_service, spreadsheet_id):
    # ensure DATA and LOGS exist and have headers
    meta = sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    titles = [s["properties"]["title"] for s in meta.get("sheets", [])]
    requests = []
    if DATA_SHEET_NAME not in titles:
        requests.append({"addSheet": {"properties": {"title": DATA_SHEET_NAME}}})
    if LOGS_SHEET_NAME not in titles:
        requests.append({"addSheet": {"properties": {"title": LOGS_SHEET_NAME}}})
    if requests:
        sheets_service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body={"requests": requests}).execute()
    # ensure headers present
    try:
        df_head = sheets_service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=f"{DATA_SHEET_NAME}!A1:Z1").execute()
        if "values" not in df_head or not df_head["values"]:
            sheets_service.spreadsheets().values().update(spreadsheetId=spreadsheet_id, range=f"{DATA_SHEET_NAME}!A1", valueInputOption="RAW", body={"values":[SHEET_HEADER]}).execute()
    except Exception:
        sheets_service.spreadsheets().values().update(spreadsheetId=spreadsheet_id, range=f"{DATA_SHEET_NAME}!A1", valueInputOption="RAW", body={"values":[SHEET_HEADER]}).execute()
    try:
        logs_head = sheets_service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=f"{LOGS_SHEET_NAME}!A1:Z1").execute()
        if "values" not in logs_head or not logs_head["values"]:
            sheets_service.spreadsheets().values().update(spreadsheetId=spreadsheet_id, range=f"{LOGS_SHEET_NAME}!A1", valueInputOption="RAW", body={"values":[LOGS_HEADER]}).execute()
    except Exception:
        sheets_service.spreadsheets().values().update(spreadsheetId=spreadsheet_id, range=f"{LOGS_SHEET_NAME}!A1", valueInputOption="RAW", body={"values":[LOGS_HEADER]}).execute()

def read_processed_file_ids(sheets_service, spreadsheet_id):
    """Lê o LOGS e retorna set de drive_file_id já processados."""
    try:
        resp = sheets_service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=f"{LOGS_SHEET_NAME}!A2:A10000").execute()
        rows = resp.get("values", [])
        return set(r[0] for r in rows if r)
    except Exception:
        return set()

def append_rows_to_sheet(sheets_service, spreadsheet_id, rows, sheet_name=DATA_SHEET_NAME):
    if not rows:
        return {"updatedRows": 0}
    values = [[r.get(h) for h in SHEET_HEADER] for r in rows]
    body = {"values": values}
    resp = sheets_service.spreadsheets().values().append(spreadsheetId=spreadsheet_id, range=f"{sheet_name}!A1", valueInputOption="USER_ENTERED", insertDataOption="INSERT_ROWS", body=body).execute()
    return resp

def append_log_entry(sheets_service, spreadsheet_id, log_row):
    body = {"values": [log_row]}
    resp = sheets_service.spreadsheets().values().append(spreadsheetId=spreadsheet_id, range=f"{LOGS_SHEET_NAME}!A1", valueInputOption="USER_ENTERED", insertDataOption="INSERT_ROWS", body=body).execute()
    return resp

# -------------------------
# MAIN UI / ORCHESTRATION
# -------------------------
def main():
    st.set_page_config(page_title="Leitura de Notas - Streamlit", layout="wide")
    st.title("Leitura de Notas Fiscais — XML-first + Vision (com LOGS e idempotência)")

    drive_service, sheets_service, vision_client = build_services()

    st.sidebar.header("Configurações")
    folder_id = st.sidebar.text_input("Drive Folder ID", value=st.secrets.get("default_drive_folder_id", ""))
    spreadsheet_id_input = st.sidebar.text_input("Google Sheets ID (deixe vazio para criar automaticamente)", value=st.secrets.get("default_sheet_id", ""))
    sheet_name = DATA_SHEET_NAME
    process_only_new = st.sidebar.checkbox("Processar apenas arquivos não processados (recomendado)", value=True)
    st.sidebar.caption("Compartilhe a pasta/planilha com o email da service account.")

    if not folder_id:
        st.warning("Insira o Drive Folder ID na sidebar para prosseguir.")
        st.stop()

    if st.button("Listar arquivos na pasta"):
        with st.spinner("Listando arquivos..."):
            files = list_files_in_folder(drive_service, folder_id)
            st.session_state["drive_files"] = files
            st.success(f"{len(files)} arquivo(s) encontrados.")
    files = st.session_state.get("drive_files", [])

    st.subheader("Arquivos encontrados")
    if files:
        df_files = pd.DataFrame([{"name": f.get("name"), "id": f.get("id"), "mimeType": f.get("mimeType"), "modifiedTime": f.get("modifiedTime")} for f in files])
        st.dataframe(df_files)
    else:
        st.info("Clique em 'Listar arquivos na pasta' para ver arquivos.")

    st.markdown("---")
    st.subheader("Processamento em lote")
    selected_names = st.multiselect("Selecione arquivos para processar", options=[f.get("name") for f in files], default=[f.get("name") for f in files][:5])
    to_process = [f for f in files if f.get("name") in selected_names]

    if st.button("Processar arquivos selecionados"):
        # create spreadsheet if needed
        spreadsheet_id = create_spreadsheet_if_missing(sheets_service, spreadsheet_id_input, title="Notas_Extracao")
        ensure_sheets_and_headers(sheets_service, spreadsheet_id)
        processed_ids = read_processed_file_ids(sheets_service, spreadsheet_id)
        overall_rows = []
        progress = st.progress(0)
        total = len(to_process)
        for i, f in enumerate(to_process):
            fname = f.get("name")
            fid = f.get("id")
            if process_only_new and fid in processed_ids:
                st.info(f"Pulado (já processado): {fname}")
                progress.progress(int((i+1)/total*100))
                continue
            st.info(f"Processando: {fname}")
            tmp = tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(fname)[1])
            tmp.close()
            try:
                download_drive_file(drive_service, fid, tmp.name)
            except Exception as e:
                st.error(f"Erro ao baixar {fname}: {e}")
                append_log_entry(sheets_service, spreadsheet_id, [fid, fname, datetime.utcnow().isoformat(), "FAILED_DOWNLOAD", 0, str(e)])
                progress.progress(int((i+1)/total*100))
                continue

            extracted_rows = []
            method = None
            message = ""
            try:
                if fname.lower().endswith(".xml"):
                    xml_rows = parse_nfe_xml(tmp.name)
                    extracted_rows = build_rows_from_extraction(fname, fid, xml_rows=xml_rows, metodo="xml")
                    method = "xml"
                elif fname.lower().endswith(".pdf"):
                    images = pdf_to_images(tmp.name, zoom=2)
                    combined_items = []
                    base_info = {"fornecedor_razao_social": None, "fornecedor_cnpj": None, "nota_numero": None, "nota_data": None, "nota_valor_total": None, "cpf_associado": None, "observacoes": ""}
                    for img_b in images:
                        text = vision_document_ocr(vision_client, img_b)
                        info = extract_basic_fields_from_text(text)
                        for k, v in info.items():
                            if base_info.get(k) is None and v:
                                base_info[k] = v
                        items = extract_items_from_text_lines(text)
                        combined_items.extend(items)
                    extracted_rows = build_rows_from_extraction(fname, fid, xml_rows=None, ocr_text=base_info, ocr_items=combined_items, metodo="vision")
                    method = "vision"
                elif any(fname.lower().endswith(ext) for ext in [".jpg", ".jpeg", ".png"]):
                    with open(tmp.name, "rb") as fimg:
                        img_b = fimg.read()
                    text = vision_document_ocr(vision_client, img_b)
                    base_info = extract_basic_fields_from_text(text)
                    items = extract_items_from_text_lines(text)
                    extracted_rows = build_rows_from_extraction(fname, fid, xml_rows=None, ocr_text=base_info, ocr_items=items, metodo="vision")
                    method = "vision"
                else:
                    message = "Formato não suportado"
            except Exception as e:
                st.error(f"Erro ao processar {fname}: {e}")
                message = str(e)

            # append to sheet if we have rows
            if extracted_rows:
                try:
                    append_rows_to_sheet(sheets_service, spreadsheet_id, extracted_rows, sheet_name=DATA_SHEET_NAME)
                    append_log_entry(sheets_service, spreadsheet_id, [fid, fname, datetime.utcnow().isoformat(), "OK", len(extracted_rows), method or message])
                    st.success(f"{len(extracted_rows)} linhas adicionadas (arquivo: {fname}).")
                except Exception as e:
                    st.error(f"Erro ao gravar no Sheets: {e}")
                    append_log_entry(sheets_service, spreadsheet_id, [fid, fname, datetime.utcnow().isoformat(), "FAILED_SHEETS", 0, str(e)])
            else:
                st.warning(f"Nenhuma linha extraída de {fname}.")
                append_log_entry(sheets_service, spreadsheet_id, [fid, fname, datetime.utcnow().isoformat(), "NO_ROWS", 0, message or method])

            # mark as processed for this run
            processed_ids.add(fid)
            progress.progress(int((i+1)/total*100))

        st.success("Processamento finalizado. Verifique a planilha.")
        # show preview of recent logs
        try:
            logs = sheets_service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=f"{LOGS_SHEET_NAME}!A1:F20").execute().get("values", [])
            if logs:
                st.markdown("Últimos registros (LOGS):")
                st.dataframe(pd.DataFrame(logs[1:], columns=logs[0]))
        except Exception:
            pass

if __name__ == "__main__":
    main()

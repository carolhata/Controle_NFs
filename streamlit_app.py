"""
streamlit_app.py
Aplicativo Streamlit: leitura de notas fiscais (XML | PDF/Imagens) numa pasta do Google Drive
e exportação linha-a-linha para Google Sheets (cada item = uma linha).

Credenciais:
- Defina st.secrets["gcp_service_account"] com O JSON da service account (string).
- Opcional: st.secrets["default_drive_folder_id"], st.secrets["default_sheet_id"]

Observações:
- O app NO CAMPO inclui secrets no código. Use Streamlit Cloud Secrets.
- Projetado para XML-first; se não houver XML, usa Google Vision Document OCR.
"""

import streamlit as st
import json
import io
import os
import tempfile
import re
from datetime import datetime
from decimal import Decimal, InvalidOperation
from dateutil import parser as dateparser
import pandas as pd
import fitz  # pymupdf
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.cloud import vision_v1
from lxml import etree
from tqdm import tqdm

# -------------------------
# CONFIG / CONSTANTS
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

# Output sheet header
SHEET_HEADER = [
    "source_filename",
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

# -------------------------
# UTIL / CREDENTIALS
# -------------------------
@st.cache_resource(show_spinner=False)
def load_service_account_info():
    if "gcp_service_account" not in st.secrets:
        st.error("Coloque o JSON da service account em Streamlit Secrets: chave 'gcp_service_account'.")
        st.stop()
    sa_json = st.secrets["gcp_service_account"]
    try:
        info = json.loads(sa_json)
    except Exception as e:
        st.error("Erro ao carregar JSON da service account a partir de st.secrets['gcp_service_account'].")
        st.stop()
    return info

@st.cache_resource(show_spinner=False)
def build_google_services():
    info = load_service_account_info()
    credentials = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    drive_service = build("drive", "v3", credentials=credentials, cache_discovery=False)
    sheets_service = build("sheets", "v4", credentials=credentials, cache_discovery=False)
    # Vision client requires separate credentials object with cloud-platform scope
    vision_credentials = service_account.Credentials.from_service_account_info(info, scopes=[VISION_SCOPE])
    vision_client = vision_v1.ImageAnnotatorClient(credentials=vision_credentials)
    return drive_service, sheets_service, vision_client

# -------------------------
# DRIVE / SHEETS
# -------------------------
def list_files_in_folder(drive_service, folder_id):
    """Lista arquivos (pdf, xml, jpg, png) na pasta do Drive."""
    q = f"'{folder_id}' in parents and trashed=false"
    fields = "nextPageToken, files(id, name, mimeType, modifiedTime, size)"
    page_token = None
    results = []
    while True:
        resp = drive_service.files().list(q=q, spaces='drive', fields=fields, pageToken=page_token, pageSize=200).execute()
        files = resp.get('files', [])
        for f in files:
            # filtra por extensão relevante
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
    """
    Parse basic NF-e XML structure (adequate for many NF-e).
    Returns a list of dict rows (one per det/item).
    """
    tree = etree.parse(xml_path)
    ns = tree.getroot().nsmap
    # some XMLs have default namespace None -> handle fallback
    def find_text(path, node=None):
        try:
            if node is None:
                node = tree
            el = node.find(path, namespaces=ns)
            return el.text.strip() if el is not None and el.text else None
        except Exception:
            return None

    root = tree.getroot()
    # Common paths (may vary by layout); try multiple candidates
    emit = root.find('.//{*}emit')
    ide = root.find('.//{*}ide')
    total = root.find('.//{*}total')
    items = root.findall('.//{*}det')

    fornecedor = None
    cnpj = None
    nota_num = None
    nota_data = None
    nota_valor_total = None

    if emit is not None:
        fornecedor = find_text('.//{*}xNome', node=emit) or find_text('.//{*}xFant', node=emit)
        cnpj = find_text('.//{*}CNPJ', node=emit)
    if ide is not None:
        nota_num = find_text('.//{*}nNF', node=ide)
        nota_data = find_text('.//{*}dEmi', node=ide)
    if total is not None:
        nota_valor_total = find_text('.//{*}vNF', node=total) or find_text('.//{*}vProd', node=total)

    rows = []
    for idx, det in enumerate(items, start=1):
        prod = det.find('.//{*}prod')
        if prod is None:
            continue
        descricao = find_text('.//{*}xProd', node=prod)
        qCom = find_text('.//{*}qCom', node=prod)
        vUnCom = find_text('.//{*}vUnCom', node=prod)
        vProd = find_text('.//{*}vProd', node=prod)
        row = {
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
        }
        rows.append(row)
    return rows

# -------------------------
# PDF -> imagens
# -------------------------
def pdf_to_images(pdf_path):
    """Renderiza cada página do PDF para imagem bytes (PNG). Retorna lista de bytes."""
    images = []
    doc = fitz.open(pdf_path)
    for page in doc:
        mat = fitz.Matrix(2, 2)  # escala para melhor OCR
        pix = page.get_pixmap(matrix=mat, alpha=False)
        img_bytes = pix.tobytes(output="png")
        images.append(img_bytes)
    doc.close()
    return images

# -------------------------
# VISION OCR
# -------------------------
def vision_document_ocr(vision_client, image_bytes):
    """Chama Document OCR (DOCUMENT_TEXT_DETECTION) e retorna full text e blocks."""
    image = vision_v1.Image(content=image_bytes)
    response = vision_client.document_text_detection(image=image)
    if response.error.message:
        raise RuntimeError(response.error.message)
    # Combine full text
    full_text = response.full_text_annotation.text if response.full_text_annotation else ""
    return full_text

# -------------------------
# TEXT EXTRACTION HEURISTICS
# -------------------------
def extract_basic_fields_from_text(text):
    """Extrai CNPJ/CPF/número/data/valores de um bloco de texto (heurístico)."""
    cnpj = None
    cpf = None
    nota_num = None
    nota_data = None
    probable_totals = []

    # CNPJ / CPF
    cnpj_m = CNPJ_REGEX.search(text)
    if cnpj_m:
        cnpj = re.sub(r'\D', '', cnpj_m.group(0))
    cpf_m = CPF_REGEX.search(text)
    if cpf_m:
        cpf = re.sub(r'\D', '', cpf_m.group(0))

    # nota numero
    nn = NOTE_NUMBER_REGEX.search(text)
    if nn:
        nota_num = nn.group(1)

    # data
    dm = DATE_REGEX.search(text)
    if dm:
        try:
            nota_data = dateparser.parse(dm.group(1), dayfirst=True).date().isoformat()
        except Exception:
            nota_data = dm.group(1)

    # valores (pegar top 5 maiores como candidatos a total)
    vals = VALUE_REGEX.findall(text)
    cleaned = []
    for v in vals:
        vv = v.replace('.', '').replace(',', '.')
        try:
            cleaned.append(Decimal(vv))
        except InvalidOperation:
            continue
    cleaned.sort(reverse=True)
    probable_totals = cleaned[:5]
    nota_valor_total = str(probable_totals[0]) if probable_totals else None

    return {
        "fornecedor_razao_social": None,  # hard to detect from plain text reliably
        "fornecedor_cnpj": cnpj,
        "nota_numero": nota_num,
        "nota_data": nota_data,
        "nota_valor_total": nota_valor_total,
        "cpf_associado": cpf,
        "observacoes": ""
    }

def extract_items_from_text_lines(text):
    """
    Heurística bem simples: separa o texto por linhas,
    identifica linhas que contenham valores e quantidades, e agrupa como itens.
    Retorna lista de dicts com descricao, quantidade, unitario, total.
    OBS: abordagem genérica — corrigir no preview.
    """
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    item_rows = []
    idx = 0
    for i, line in enumerate(lines):
        # busca valores na linha
        values = VALUE_REGEX.findall(line)
        if not values:
            continue
        # heurística: se linha tem 2 ou 3 valores -> pode ser: qty unit total  OR unit total
        if len(values) >= 2:
            # descrição = part before first value
            first_val_span = VALUE_REGEX.search(line)
            desc = line[:first_val_span.start()].strip()
            # pegar os últimos dois valores como unit e total (ou somente total)
            last_vals = values[-2:] if len(values) >= 2 else [values[-1]]
            qty = None
            unit = None
            total = None
            if len(last_vals) == 2:
                try:
                    unit = Decimal(last_vals[-2].replace('.', '').replace(',', '.'))
                except Exception:
                    unit = None
                try:
                    total = Decimal(last_vals[-1].replace('.', '').replace(',', '.'))
                except Exception:
                    total = None
            elif len(last_vals) == 1:
                try:
                    total = Decimal(last_vals[-1].replace('.', '').replace(',', '.'))
                except Exception:
                    total = None
            idx += 1
            item_rows.append({
                "item_index": idx,
                "item_descricao": desc or None,
                "item_quantidade": qty,
                "item_valor_unitario": unit,
                "item_valor_total": total
            })
        else:
            continue
    return item_rows

# -------------------------
# NORMALIZAÇÃO / CONSTRUÇÃO DE DF
# -------------------------
def build_rows_from_file(filename, xml_rows=None, ocr_text=None, ocr_items=None, metodo="xml"):
    """
    Monta lista de output rows respeitando SHEET_HEADER.
    xml_rows: output do parse_nfe_xml
    ocr_text: dict com campos básicos
    ocr_items: lista de itens heurísticos
    """
    rows = []
    processed_at = datetime.utcnow().isoformat()
    if metodo == "xml" and xml_rows:
        for r in xml_rows:
            out = {
                "source_filename": filename,
                "fornecedor_razao_social": r.get("fornecedor_razao_social"),
                "fornecedor_cnpj": re.sub(r'\D', '', r.get("fornecedor_cnpj") or "") if r.get("fornecedor_cnpj") else None,
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
            }
            rows.append(out)
    else:
        # usar ocr_text + ocr_items
        base = ocr_text or {}
        for item in (ocr_items or []):
            out = {
                "source_filename": filename,
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
            }
            rows.append(out)
    return rows

# -------------------------
# SHEETS WRITE
# -------------------------
def append_rows_to_sheet(sheets_service, spreadsheet_id, rows, sheet_name="Sheet1"):
    """
    Aplica append de linhas (rows: list of dicts) para a planilha.
    Se planilha vazia, escreve header antes.
    """
    if not rows:
        return {"updatedRows": 0}
    # Build values
    values = []
    for r in rows:
        rowvals = [r.get(h) for h in SHEET_HEADER]
        values.append(rowvals)
    body = {"values": values}
    range_name = f"{sheet_name}!A1"
    # Check if sheet has header: read A1
    try:
        sheet = sheets_service.spreadsheets()
        # Append
        resp = sheet.values().append(spreadsheetId=spreadsheet_id, range=range_name,
                                     valueInputOption="USER_ENTERED", body=body).execute()
        return resp
    except Exception as e:
        st.error(f"Erro ao escrever no Google Sheets: {e}")
        return None

# -------------------------
# MAIN APP
# -------------------------
def main():
    st.set_page_config(page_title="Leitura de Notas - Streamlit", layout="wide")
    st.title("Leitura de Notas Fiscais (XML-first + Google Vision)")

    # Build services
    drive_service, sheets_service, vision_client = build_google_services()

    # Sidebar controls
    st.sidebar.header("Configurações")
    folder_id = st.sidebar.text_input("Drive Folder ID", value=st.secrets.get("default_drive_folder_id", ""))
    spreadsheet_id = st.sidebar.text_input("Google Sheets ID (onde salvar)", value=st.secrets.get("default_sheet_id", ""))
    sheet_name = st.sidebar.text_input("Nome da aba (Sheet)", value="Sheet1")
    process_new_only = st.sidebar.checkbox("Processar apenas arquivos não processados (recomendado)", value=True)
    st.sidebar.caption("Importante: compartilhe a pasta do Drive com a service account utilizada.")

    if not folder_id:
        st.warning("Insira o Drive Folder ID (na sidebar) para prosseguir.")
        st.stop()

    cols = st.columns([2,1,1])
    with cols[0]:
        if st.button("Listar arquivos na pasta"):
            with st.spinner("Listando..."):
                files = list_files_in_folder(drive_service, folder_id)
                st.session_state["drive_files"] = files
                st.success(f"{len(files)} arquivo(s) encontrados.")
    if "drive_files" in st.session_state:
        files = st.session_state["drive_files"]
    else:
        files = []

    st.subheader("Arquivos encontrados")
    if files:
        df_files = pd.DataFrame([{
            "name": f.get("name"),
            "id": f.get("id"),
            "mimeType": f.get("mimeType"),
            "modifiedTime": f.get("modifiedTime"),
            "size": f.get("size")
        } for f in files])
        st.dataframe(df_files)
    else:
        st.info("Nenhum arquivo listado ainda. Clique em 'Listar arquivos na pasta'.")

    st.markdown("---")
    st.subheader("Processamento em lote")
    selected = st.multiselect("Selecione arquivos para processar (por nome)", options=[f.get("name") for f in files], default=[f.get("name") for f in files][:5])

    if st.button("Processar arquivos selecionados"):
        if not spreadsheet_id:
            st.error("Insira o Google Sheets ID (sidebar) antes de processar.")
            st.stop()
        to_process = [f for f in files if f.get("name") in selected]
        overall_rows = []
        progress_bar = st.progress(0)
        for i, f in enumerate(to_process):
            fname = f.get("name")
            fid = f.get("id")
            st.info(f"Processando: {fname}")
            with st.spinner(f"Baixando {fname}..."):
                tmpf = tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(fname)[1])
                tmpf.close()
                try:
                    download_drive_file(drive_service, fid, tmpf.name)
                except Exception as e:
                    st.error(f"Erro ao baixar {fname}: {e}")
                    continue

            # if xml present
            if fname.lower().endswith(".xml"):
                try:
                    xml_rows = parse_nfe_xml(tmpf.name)
                    rows = build_rows_from_file(fname, xml_rows=xml_rows, metodo="xml")
                    overall_rows.extend(rows)
                    st.success(f"{len(rows)} itens extraídos (XML).")
                except Exception as e:
                    st.error(f"Erro ao parsear XML {fname}: {e}")
            elif fname.lower().endswith(".pdf"):
                try:
                    images = pdf_to_images(tmpf.name)
                    combined_items = []
                    base_text_info = {"fornecedor_razao_social": None, "fornecedor_cnpj": None, "nota_numero": None,
                                      "nota_data": None, "nota_valor_total": None, "cpf_associado": None, "observacoes": ""}
                    for img_b in images:
                        text = vision_document_ocr(vision_client, img_b)
                        info = extract_basic_fields_from_text(text)
                        # merge basic info (prefer first non-null)
                        for k,v in info.items():
                            if base_text_info.get(k) is None and v:
                                base_text_info[k] = v
                        items = extract_items_from_text_lines(text)
                        combined_items.extend(items)
                    rows = build_rows_from_file(fname, xml_rows=None, ocr_text=base_text_info, ocr_items=combined_items, metodo="vision")
                    overall_rows.extend(rows)
                    st.success(f"{len(rows)} itens extraídos (Vision OCR heurístico).")
                except Exception as e:
                    st.error(f"Erro ao processar PDF {fname}: {e}")
            elif any(fname.lower().endswith(ext) for ext in [".jpg", ".jpeg", ".png"]):
                try:
                    with open(tmpf.name, "rb") as fimg:
                        img_b = fimg.read()
                    text = vision_document_ocr(vision_client, img_b)
                    base = extract_basic_fields_from_text(text)
                    items = extract_items_from_text_lines(text)
                    rows = build_rows_from_file(fname, xml_rows=None, ocr_text=base, ocr_items=items, metodo="vision")
                    overall_rows.extend(rows)
                    st.success(f"{len(rows)} itens extraídos (imagem).")
                except Exception as e:
                    st.error(f"Erro ao processar imagem {fname}: {e}")
            else:
                st.warning(f"Formato não suportado: {fname}")

            # update progress
            progress_bar.progress(int((i+1)/len(to_process)*100))

        # Preview + edit
        if overall_rows:
            df = pd.DataFrame(overall_rows)
            st.markdown("### Pré-visualização das linhas extraídas (edite se necessário)")
            edited = st.data_editor(df, num_rows="dynamic")
            st.markdown("Quando estiver pronto, clique em **Enviar para Google Sheets**.")
            if st.button("Enviar para Google Sheets"):
                with st.spinner("Gravando no Google Sheets..."):
                    resp = append_rows_to_sheet(sheets_service, spreadsheet_id, edited.to_dict(orient="records"), sheet_name=sheet_name)
                    st.success("Linhas enviadas.")
                    st.write(resp)
        else:
            st.info("Nenhuma linha extraída.")

if __name__ == "__main__":
    main()

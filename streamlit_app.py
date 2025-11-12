# streamlit_app.py
"""
Streamlit app: varre uma pasta do Google Drive, detecta arquivos novos,
extrai campos de notas fiscais e grava no Google Sheets.
- Requer: colocar JSON da Service Account em st.secrets["GOOGLE_SERVICE_ACCOUNT"]
- Fluxo: listar pasta -> comparar com Processed_Files sheet -> processar novos -> anotar Processed_Files
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
    # Se for Google Docs (document) precisamos usar export; aqui detectamos pelo mime se passado
    meta = service.files().get(fileId=file_id, fields="id,name,mimeType").execute()
    mime = meta.get("mimeType")
    name = meta.get("name")
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
        return dest_path, mime
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
        return dest_path, mime

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
CNPJ_RE = re.compile(r"(?:CNPJ[:\s]*|C\.?NPJ[:\s]*|CNPJ\s*)?([0-9]{2}[\.\/-]?[0-9]{3}[\.\/-]?[0-9]{3}[\/-]?[0-9]{4}[-]?[0-9]{2})")
CPF_RE = re.compile(r"(?:CPF[:\s]*|CPF\s*)?([0-9]{3}[\.\/-]?[0-9]{3}[\.\/-]?[0-9]{3}[-]?[0-9]{2})")
VAL_RE = re.compile(r"R\$\s*[0-9\.,]+|[0-9]{1,3}(?:[\.][0-9]{3})*(?:[\,][0-9]{2})|[0-9]+[\,\.][0-9]{2}")
DATE_RE = re.compile(r"([0-3]?[0-9][\/\-][0-1]?[0-9][\/\-][0-9]{2,4}|[0-9]{4}-[0-9]{2}-[0-9]{2})")
NF_RE = re.compile(r"(?:N(?:\.|º|o)?\s*F(?:iscal)?[:\s]*|Nota\s+Fiscal[:\s]*|N[:º\s]*)([0-9\-/\.]+)")

def normalize_money(s):
    if not s:
        return None
    s = s.strip()
    s = re.sub(r"[Rr]\$\s*", "", s)
    s = s.replace(".", "").replace(",", ".")
    m = re.findall(r"[0-9]+(?:\.[0-9]+)?", s)
    if m:
        try:
            return float(m[0])
        except:
            return None
    return None

def extract_fields_from_text(text):
    out = {}
    m = CNPJ_RE.search(text)
    out["cnpj"] = m.group(1) if m else None
    m = CPF_RE.search(text)
    out["cpf"] = m.group(1) if m else None
    m = DATE_RE.search(text)
    out["data_compra"] = m.group(1) if m else None
    m = NF_RE.search(text)
    out["numero_nota"] = m.group(1) if m else None
    total = None
    for keyword in ["valor total", "total da nota", "total", "valor da nota", "total geral"]:
        idx = text.lower().find(keyword)
        if idx != -1:
            snippet = text[idx: idx + 150]
            mval = VAL_RE.search(snippet)
            if mval:
                total = normalize_money(mval.group(0))
                break
    if total is None:
        all_vals = VAL_RE.findall(text)
        if all_vals:
            nums = [(v, normalize_money(v)) for v in all_vals]
            nums_parsed = [t for t in nums if t[1] is not None]
            if nums_parsed:
                total = max(nums_parsed, key=lambda x: x[1])[1]
    out["valor_total"] = total
    company = None
    if out["cnpj"]:
        lines = text.splitlines()
        for i,l in enumerate(lines):
            if out["cnpj"] in l:
                if i>0:
                    company = lines[i-1].strip()
                break
    if not company:
        for keyword in ["LTDA", "Ltda", "MEI", "EIRELI", "S.A.", "SA", "S A"]:
            m = re.search(r".{2,80}"+keyword+r".{0,40}", text)
            if m:
                company = m.group(0).strip()
                break
    if not company:
        for l in text.splitlines():
            if len(l.strip())>4 and l.strip()==l.strip().upper():
                company = l.strip()
                break
    out["empresa"] = company
    address = None
    for keyword in ["Endereço","Endereço:", "Rua ", "R. ", "Av ", "Avenida", "Logradouro"]:
        idx = text.find(keyword)
        if idx!=-1:
            snippet = text[idx: idx+120]
            address = snippet.replace("Endereço:", "").strip()
            break
    out["endereco"] = address
    items=[]
    for line in text.splitlines():
        if re.search(r"R\$|\d+,\d{2}|\d+\.\d{2}", line):
            if len(line.strip())>5 and (len(re.sub(r"[0-9\W]","",line))>0):
                items.append(line.strip())
    out["itens_descricoes"] = "\n".join(items[:40]) if items else None
    return out

# ---------- UI inputs ----------
st.sidebar.header("Parâmetros")
drive_folder_id = st.sidebar.text_input("ID da pasta no Google Drive (folderId)")
spreadsheet_id = st.sidebar.text_input("ID do Google Sheets (spreadsheetId)")
sheet_tab = st.sidebar.text_input("Nome da aba para dados", value="NF_Import")
processed_tab = st.sidebar.text_input("Aba para arquivos processados", value="Processed_Files")

st.sidebar.markdown("Depois de preencher, clique em 'Verificar nova(s) NF(s)'")

# ---------- Main processing ----------
if st.sidebar.button("Verificar nova(s) NF(s)"):
    if not drive_folder_id or not spreadsheet_id:
        st.error("Forneça folderId do Drive e spreadsheetId do Sheets.")
        st.stop()

    creds = get_google_creds()
    drive = build_drive_service(creds)
    gs_client = build_sheets_client(creds)

    with st.spinner("Listando arquivos na pasta..."):
        files = list_files_in_folder(drive, drive_folder_id)
    st.success(f"{len(files)} arquivo(s) encontrados na pasta.")

    # abrir planilha e carregar abas necessárias
    try:
        sh = gs_client.open_by_key(spreadsheet_id)
    except Exception as e:
        st.error(f"Erro ao abrir planilha: {e}")
        st.stop()

    # garantir a aba NF_Import
    try:
        ws_data = sh.worksheet(sheet_tab)
    except Exception:
        ws_data = sh.add_worksheet(title=sheet_tab, rows="1000", cols="30")

    # garantir a aba Processed_Files
    try:
        ws_proc = sh.worksheet(processed_tab)
    except Exception:
        ws_proc = sh.add_worksheet(title=processed_tab, rows="1000", cols="10")

    # ler processados
    proc_df = pd.DataFrame(ws_proc.get_all_records()) if ws_proc.get_all_records() else pd.DataFrame(columns=["fileId","name","mimeType","processed_at","modifiedTime"])
    processed_ids = set(proc_df["fileId"].astype(str).tolist()) if not proc_df.empty else set()

    # identificar novos
    new_files = [f for f in files if str(f.get("id")) not in processed_ids]
    st.write(f"Novos arquivos a processar: {len(new_files)}")

    results_rows = []
    processed_rows = []

    for f in new_files:
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
                extracted = extract_text_via_ocr(path)

            if not extracted or len(extracted.strip())==0:
                st.warning(f"Nenhum texto extraído do arquivo {name}. Pulei.")
                # registrar mesmo assim como processado para evitar loop (opcional)
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

    # gravar resultados na aba NF_Import
    if results_rows:
        try:
            existing_data = pd.DataFrame(ws_data.get_all_records()) if ws_data.get_all_records() else pd.DataFrame()
            new_data_df = pd.DataFrame(results_rows)
            if existing_data.empty:
                set_with_dataframe(ws_data, new_data_df, include_index=False, include_column_header=True)
            else:
                combined = pd.concat([existing_data, new_data_df], ignore_index=True)
                set_with_dataframe(ws_data, combined, include_index=False, include_column_header=True)
            st.success(f"{len(results_rows)} linha(s) gravadas em '{sheet_tab}'.")
        except Exception as e:
            st.error(f"Erro ao gravar dados: {e}")

    # gravar Processed_Files
    if processed_rows:
        try:
            existing_proc = pd.DataFrame(ws_proc.get_all_records()) if ws_proc.get_all_records() else pd.DataFrame()
            new_proc_df = pd.DataFrame(processed_rows)
            if existing_proc.empty:
                set_with_dataframe(ws_proc, new_proc_df, include_index=False, include_column_header=True)
            else:
                # evitar duplicatas: concat e drop_duplicates por fileId mantendo última ocorrência
                combined_proc = pd.concat([existing_proc, new_proc_df], ignore_index=True)
                combined_proc = combined_proc.sort_values("processed_at").drop_duplicates(subset=["fileId"], keep="last")
                set_with_dataframe(ws_proc, combined_proc, include_index=False, include_column_header=True)
            st.success(f"{len(processed_rows)} arquivo(s) marcados como processados.")
        except Exception as e:
            st.error(f"Erro ao gravar Processed_Files: {e}")

    # mostrar resumo
    st.subheader("Resumo de execução")
    st.write(f"Arquivos encontrados: {len(files)}")
    st.write(f"Novos processados: {len(results_rows)} (gravados em '{sheet_tab}')")
    st.write(f"Arquivos marcados processados: {len(processed_rows)} (gravados em '{processed_tab}')")

    # exibir primeiras linhas gravadas
    if results_rows:
        st.subheader("Amostra dos dados gravados")
        st.dataframe(pd.DataFrame(results_rows).head(20))

    if processed_rows:
        st.subheader("Amostra dos arquivos marcados como processados")
        st.dataframe(pd.DataFrame(processed_rows).head(20))

st.markdown("---")
st.markdown("**Notas / recomendações**")
st.markdown(
    """
    - A Service Account precisa ser *Viewer* na pasta (ou nos arquivos) e *Editor* na planilha.
    - O app registra os `fileId` processados na aba `Processed_Files` para não reprocessar.
    - Para arquivos escaneados, OCR depende do binário Tesseract disponível no ambiente.
    - Podemos adaptar para:
        * Processar somente arquivos com extensão específica (.pdf, .docx, .xml)
        * Rodar em lote (bulk) e enviar relatório por e-mail
        * Usar Google Vision API se precisar de OCR de alta qualidade
    """
)

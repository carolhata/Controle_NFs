# streamlit_app.py
"""
App Streamlit: importar nota fiscal do Google Drive -> extrair campos -> escrever no Google Sheets
Arquivos esperados: Google Docs, PDF pesquisável, PDF escaneado/imagem
Autenticação: via Service Account JSON (colocado no Streamlit Secrets como "GOOGLE_SERVICE_ACCOUNT")
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

# ---------- Config / constantes ----------
# escopos mínimos para Drive + Sheets
SCOPES = [
    "https://www.googleapis.com/auth/drive.readonly",
    "https://www.googleapis.com/auth/spreadsheets"
]

st.set_page_config(page_title="Controle NF — Extrator Drive → Sheets", layout="wide")

st.title("📥 → 🧾 → 📊  Importador de Nota Fiscal (Google Drive → Google Sheets)")

st.markdown(
    """
    **Fluxo:** o app acessa um arquivo no Google Drive (ID do arquivo), tenta extrair o texto,
    busca campos chave (Nome da empresa, CNPJ, descrição dos itens com valores, data da compra, valor total, número da nota, CPF e endereço),
    e escreve uma linha (ou várias, se houver várias notas) em uma planilha do Google Sheets.
    """
)

# ---------- Helpers de autenticação ----------
@st.cache_resource
def get_google_creds():
    """
    Lê a credencial da conta de serviço a partir do Streamlit secrets.
    O segredo deve estar em: st.secrets["GOOGLE_SERVICE_ACCOUNT"] (string JSON ou dict).
    """
    if "GOOGLE_SERVICE_ACCOUNT" not in st.secrets:
        st.error("Credenciais Google não encontradas no Streamlit Secrets. Adicione 'GOOGLE_SERVICE_ACCOUNT'.")
        st.stop()
    sa = st.secrets["GOOGLE_SERVICE_ACCOUNT"]
    # st.secrets já traz um dict se você colou o JSON no secrets, ou uma string; trate ambos
    if isinstance(sa, str):
        import json
        sa_info = json.loads(sa)
    else:
        sa_info = sa
    creds = Credentials.from_service_account_info(sa_info, scopes=SCOPES)
    return creds

# ---------- Drive / Sheets helpers ----------
def build_drive_service(creds):
    return build("drive", "v3", credentials=creds, cache_discovery=False)

def build_sheets_client(creds):
    # usar gspread authorize
    return authorize(creds)

def download_drive_file(service, file_id, dest_path):
    # obtém metadados
    meta = service.files().get(fileId=file_id, fields="id,name,mimeType").execute()
    mime = meta.get("mimeType")
    name = meta.get("name")
    # se for Google Docs, exportar para text/plain ou docx
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
                page_text = p.extract_text()
                if page_text:
                    text += page_text + "\n"
    except Exception as e:
        st.warning(f"pdfplumber falhou: {e}")
    return text

def extract_text_via_ocr(path):
    text = ""
    try:
        # converte cada página/image em texto
        # tentamos abrir com PIL — funciona para imagens; se for PDF tentaremos com pdfplumber para imagens das páginas
        if path.lower().endswith(".pdf"):
            try:
                with pdfplumber.open(path) as pdf:
                    for p in pdf.pages:
                        im = p.to_image(resolution=200).original
                        txt = pytesseract.image_to_string(im, lang='por+eng')
                        text += txt + "\n"
            except Exception as e:
                st.warning(f"OCR via pdf->imagem falhou: {e}")
        else:
            im = Image.open(path)
            text = pytesseract.image_to_string(im, lang='por+eng')
    except Exception as e:
        st.error(f"OCR falhou: {e}")
    return text

# ---------- Parsers para campos brasileiros ----------
# CNPJ: 14 dígitos (com ou sem pontuação)
CNPJ_RE = re.compile(r"(?:CNPJ[:\\s]*|C\.?NPJ[:\\s]*|CNPJ\\s*)?([0-9]{2}[\\.\\/-]?[0-9]{3}[\\.\\/-]?[0-9]{3}[\\/\\-]?[0-9]{4}[\\-]?[0-9]{2})")
CPF_RE = re.compile(r"(?:CPF[:\\s]*|CPF\\s*)?([0-9]{3}[\\.\\/-]?[0-9]{3}[\\.\\/-]?[0-9]{3}[\\-]?[0-9]{2})")
# valor total: procura por R$ 1.234,56 ou 1234.56 / 1234,56
VAL_RE = re.compile(r"R\\$\\s*[0-9\\.,]+|[0-9]{1,3}(?:[\\.\\,][0-9]{3})*(?:[\\.,][0-9]{2})")
# data: dd/mm/yyyy ou yyyy-mm-dd
DATE_RE = re.compile(r"([0-3]?[0-9][/\\-][0-1]?[0-9][/\\-][0-9]{2,4}|[0-9]{4}-[0-9]{2}-[0-9]{2})")
# numero nota: procura por 'NF', 'Nº', 'Nº nota', 'Nota Fiscal' seguido de números
NF_RE = re.compile(r"(?:N(?:\\.|º|o)?\\s*F(?:iscal)?[:\\s]*|Nota\\s+Fiscal[:\\s]*|N[:º\\s]*)([0-9\\-\\/\\.]+)")

def normalize_money(s):
    if not s:
        return None
    s = s.strip()
    # remove R$
    s = re.sub(r"[Rr]\\$\\s*", "", s)
    # if has comma as decimal separator and dot as thousand, handle
    s = s.replace(".", "").replace(",", ".")
    try:
        return float(re.findall(r"[0-9]+(?:\\.[0-9]+)?", s)[0])
    except Exception:
        return None

def extract_fields_from_text(text):
    # tenta extrair os campos solicitados
    out = {}
    # CNPJ
    m = CNPJ_RE.search(text)
    out["cnpj"] = m.group(1) if m else None
    # CPF
    m = CPF_RE.search(text)
    out["cpf"] = m.group(1) if m else None
    # Datas (pega a primeira)
    m = DATE_RE.search(text)
    out["data_compra"] = m.group(1) if m else None
    # Numero NF
    m = NF_RE.search(text)
    out["numero_nota"] = m.group(1) if m else None
    # Valor total: tentamos localizar palavras-chaves próximas a Tot(al)
    total = None
    # procura ocorrências de palavras semelhantes
    for keyword in ["valor total", "total da nota", "total", "valor da nota", "valor liquido", "valor bruto", "total geral"]:
        idx = text.lower().find(keyword)
        if idx != -1:
            # pega trecho a direita
            snippet = text[idx: idx + 150]
            mval = VAL_RE.search(snippet)
            if mval:
                total = normalize_money(mval.group(0))
                break
    # fallback: primeiro valor grande encontrado
    if total is None:
        all_vals = VAL_RE.findall(text)
        if all_vals:
            # normalize and pick the largest numeric (heurística)
            nums = [(v, normalize_money(v)) for v in all_vals]
            nums_parsed = [t for t in nums if t[1] is not None]
            if nums_parsed:
                total = max(nums_parsed, key=lambda x: x[1])[1]
    out["valor_total"] = total
    # Nome da empresa: heurística -> linha acima do CNPJ ou primeira LINE com palavra 'Ltd','LTDA','EIRELI','ME','MEI' ou caixa alta
    company = None
    if out["cnpj"]:
        # procurar linha que contenha o cnpj e pegar a linha anterior
        lines = text.splitlines()
        for i,l in enumerate(lines):
            if out["cnpj"] in l:
                if i>0:
                    company = lines[i-1].strip()
                break
    if not company:
        # procurar padrões de razão social
        for keyword in ["LTDA", "Ltda", "MEI", "EIRELI", "S\\.A\\.", "SA", "S A", "S\\.A"]:
            m = re.search(r".{2,80}"+keyword+r".{0,40}", text)
            if m:
                company = m.group(0).strip()
                break
    if not company:
        # fallback: primeira linha longa (provável cabeçalho)
        for l in text.splitlines():
            if len(l.strip())>4 and len(l.strip())<120 and l.strip()==l.strip().upper():
                company = l.strip()
                break
    out["empresa"] = company
    # Endereço: heurística: procurar 'Endereço' ou trecho com 'Rua','Av','Avenida','Logradouro' dentro de linhas próximas ao CNPJ ou nome
    address = None
    for keyword in ["Endereço","Endereço:", "Rua ", "R. ", "Av ", "Avenida", "Logradouro"]:
        idx = text.find(keyword)
        if idx!=-1:
            snippet = text[idx: idx+120]
            address = snippet.replace("Endereço:", "").strip()
            break
    out["endereco"] = address
    # Descrição de itens com valores: tentativa de extrair blocos com padrão 'produto - R$ valor' ou linhas com 'x R$'
    items = []
    for line in text.splitlines():
        if re.search(r"R\\$|\\d+,\\d{2}|\\d+\\.\\d{2}", line):
            # heurística: linha que contém valor e alguma descrição (palavras)
            if len(line.strip())>5 and (len(re.sub(r"[0-9\\W]", "", line))>0):
                items.append(line.strip())
    out["itens_descricoes"] = "\\n".join(items[:20]) if items else None
    return out

# ---------- UI ----------
st.sidebar.header("Configuração")
st.sidebar.markdown(
    """
    1. Coloque o JSON da Service Account em **Settings → Secrets** do Streamlit como `GOOGLE_SERVICE_ACCOUNT`.
    2. Forneça o ID do arquivo do Google Drive e o ID da planilha do Google Sheets.
    """
)

st.sidebar.markdown("**Entradas**")
file_id = st.sidebar.text_input("ID do arquivo no Google Drive (fileId)")
sheet_id = st.sidebar.text_input("ID do Google Sheets (spreadsheetId)")
sheet_tab = st.sidebar.text_input("Nome da guia (aba) para inserir os dados", value="NF_Import")

if st.sidebar.button("Rodar extração"):
    if not file_id or not sheet_id:
        st.error("Forneça fileId do Drive e spreadsheetId do Sheets na barra lateral.")
    else:
        creds = get_google_creds()
        drive = build_drive_service(creds)
        gs_client = build_sheets_client(creds)

        with st.spinner("Baixando arquivo do Drive..."):
            tmpf = tempfile.NamedTemporaryFile(delete=False, suffix=".bin")
            path, mime = download_drive_file(drive, file_id, tmpf.name)
            tmpf.close()
            st.success(f"Arquivo baixado: {os.path.basename(path)} (mime: {mime})")

        # detect mime local se necessário
        kind = magic.from_file(path, mime=True)
        st.write(f"Detecção MIME local: {kind}")

        extracted = ""
        # se for texto (exportado google doc) ou txt
        if path.lower().endswith(".txt") or mime=="text/plain":
            with open(path, "r", encoding="utf-8", errors="ignore") as f:
                extracted = f.read()
        elif path.lower().endswith(".pdf") or "pdf" in kind:
            st.info("Tentando extrair texto do PDF com pdfplumber...")
            extracted = extract_text_from_pdf(path)
            if not extracted or len(extracted.strip())<30:
                st.info("Pouco texto extraído — tentando OCR (pytesseract).")
                extracted = extract_text_via_ocr(path)
        else:
            # tentar abrir como imagem
            try:
                st.info("Tentando OCR de imagem...")
                extracted = extract_text_via_ocr(path)
            except Exception as e:
                st.error(f"Não foi possível extrair texto do arquivo: {e}")

        if not extracted or len(extracted.strip())==0:
            st.error("Nenhum texto extraído do arquivo. Verifique se o arquivo é pesquisável ou use OCR/Google Vision.")
        else:
            st.subheader("Trecho extraído (pré-visualização)")
            st.text_area("Texto extraído (role)", value=extracted[:4000], height=250)

            st.info("Analisando texto e extraindo campos...")
            fields = extract_fields_from_text(extracted)

            st.write("Campos extraídos (heurísticos):")
            st.json(fields)

            # preparar linha / DataFrame para inserir na planilha
            row = {
                "timestamp_import": datetime.now().isoformat(),
                "empresa": fields.get("empresa"),
                "cnpj": fields.get("cnpj"),
                "descricao_itens": fields.get("itens_descricoes"),
                "data_compra": fields.get("data_compra"),
                "valor_total": fields.get("valor_total"),
                "numero_nota": fields.get("numero_nota"),
                "cpf": fields.get("cpf"),
                "endereco": fields.get("endereco"),
                "drive_file_id": file_id,
                "drive_mime": mime
            }
            df = pd.DataFrame([row])

            st.subheader("Linha a ser gravada no Google Sheets")
            st.dataframe(df)

            # confirmar gravação
            if st.button("Gravar no Google Sheets"):
                try:
                    sh = gs_client.open_by_key(sheet_id)
                except Exception as e:
                    st.error(f"Erro ao abrir planilha: {e}")
                    st.stop()
                # criar aba se não existir
                try:
                    worksheet = None
                    try:
                        worksheet = sh.worksheet(sheet_tab)
                    except Exception:
                        worksheet = sh.add_worksheet(title=sheet_tab, rows="100", cols="20")
                    # ler existente e anexar
                    existing = pd.DataFrame(worksheet.get_all_records())
                    if existing.empty:
                        set_with_dataframe(worksheet, df, include_index=False, include_column_header=True)
                    else:
                        new_df = pd.concat([existing, df], ignore_index=True)
                        set_with_dataframe(worksheet, new_df, include_index=False, include_column_header=True)
                    st.success("Gravado com sucesso no Google Sheets.")
                except Exception as e:
                    st.error(f"Erro ao gravar no Google Sheets: {e}")

st.markdown("---")
st.markdown("### Validação / Observações importantes")
st.markdown(
    """
    - O parser usa heurísticas — para documentos muito diferentes (layout diverso, PDF escaneado), os campos podem exigir regras específicas.
    - Para OCR confiável em PDFs escaneados recomendamos usar **Google Vision API** (melhor taxa de acerto), o que exigirá um conjunto de credenciais GCP adicionais.
    - Se for usar `pytesseract`, o binário **Tesseract** deve estar instalado no servidor (no Streamlit Cloud pode não estar presente por padrão).
    - Guardamos apenas o `fileId` no registro — o arquivo original permanece no seu Drive.
    """
)

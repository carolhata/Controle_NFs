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
                    # Usando lang='por+eng'
                    txt = pytesseract.image_to_string(im, lang='por+eng')
                    text += txt + "\n"
        else:
            im = Image.open(path)
            # Usando lang='por+eng'
            txt = pytesseract.image_to_string(im, lang='por+eng')
            text += txt
    except Exception as e:
        st.warning(f"OCR falhou: {e}")
    return text

# ---------- Parsers ----------
CNPJ_RE = re.compile(r"(?:CNPJ[:\s]*|C\.?NPJ[:\s]*|CNPJ\s*)?([0-9]{2}[\.\/-]?[0-9]{3}[\.\/-]?[0-9]{3}[\/\-]?[0-9]{4}[-]?[0-9]{2})")
CPF_RE = re.compile(r"(?:CPF[:\s]*|CPF\s*)?([0-9]{3}[\.\/-]?[0-9]{3}[\.\/-]?[0-9]{3}[-]?[0-9]{2})")
# Ajustado VAL_RE para ser mais explícito para formatos de dinheiro
VAL_RE = re.compile(r"R\$?\s*(\d{1,3}(?:\.\d{3})*(?:,\d{2})|\d+,\d{2})") # Captura valores como 1.234,56 ou 123,45
DATE_RE = re.compile(r"([0-3]?[0-9][\/\-][0-1]?[0-9][\/\-][0-9]{2,4}|[0-9]{4}-[0-9]{2}-[0-9]{2})")
# Ajustado NF_RE para incluir 'SAT No.' e outras variações
NF_RE = re.compile(r"(?:N(?:\.|º|o)?\s*F(?:iscal)?[:\s]*|Nota\s+Fiscal[:\s]*|N[:º\s]*|SAT\s*No\.?\s*|\s*NFC-e\s*|\s*NF-e\s*|Nr\s+Documento[:\s]*)([0-9\-/\.]+)")


def normalize_money(s):
    if not s:
        return None
    s = s.strip()
    s = re.sub(r"[Rr]\$\s*", "", s) # Remove "R$"
    s = s.replace(".", "") # Remove separador de milhar
    s = s.replace(",", ".") # Troca vírgula por ponto para decimais
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

    # Extrair Data de Compra
    m = DATE_RE.search(text)
    out["data_compra"] = m.group(1) if m else None
    if out["data_compra"]: st.write(f"Data de Compra encontrada: {out['data_compra']}")

    # Extrair Número da Nota
    m = NF_RE.search(text)
    out["numero_nota"] = m.group(1) if m else None
    if out["numero_nota"]: st.write(f"Número da Nota encontrado: {out['numero_nota']}")

    # Extrair Valor Total
    total = None
    for keyword in ["valor total", "total da nota", "total", "valor da nota", "total geral", "valor a pagar"]:
        idx = text.lower().find(keyword)
        if idx != -1:
            snippet = text[idx: idx + 150]
            mval = VAL_RE.search(snippet)
            if mval:
                total = normalize_money(mval.group(1)) # group(1) pois a regex tem grupo de captura
                if total is not None:
                    break
    if total is None: # Se não encontrou por palavra-chave, tenta o maior valor numérico
        all_vals = VAL_RE.findall(text)
        if all_vals:
            nums_parsed = [normalize_money(v) for v in all_vals if normalize_money(v) is not None]
            if nums_parsed:
                total = max(nums_parsed) # Pega o maior valor numérico encontrado
    out["valor_total"] = total
    if out["valor_total"]: st.write(f"Valor Total encontrado: {out['valor_total']}")

    # Extrair Empresa (Razão Social) - Lógica aprimorada
    company = None
    lines = [l.strip() for l in text.splitlines() if l.strip()] # Todas as linhas não vazias
    
    if lines:
        # 1. Tentar a linha acima do CNPJ se o CNPJ for encontrado e a linha for razoável
        m_cnpj = CNPJ_RE.search(text)
        if m_cnpj:
            cnpj_pos = m_cnpj.start()
            text_before_cnpj = text[:cnpj_pos].splitlines()
            text_before_cnpj = [l.strip() for l in text_before_cnpj if l.strip()]

            if text_before_cnpj:
                # Tentar a última linha antes do CNPJ
                candidate_company_line = text_before_cnpj[-1]
                # Heurística: se a linha é predominantemente maiúscula, sem muitos números ou datas
                if candidate_company_line.isupper() and len(candidate_company_line) > 5 and not re.search(r'\d{1,2}/\d{1,2}/\d{2,4}|\d{3}-\d{3}|\d+\,\d+|\d+\.\d+', candidate_company_line):
                    company = candidate_company_line
                elif len(text_before_cnpj) > 1: # Tentar a penúltima se a última não for boa
                    candidate_company_line = text_before_cnpj[-2]
                    if candidate_company_line.isupper() and len(candidate_company_line) > 5 and not re.search(r'\d{1,2}/\d{1,2}/\d{2,4}|\d{3}-\d{3}|\d+\,\d+|\d+\.\d+', candidate_company_line):
                        company = candidate_company_line

        # 2. Se ainda não encontrou, procurar por palavras-chave de empresa na primeira parte do documento
        if not company:
            search_area = "\n".join(lines[:10]) # Limitar a busca às primeiras 10 linhas
            company_keywords = ["LTDA", "MEI", "EIRELI", "S.A.", "SA", "S A", "COMERCIO", "SERVICOS", "MATERIAIS"] # Adicionado MATERIAIS
            for keyword in company_keywords:
                # Procurar a linha que contém a palavra-chave e não é muito curta
                for l in search_area.splitlines():
                    if keyword in l.upper() and len(l) > 10: # Aumentar o mínimo de caracteres
                        company = l.strip()
                        break
                if company:
                    break

        # 3. Último recurso: a primeira linha significativa em maiúsculas (no topo)
        if not company and lines:
            for l in lines[:5]: # Procurar nas primeiras 5 linhas
                if l.isupper() and len(l) > 10 and not re.search(r'\d{1,2}/\d{1,2}/\d{2,4}|\d{3}-\d{3}|\d+\,\d+|\d+\.\d+', l): # Checar se não parece endereço ou data
                    company = l
                    break
    
    out["empresa"] = company
    if out["empresa"]: st.write(f"Empresa encontrada: {out['empresa']}")

    # Extrair Endereço
    address = None
    address_keywords = ["Endereço", "Endereço:", "Rua ", "R. ", "Av ", "Avenida", "Logradouro", "BAIRRO", "CIDADE", "CEP"]
    # Buscar em um snippet maior ou em várias linhas após uma palavra-chave
    for keyword in address_keywords:
        idx = text.find(keyword)
        if idx != -1:
            snippet = text[idx: idx + 200] # Aumentar o snippet de busca
            # Tentar pegar a linha completa ou as 2-3 linhas seguintes para o endereço
            address_lines = snippet.splitlines()
            if address_lines:
                # Filtrar linhas que parecem ser endereços (contém palavras-chave de rua/número/bairro/cidade/cep)
                potential_address_lines = []
                for al in address_lines[:5]: # Olhar as próximas 5 linhas
                    if any(sub_keyword in al for sub_keyword in ["Rua", "Av", "Bairro", "CEP", "Cidade", "Numero", "Nº", ","]) and len(al) > 10:
                        potential_address_lines.append(al.strip())
                
                if potential_address_lines:
                    address = " ".join(potential_address_lines)
            if address:
                break
    out["endereco"] = address
    if out["endereco"]: st.write(f"Endereço encontrado: {out['endereco']}")

    # Extrair Descrição de Itens - Lógica aprimorada
    items=[]
    # Palavras-chave para ignorar linhas que não são itens de descrição
    ignore_keywords_items = ["SUBTOTAL", "TOTAL", "IMPOSTOS", "ICMS", "ISS", "DESCONTO", 
                             "FORMA DE PAGAMENTO", "TROCO", "CRÉDITO", "DÉBITO", "DINHEIRO", 
                             "VALOR", "CPF DO CONSUMIDOR", "CNPJ", "IE", "CUPOM FISCAL", 
                             "SAT NO", "NOTA FISCAL", "LANÇAMENTO", "DATA"]

    for line in text.splitlines():
        line_clean = line.strip()
        # Verifica se contém padrão de dinheiro/número (ex: "10,00" ou "10.00")
        if re.search(r"\d+,\d{2}|\d+\.\d{2}", line_clean):
            # Verifica o comprimento mínimo e que tem alguma letra (não só números e símbolos)
            if len(line_clean) > 5 and re.search(r"[a-zA-Z]", line_clean):
                # Verifica se a linha NÃO contém palavras-chave de ignorar (case-insensitive)
                if not any(keyword in line_clean.upper() for keyword in ignore_keywords_items):
                    items.append(line_clean)
    out["itens_descricoes"] = "\n".join(items[:40]) if items else None
    if out["itens_descricoes"]: st.write(f"Itens encontrados:\n{out['itens_descricoes']}")

    return out

# ---------- UI inputs ----------
st.sidebar.header("Parâmetros")

# Carregar do secrets
creds_check = get_google_creds() # Apenas para garantir que secrets está configurado

drive_folder_id = st.sidebar.text_input("ID da pasta no Google Drive (folderId)", value=st.secrets.get("DRIVE_FOLDER_ID", ""))
spreadsheet_id = st.sidebar.text_input("ID do Google Sheets (spreadsheetId)", value=st.secrets.get("GOOGLE_SHEET_ID", ""))
sheet_tab = st.sidebar.text_input("Nome da aba para dados", value=st.secrets.get("DATA_SHEET_NAME", "NF_Import"))
processed_tab = st.sidebar.text_input("Aba para arquivos processados", value=st.secrets.get("PROCESSED_FILES_SHEET_NAME", "Processed_Files"))

st.sidebar.markdown("Depois de preencher, clique em 'Verificar nova(s) NF(s)'")

# ---------- Main processing ----------
if st.sidebar.button("Verificar nova(s) NF(s)"):
    if not drive_folder_id or not spreadsheet_id:
        st.error("Forneça folderId do Drive e spreadsheetId do Sheets nas configurações de Secrets ou nos campos acima.")
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
                st.info("Tipo de arquivo desconhecido para extração direta — tentando OCR.")
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

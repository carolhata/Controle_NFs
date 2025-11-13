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

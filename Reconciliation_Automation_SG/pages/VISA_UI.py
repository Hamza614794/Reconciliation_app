import streamlit as st
import pandas as pd
import os
import re
import numpy as np
import zipfile
from datetime import datetime
import plotly.graph_objects as go
from Reconciliation_Automation_SG.processing_bank_sources_VISA import *


def upload_all_sources():

    df_cybersource = pd.DataFrame(columns=default_columns_cybersource)
    df_pos = pd.DataFrame(columns=default_columns_pos)
    df_sai_manuelle = pd.DataFrame(columns=default_columns_saisie_manuelle)

    try:
        if uploaded_cybersource_file:
            cybersource_file_path = save_uploaded_file(uploaded_cybersource_file)
            validate_file_name_and_date(uploaded_cybersource_file.name, 'CYBERSOURCE', date_to_validate=None)
            df_cybersource = reading_cybersource(cybersource_file_path)
            VISA_transactions_cybersource = df_cybersource[df_cybersource['RESEAU'] == 'VISA INTERNATIONAL']
            total_transactions['Cybersource'] = VISA_transactions_cybersource['NBRE_TRANSACTION'].sum()
    except Exception as e:
        st.error(f"Erreur lors du traitement du fichier Cybersource :{e}")

    try:
        if uploaded_pos_file:
            pos_file_path = save_uploaded_file(uploaded_pos_file)
            validate_file_name_and_date(uploaded_pos_file.name, 'POS', date_to_validate=None)
            df_pos = reading_pos(pos_file_path)
            VISA_transactions_pos = df_pos[(df_pos['RESEAU'] == 'VISA INTERNATIONAL') &
                                                 (~df_pos['TYPE_TRANSACTION'].str.endswith('_MDS'))]
            total_transactions['POS'] = VISA_transactions_pos['NBRE_TRANSACTION'].sum()
        else:
            st.error("error")
    except Exception as e:
        st.error(f"Erreur lors du traitement du fichier POS :{e}")

    try:
        if uploaded_sai_manuelle_file:
            sai_manuelle_file_path = save_uploaded_file(uploaded_sai_manuelle_file)
            validate_file_name_and_date(uploaded_sai_manuelle_file.name, 'SAIS_MANU', date_to_validate=None)
            df_sai_manuelle = reading_saisie_manuelle(sai_manuelle_file_path)
            VISA_transactions_sai_manuelle = df_sai_manuelle[df_sai_manuelle['RESEAU'] == 'VISA INTERNATIONAL']
            total_transactions['Saisie Manuelle'] = VISA_transactions_sai_manuelle['NBRE_TRANSACTION'].sum()
    except Exception as e:
        st.error(f"Erreur lors du traitement du fichier de Saisie Manuelle :{e}")

    return df_cybersource, df_sai_manuelle, df_pos


def filter_sources(df_cybersource, df_sai_manuelle, df_pos):
    try:
        filtered_cybersource_df, filtered_saisie_manuelle_df, filtered_pos_df = filtering_sources(df_cybersource, df_sai_manuelle, df_pos)
        return filtered_cybersource_df, filtered_saisie_manuelle_df, filtered_pos_df
    except Exception as e:
        st.error(f"Erreur lors du filtrage des fichiers source :{e}")


def pie_chart():
    if total_transactions['Cybersource'] > 0 or total_transactions['POS'] > 0 or total_transactions['Saisie Manuelle'] > 0:
        st.header("	:bar_chart:  Répartition des transactions par source", divider='grey')

        def create_interactive_pie_chart(total_transactions):
            labels = list(total_transactions.keys())
            sizes = list(total_transactions.values())
            fig = go.Figure(data=[go.Pie(labels=labels, values=sizes, hole=.3, textinfo='label+percent+value')])
            return fig

        fig = create_interactive_pie_chart(total_transactions)
        st.plotly_chart(fig)


def handle_recon(
    filtered_cybersource_df,
    filtered_saisie_manuelle_df,
    filtered_pos_df,
    zip_file_path,
    zip_reject_path
):
    if uploaded_recycled_file:
        # Save the uploaded recycled file
        recycled_file_path = save_uploaded_file(uploaded_recycled_file)
        df_recyc = pd.read_excel(recycled_file_path)

        st.write ("### Les rejets présents dans le fichier ###")
        st.dataframe(df_recyc)
        st.write("La date du filtrage : ", filtering_date)

        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            # Lister tous les fichiers dans le ZIP
            for file_name in zip_ref.namelist():
                if file_name.endswith('.TXT'):

                    with zip_ref.open(file_name) as file:
                        content = file.read().decode('utf-8')
                        # Appliquer l'extraction des données sur le fichier
                        #transaction_data = extract_transaction_data(file_name, content)
                        #transaction_list.append(transaction_data)
                        #transaction_list = []
                        #transaction_data = extract_transaction_data(file_name, content)
                        #transaction_list.append(transaction_data)
                        #print(transaction_list)
                        print(file_name)
        # Perform merging with recycled data
        result_df= merging_with_recycled(
            recycled_file_path,
            filtered_cybersource_df,
            filtered_saisie_manuelle_df,
            filtered_pos_df,
            filtering_date,
            file_name, content, zip_file_path, zip_reject_path
        )
        st.header("Résultats de réconciliation")
        st.dataframe(result_df, use_container_width=True)
    else:
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            # Lister tous les fichiers dans le ZIP
            for file_name in zip_ref.namelist():
                if file_name.endswith('.TXT'):

                    with zip_ref.open(file_name) as file:
                        content = file.read().decode('utf-8')
                        # Appliquer l'extraction des données sur le fichier
                        #transaction_data = extract_transaction_data(file_name, content)
                        #transaction_list.append(transaction_data)
                        #transaction_list = []
                        #transaction_data = extract_transaction_data(file_name, content)
                        #transaction_list.append(transaction_data)
                        #print(transaction_list)
                        print(file_name)
        

        # Handle case without recycled file
        result_df, total_nbre_transactions = merging_sources_without_recycled(
            filtered_cybersource_df, filtered_saisie_manuelle_df, filtered_pos_df, file_name, content, zip_file_path, zip_reject_path
        )
        st.header("Résultats de réconciliation")
        st.dataframe(result_df, use_container_width=True)






def process_zip_and_extract(zip_file, temp_dir="temp_unzipped"):
    """
    Process the uploaded ZIP file, extract all files, and process using `extract_transaction_data`.
    """
    try:
        # Ensure temp directory exists
        if not os.path.exists(temp_dir):
            os.makedirs(temp_dir)

        # Unzip the file
        with zipfile.ZipFile(zip_file, 'r') as z:
            z.extractall(temp_dir)

        # Collect extracted data
        extracted_data = []
        for root, _, files in os.walk(temp_dir):
            for file in files:
                file_path = os.path.join(root, file)

                # Read the file content
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()

                # Extract data
                file_data = extract_transaction_data(file_path, content)
                extracted_data.append(file_data)

        # Cleanup temp directory
        for root, _, files in os.walk(temp_dir):
            for file in files:
                os.remove(os.path.join(root, file))
        os.rmdir(temp_dir)

        # Return collected data as a DataFrame
        return pd.DataFrame(extracted_data)

    except Exception as e:
        st.error(f"Error while processing the ZIP file: {e}")
        return pd.DataFrame()
    

def process_zip_and_extract_EP100(zip_file, temp_dir="temp_unzipped"):
    """
    Process the uploaded ZIP file, extract all files, and process using `extract_transaction_data`.
    """
    try:
        # Ensure temp directory exists
        if not os.path.exists(temp_dir):
            os.makedirs(temp_dir)

        # Unzip the file
        with zipfile.ZipFile(zip_file, 'r') as z:
            z.extractall(temp_dir)

        # Collect extracted data
        #extracted_data = []
        all_file_data = []
        for root, _, files in os.walk(temp_dir):
            for file in files:
                file_path = os.path.join(root, file)

                # Read the file content
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()

                # Extract data from the file
                file_data = extract_EP_rejects(file_path, content)

                # If file_data is not empty, append it to the list
                if not file_data.empty:
                    all_file_data.append(file_data)

        # Concatenate all DataFrames into one
        if all_file_data:
            combined_file_data = pd.concat(all_file_data, ignore_index=True)
        else:
            combined_file_data = pd.DataFrame()  # Return an empty DataFrame if no data
                
        # Cleanup temp directory
        for root, _, files in os.walk(temp_dir):
            for file in files:
                os.remove(os.path.join(root, file))
        os.rmdir(temp_dir)

        # Return collected data as a DataFrame
        return combined_file_data
    except Exception as e:
        st.error(f"Error while processing the ZIP file: {e}")
        return pd.DataFrame()
    

def process_zip_and_extract_EP100_V2(zip_file, temp_dir="temp_unzipped"):
    """
    Process the uploaded ZIP file, extract all files, and collect file_path and content.
    """
    try:
        if not os.path.exists(temp_dir):
            os.makedirs(temp_dir)

        with zipfile.ZipFile(zip_file, 'r') as z:
            z.extractall(temp_dir)

        # Collect extracted data
        extracted_data = []
        for root, _, files in os.walk(temp_dir):
            for file in files:
                file_path = os.path.join(root, file)
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                extracted_data.append({"file_path": file_path, "content": content})

        # Cleanup temporary files
        for root, _, files in os.walk(temp_dir):
            for file in files:
                os.remove(os.path.join(root, file))
        os.rmdir(temp_dir)

        if extracted_data:
            # Return all file paths and contents
            file_paths = [file_data['file_path'] for file_data in extracted_data]
            contents = [file_data['content'] for file_data in extracted_data]
            return file_paths, contents
        else:
            return None, None

    except Exception as e:
        st.error(f"Error while processing the ZIP file: {e}")
        return None, None
    

def process_zip_and_extract_EP747_V2(zip_file, temp_dir="temp_unzipped"):
    """
    Process the uploaded ZIP file, extract all files, and collect file_path and content.
    """
    try:
        if not os.path.exists(temp_dir):
            os.makedirs(temp_dir)

        with zipfile.ZipFile(zip_file, 'r') as z:
            z.extractall(temp_dir)

        # Collect extracted data
        extracted_data = []
        for root, _, files in os.walk(temp_dir):
            for file in files:
                file_path = os.path.join(root, file)
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                extracted_data.append({"file_path": file_path, "content": content})

        # Cleanup temporary files
        for root, _, files in os.walk(temp_dir):
            for file in files:
                os.remove(os.path.join(root, file))
        os.rmdir(temp_dir)

        # Return first file_path and content found
        if extracted_data:
            file_data = extracted_data[0]  # Assuming one file for simplicity
            return file_data['file_path'], file_data['content']
        else:
            return None, None

    except Exception as e:
        st.error(f"Error while processing the ZIP file: {e}")
        return None, None




def main():
    global zip_uploaded_visa_EP747, zip_uploaded_visa_EP100_204, uploaded_cybersource_file, uploaded_pos_file, uploaded_sai_manuelle_file, filtering_date, uploaded_recycled_file, total_transactions
    st.sidebar.image("assets/Logo_hps_0.png", use_column_width=True)
    st.sidebar.divider()
    st.sidebar.page_link("app.py", label="**Accueil**", icon="🏠")
    #st.sidebar.page_link("pages/results_recon.py", label="**:alarm_clock: Historique**")
    #st.sidebar.page_link("pages/Dashboard.py", label="  **📊 Tableau de bord**" )
    st.sidebar.page_link("pages/VISA_UI.py", label="**🔀 Réconciliation VISA**")
    #uploaded_mastercard_file = st.file_uploader(":arrow_down: **Chargez le fichier Mastercard**", type=["001"])
    st.header("📤 :violet[Réconciliation VISA]", divider='rainbow')
    zip_uploaded_visa_EP747 = st.file_uploader(":arrow_down: **Chargez le fichier ZIP EP747**", type=["zip"])
    if zip_uploaded_visa_EP747 is not None:
        try:
            # Process the ZIP file and extract data
            with st.spinner("Traitement du fichier ZIP en cours..."):
                ep747_data = process_zip_and_extract(zip_uploaded_visa_EP747)
            
            if not ep747_data.empty:
                st.success("Fichier traité avec succès !")
                
                # Display extracted data
                st.write("### Résultats extraits des rapports EP747")
                st.dataframe(ep747_data)
                
                # Allow download of the processed data
                csv = ep747_data.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="Télécharger les résultats extraits",
                    data=csv,
                    file_name="EP747_results.csv",
                    mime="text/csv",
                )
            else:
                st.warning("Aucune donnée extraite du fichier ZIP.")
        except Exception as e:
            st.error("Erreur lors du traitement du fichier ZIP.")
            st.write(e)

    zip_uploaded_visa_EP100_204 = st.file_uploader(":arrow_down: **Chargez le fichier ZIP EP100A**", type=["zip"])
    if zip_uploaded_visa_EP100_204 is not None:
        try:
            # Process the ZIP file and extract data
            with st.spinner("Traitement du fichier ZIP en cours..."):
                ep100_data = process_zip_and_extract_EP100(zip_uploaded_visa_EP100_204)
            
            if not  ep100_data.empty:
                st.success("Fichier traité avec succès !")
                
                # Display extracted data
                st.write("### Rejets extraits des rapports EP100A")
                st.dataframe(ep100_data)
                
                # Allow download of the processed data
                csv = ep100_data.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="Télécharger les rejets",
                    data=csv,
                    file_name="EP100_results.csv",
                    mime="text/csv",
                )
            else:
                st.warning("Aucune donnée extraite du fichier ZIP.")
        except Exception as e:
            st.error("Erreur lors du traitement du fichier ZIP.")
            st.write(e)
            
    uploaded_cybersource_file = st.file_uploader(":arrow_down: **Chargez le fichier Cybersource**", type=["csv"])
    uploaded_pos_file = st.file_uploader(":arrow_down: **Chargez le fichier POS**", type=["csv"])
    uploaded_sai_manuelle_file = st.file_uploader(":arrow_down: **Chargez le fichier du saisie manuelle**", type=["csv"])
    st.divider()
    filtering_date = st.date_input("**Veuillez entrer la date du filtrage pour les transactions rejetées**")
    st.divider()

    uploaded_recycled_file = st.file_uploader(":arrow_down: **Chargez le fichier des transactions recyclées**", type=["xlsx"])
    with st.expander(" **Cliquez ici pour télécharger un modèle du fichier des transactions recyclées**"):
        st.write("**afin de s'assurer que le fichier sera bien traité** ")
        file_path = "assets/template_rejets_recyclées.xlsx"  # Update this with your actual file path
        # Read the file content
        with open(file_path, 'rb') as file:
            file_content = file.read()
        st.download_button(
            label="Télécharger le Modèle ",
            data=file_content,
            file_name=os.path.basename(file_path),
            mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            type="primary"
        )

    st.divider()
    global default_columns_cybersource, default_columns_saisie_manuelle, default_columns_pos, total_transactions

    default_columns_cybersource = ['NBRE_TRANSACTION', 'MONTANT_TOTAL', 'CUR', 'FILIALE', 'RESEAU', 'TYPE_TRANSACTION']
    default_columns_saisie_manuelle = ['NBRE_TRANSACTION', 'MONTANT_TOTAL', 'CUR', 'FILIALE', 'RESEAU']
    default_columns_pos = ['FILIALE', 'RESEAU', 'TYPE_TRANSACTION', 'DATE_TRAI', 'CUR', 'NBRE_TRANSACTION', 'MONTANT_TOTAL']

   

    total_transactions = {'Cybersource': 0, 'POS': 0, 'Saisie Manuelle': 0}

    try:
        df_cybersource, df_sai_manuelle, df_pos = upload_all_sources()
    except Exception as e:
        st.error(f"Erreur lors du chargement des sources")
        st.write(e)


    try:
        filtered_cybersource_df, filtered_saisie_manuelle_df, filtered_pos_df = filter_sources(df_cybersource, df_sai_manuelle, df_pos)
    except Exception as e:
        st.error(f"Impossible de traiter les fichiers")
        st.write(e)

    pie_chart()
    try:
        if zip_uploaded_visa_EP747:
            if not ep747_data.empty:
                ep747_data = process_zip_and_extract(zip_uploaded_visa_EP747)
                ep100_data = process_zip_and_extract_EP100(zip_uploaded_visa_EP100_204)
                handle_recon(filtered_cybersource_df, filtered_saisie_manuelle_df, filtered_pos_df, zip_uploaded_visa_EP747, zip_uploaded_visa_EP100_204)
        else:
            st.warning("Charger les rapports EP747 / EP 100 et les sources pour continuer")
    except Exception as e:
        st.warning(f"Charger les sources")
        st.write(e)
   
    

if __name__ == "__main__":
    main()

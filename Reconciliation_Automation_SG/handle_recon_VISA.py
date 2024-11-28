# librairies
import pandas as pd
from parser_TT140_MasterCard import *
import os
import re
import tempfile
from openpyxl.styles import PatternFill
import openpyxl
from openpyxl.styles import Font, PatternFill, Alignment, numbers
from openpyxl.worksheet.table import Table, TableStyleInfo
import streamlit as st
import io
import numpy as np


# Define the function to read CSV files with delimiters
def read_csv_with_delimiters(file_path, default_columns=None, default_delimiter=','):
    """
    Lire un fichier CSV avec delimiteurs , ou ; ou espace
    
    Les paramètres:
        Chemin de fichier (str): Le chemin du fichier CSV.
        Le delimiteur par défaut (str): Le delimiteur par défaut à utiliser si le fichier est vide.
        
    Retourne:
        pd.DataFrame: Le dataframe avec le contenu CSV.
    """
    try:
        with open(file_path, 'r') as f:
            first_line = f.readline()

        # Détecter le délimiteur
        if ';' in first_line:
            delimiter = ';'
        elif ',' in first_line:
            delimiter = ','
        elif ' ' in first_line:
            delimiter = '\s+'  # regex for one or more spaces
        else:
            delimiter = default_delimiter
    except FileNotFoundError:
        delimiter = default_delimiter

    try:
        df = pd.read_csv(file_path, sep=delimiter, engine='python')
    except pd.errors.EmptyDataError:
        df = pd.DataFrame(columns=default_columns)
    
    return df


# Function to save uploaded file to a temporary location
def save_uploaded_file(uploaded_file):
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        temp_file.write(uploaded_file.read())
        return temp_file.name
    

default_columns_cybersource = ['NBRE_TRANSACTION', 'MONTANT_TOTAL', 'CUR', 'FILIALE', 'RESEAU', 'TYPE_TRANSACTION']
default_columns_saisie_manuelle = ['NBRE_TRANSACTION', 'MONTANT_TOTAL', 'CUR', 'FILIALE', 'RESEAU']
default_columns_pos = ['FILIALE', 'RESEAU', 'TYPE_TRANSACTION', 'DATE_TRAI', 'CUR', 'NBRE_TRANSACTION', 'MONTANT_TOTAL']



def reading_cybersource(cybersource_file):
    # Lire le fichier cybersource
    if os.path.exists(cybersource_file):
        df_cybersource = read_csv_with_delimiters(cybersource_file, default_columns_cybersource)

        # nettoyer les espaces à partir des noms de colonnes
        df_cybersource.columns = df_cybersource.columns.str.strip()
        
        # affecter aux transactions Cybersource le type ACHAT pour merger après avec les transactions POS de type ACHAT
        df_cybersource['TYPE_TRANSACTION'] = 'ACHAT'
        df_cybersource = df_cybersource.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
        #df_cybersource['RESEAU'] = df_cybersource['RESEAU'].astype(str)
        #df_cybersource['FILIALE'] = df_cybersource['FILIALE'].astype(str)
        #df_cybersource['CUR'] = df_cybersource['CUR'].astype(str)
        return df_cybersource
    else:
        df_cybersource = pd.DataFrame(columns=default_columns_cybersource)
        print("The Cybersource file does not exist at the specified path.")


# Read Saisie Manuelle file
def reading_saisie_manuelle(saisie_manuelle_file):
    # Lire le fichier saisie manuelle
    if os.path.exists(saisie_manuelle_file):
        df_sai_manuelle = read_csv_with_delimiters(saisie_manuelle_file, default_columns_saisie_manuelle)

        # nettoyer les espaces à partir des noms de colonnes
        df_sai_manuelle.columns = df_sai_manuelle.columns.str.strip()

        # affecter aux transactions Cybersource le type ACHAT pour merger après avec les transactions POS de type ACHAT
        df_sai_manuelle['TYPE_TRANSACTION'] = 'ACHAT'
        df_sai_manuelle = df_sai_manuelle.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
        return df_sai_manuelle
    else:
        df_sai_manuelle = pd.DataFrame(columns=default_columns_saisie_manuelle)
        print("The Saisie Manuelle file does not exist at the specified path.")


# Read POS file
def reading_pos(pos_file):
    # Lire le fichier POS
    if os.path.exists(pos_file):
        df_pos = read_csv_with_delimiters(pos_file, default_columns_pos)

        # nettoyer les espaces à partir des noms de colonnes
        df_pos.columns = df_pos.columns.str.strip()
        df_pos = df_pos.apply(lambda x: x.str.strip() if x.dtype == "object" else x)

        # renommer la colonne BANQUE en colonne FILIALE pour normaliser le dataframe avec les autres dataframes des autres sources
        df_pos.rename(columns={'BANQUE': 'FILIALE'}, inplace=True)
        return df_pos
    else:
        df_pos = pd.DataFrame(columns=default_columns_pos)
        print("The POS file does not exist at the specified path.")


def filtering_sources(df_cybersource, df_sai_manuelle, df_pos):
    # Filter chaque source pour recevoir uniquement les transactions de type VISA INTERNATIONAL
    filtered_cybersource_df = df_cybersource[df_cybersource['RESEAU'] == 'VISA INTERNATIONAL']
    filtered_saisie_manuelle_df = df_sai_manuelle[df_sai_manuelle['RESEAU'] == 'VISA INTERNATIONAL']
    filtered_pos_df = df_pos[(df_pos['RESEAU'] == 'VISA INTERNATIONAL') & 
                            (~df_pos['TYPE_TRANSACTION'].str.endswith('_MDS'))]
    return filtered_cybersource_df, filtered_saisie_manuelle_df, filtered_pos_df


def validate_file_name_and_date(file_name, source, date_to_validate=None):
    """
    Validate the file name based on the source, the required pattern,
    and optionally validate the date extracted from the file name.

    Parameters:
        file_name (str): The name of the uploaded file.
        source (str): The source type (CYBERSOURCE, POS, or SAIS_MANU).
        date_to_validate (str): The date to validate against the one in the file name. (Optional)

    Returns:
        bool: True if the file name is valid, False otherwise.

    Raises:
        ValueError: If the file name is invalid or if the date does not match the expected pattern.
    """
    pattern = f"^TRANSACTION_{source}_TRAITE_SG_\\d{{2}}-\\d{{2}}-\\d{{2}}_\\d{{6}}\\.CSV$"
    if not re.match(pattern, file_name):
        raise ValueError(f"Invalid file name: {file_name}. Expected pattern: TRANSACTION_{source}_TRAITE_SG_YY-MM-DD_HHMMSS.CSV")

    # Extract the date from the file name
    date_match = re.search(r"\d{2}-\d{2}-\d{2}", file_name)
    if date_match:
        extracted_date = date_match.group(0)
        if date_to_validate:
            if extracted_date != date_to_validate:
                raise ValueError(f"Date extracted from the file name ({extracted_date}) does not match the provided date ({date_to_validate}).")
    else:
        raise ValueError("Date not found in the file name.")

    return True


# Fonction qui convertit un fichier excel en csv en dataframe, pour le cas du fichier des rejets recyclées
def excel_to_csv_to_df(excel_file_path, sheet_name=0):
    """
    Convertit un fichier xlsx en csv en dataframe.

    Parameters:
        excel_file_path (str): Chemin pour le fichier excel.
        nom de la feuille excel (str or int): Nom ou index de la feuille excel (Par défaut la première feuille).

    Returns:
        pd.DataFrame: Dataframe contenant les donnèes de csv.
    """
    try:
        # Lire le fichier excel
        df = pd.read_excel(excel_file_path, sheet_name=sheet_name, header=0)

        # Définir le chemin du fichier csv
        csv_file_path = excel_file_path.replace('.xlsx', '.csv')

        # Sauvegarder le dataframe en csv
        df.to_csv(csv_file_path, index=False)

        # Lire le csv en dataframe
        df_csv = read_csv_with_delimiters(csv_file_path)
        return df_csv

    except PermissionError as p_error:
        print(f"Permission error: {p_error}. Please check your file permissions.")
    except Exception as e:
        print(f"An error occurred: {e}")
    return None


def standardize_date_format(date_column, desired_format='%Y-%m-%d'):
    """
    Standardize the date format in a given column.

    Parameters:
        date_column (pd.Series): The column containing dates to standardize.
        desired_format (str): The desired date format (default is '%Y-%m-%d').

    Returns:
        pd.Series: The column with dates in the standardized format.
    """
    # Convert all dates to datetime objects
    date_column = pd.to_datetime(date_column , dayfirst=False  , yearfirst=True)
    # Format all datetime objects to the desired format
    date_column = date_column.dt.strftime(desired_format )

    return date_column


# Fonction pour extraire le nombres de transactions des rapports SETTLEMENT de VISA
def extract_transaction_data(file_path, content):
    filename = os.path.basename(file_path)
    bin_number = filename.split('_')[0]  # Extraire BIN des fichiers


     # pour ouvrir en cas de présence de dossier
     # with open(file_path, 'r') as file:
        #content = file.read()
        
    cleaned_content = re.sub(
    r'(ISSUER TRANSACTIONS.*?(?:\*\*\*  END OF VSS-120 REPORT|FINAL SETTLEMENT NET AMOUNT|TOTAL MERCHANDISE CREDIT)|'
    r'REPORT ID:\s+VSS-130.*?(?:\*\*\*  END OF VSS-130 REPORT)|'
    r'REPORT ID:\s+VSS-140.*?(?:\*\*\*  END OF VSS-140 REPORT|TOTAL MERCHANDISE CREDIT)|'
    r'REPORT ID:\s+VSS-115.*?(?:\*\*\*  END OF VSS-115 REPORT)|'
    r'ACQUIRER TRANSACTIONS\s+PURCHASE\s+DISPUTE FIN.*?TOTAL PURCHASE\s+([\d,]+)|'
    r'ACQUIRER TRANSACTIONS\s+PURCHASE\s+ DISPUTE RESP FIN.*?TOTAL PURCHASE\s+([\d,]+))', 
    '', 
    content, 
    flags=re.DOTALL
)

    # Chercher la section d'extraction commençant de "ACQUIRER TRANSACTIONS" jusqu'à "END OF VSS-120 REPORT"
    acquirer_section = re.search(r'ACQUIRER TRANSACTIONS(.*)(?=\*\*\*  END OF VSS-120 REPORT)', cleaned_content, re.DOTALL)

    if acquirer_section:
        acquirer_content = acquirer_section.group(1).strip()  # Extraire et nettoyer le contenu du bloc

        # Extraire le nombre de transactions TOTAL ORIGINAL SALE
        total_original_sale = re.search(r'TOTAL PURCHASE\s+([\d,]+)', acquirer_content)
        if total_original_sale:
            transaction_count = total_original_sale.group(1).replace(',', '')
        else:
            transaction_count = None

        # Extraire le nombre de transactions MANUAL CASH (CASH ADVANCE)
        manual_cash_strict = re.search(r'^\s*MANUAL CASH\s+(\d+)', acquirer_content, re.MULTILINE)

        total_purchase_mga = re.search(r'CLEARING CURRENCY:\s+MGA.*?TOTAL PURCHASE\s+([\d,]+)', acquirer_content, re.DOTALL)

        # Extract le nombre de transactions TOTAL PURCHASE (ACHAT) pour MGA CLEARING CURRENCY EUR
        nbr_transaction_eur = re.search(r'CLEARING CURRENCY:\s+EUR.*?ORIGINAL SALE\s+(\d+)', acquirer_content, re.DOTALL)
        
        if nbr_transaction_eur:
            transaction_count_EUR = nbr_transaction_eur.group(1).replace(',', '')
        else:
            transaction_count_EUR = None

        if total_purchase_mga:
            transaction_count_MGA = total_purchase_mga.group(1).replace(',', '')
        else:
            transaction_count_MGA = None

        # Extraire le nombre de transactions MERCHANDISE CREDIT (CREDIT VOUCHER)
        merchandise_credit_section = re.search(r'TOTAL MERCHANDISE CREDIT\s+(\d+)', acquirer_content)

        # Extraire le nombre de rejets
        total_rejects = re.search(r'ORIGINAL SALE\s+RETURN\s+[A-Z0-9]+\s+(\d+)', acquirer_content)
        if total_rejects:
            transaction_rejects_count = total_rejects.group(1) #Prendre les elements de la première position comme élement d'extraction
        else:
            transaction_rejects_count = "None"
        
        xof_rejects = re.search(r'ORIGINAL SALE\s+RETURN\s+[A-Z0-9]+\s+(\d+)', acquirer_content)
        if xof_rejects:
            xof_rejects_count = xof_rejects.group(1) #Prendre les elements de la deuxième position comme élement d'extraction (XOFS)
        else:
            xof_rejects_count = "None"

        # Créer un dictionnaire pour stocker les résultats
        transaction_data = {
            "file": file_path,
            "Numero de BIN": bin_number,
            "Nbr de transactions": transaction_count,
            "CASH ADVANCE": None,
            "Nbr de transactions (CLEARING CURR EUR POUR MDG)": None,
            "CREDIT VOUCHER": None,
            "total de rejets" : transaction_rejects_count,
            "total de xof rejetées" : xof_rejects_count
        }

        # Ajouter le nombre de transactions MANUAL CASH (CASH ADVANCE)
        if manual_cash_strict:
            manual_cash_value = manual_cash_strict.group(1)
            if manual_cash_value != "0":  # Ignorer si MANUAL CASH est 0
                transaction_data["CASH ADVANCE"] = manual_cash_value

        # Ajouter le nombre de transactions pour TOTAL PURCHASE (CLEARING CURRENCY EUR) seulement si le BIN est 489316
        if bin_number == "489316":
            transaction_data["Nbr de transactions (CLEARING CURR EUR POUR MDG)"] = transaction_count_EUR
            transaction_data["Nbr de transactions"] = transaction_count_MGA

        # Ajouter le nombre de transactions pour TOTAL MERCHANDISE CREDIT (CREDIT VOUCHER) si ça existe
        if merchandise_credit_section:
            merchandise_credit_count = merchandise_credit_section.group(1)
            transaction_data["CREDIT VOUCHER"] = merchandise_credit_count

        return transaction_data
    else:
        return {"file": file_path, "error": "ACQUIRER TRANSACTIONS section not found."}
    
def merging_sources_without_recycled(filtered_cybersource_df, filtered_saisie_manuelle_df, filtered_pos_df, file_path, content):
    # Fusionner avec le dataframe POS avec le dataframe SAISIE MANUELLE en se basant sur les colonnes précisées
    result_df = pd.merge(
        filtered_pos_df,
        filtered_saisie_manuelle_df,
        on=['FILIALE', 'RESEAU', 'CUR', 'TYPE_TRANSACTION'],
        suffixes=('_pos', '_saisie'),
        how='outer'  # Utiliser fusionnage de type outer pour garder toutes les lignes
    )

    # Fusionner avec les dataframes fusionnés (POS et SAISIE MANUELLE) avec le dataframe CYBERSOURCE filtré
    result_df = pd.merge(
        result_df,
        filtered_cybersource_df,
        on=['FILIALE', 'RESEAU', 'CUR', 'TYPE_TRANSACTION'],
        suffixes=('_merged', '_cybersource'),
        how='outer'  # Utiliser fusionnage de type outer pour garder toutes les lignes
    )

    # Remplacer les valeurs nulles avec 0, et sommer les nombres de transactions
    result_df['NBRE_TRANSACTION'] = (result_df['NBRE_TRANSACTION_pos'].fillna(0) +
                                    result_df['NBRE_TRANSACTION_saisie'].fillna(0) +
                                    result_df['NBRE_TRANSACTION'].fillna(0))

    # Convertir le 'NBRE_TRANSACTION' en entier
    result_df['NBRE_TRANSACTION'] = result_df['NBRE_TRANSACTION'].astype(int)

    # Sommer les montants de toutes les sources
    result_df['MONTANT_TOTAL'] = (result_df['MONTANT_TOTAL_pos'].fillna(0) +
                                    result_df['MONTANT_TOTAL_saisie'].fillna(0) +
                                    result_df['MONTANT_TOTAL'].fillna(0))

    # Supprimer les colonnes inecessaires utilisées en fusionnage
    result_df.drop(['NBRE_TRANSACTION_pos', 'NBRE_TRANSACTION_saisie', 'MONTANT_TOTAL_pos', 'MONTANT_TOTAL_saisie'], axis=1, inplace=True)

    # Supprimer les lignes dupliquées
    result_df.drop_duplicates(subset=['FILIALE', 'RESEAU', 'CUR', 'TYPE_TRANSACTION', 'DATE_TRAI'], inplace=True)

    result_df = result_df.copy()

    # Définir les colonnes du dataframe du résultat de réconciliation
    new_columns = [
        'FILIALE', 'Réseau', 'Type', 'Date', 'Devise', 'NbreTotaleDeTransactions',
        'Montant Total de Transactions', 'Rapprochement', 'Nbre Total de Rejets',
        'Montant de Rejets', 'Nbre de Transactions (Couverture)', 
        'Montant de Transactions (Couverture)'
    ]

    # Mapper entre les noms de colonnes (merged_df et le dataframe résultat)
    column_mapping = {
        'FILIALE': 'FILIALE',
        'RESEAU': 'Réseau',
        'TYPE_TRANSACTION': 'Type',
        'DATE_TRAI': 'Date',
        'CUR': 'Devise',
        'NBRE_TRANSACTION': 'NbreTotaleDeTransactions',
        'MONTANT_TOTAL': 'Montant Total de Transactions'
    }

    # Renommer les colonnes
    result_df.rename(columns=column_mapping, inplace=True)

    # Remplacer les colonnes vides avec colonnes par défaut
    for column in set(new_columns) - set(result_df.columns):
        result_df[column] = ''

    # Réordonner les colonnes
    result_df = result_df[new_columns]


    transaction_list = []

    transaction_data = extract_transaction_data(file_path, content)

    transaction_list.append(transaction_data)

    # Initialiser la mapping des FILIALES à BINs
    visa_banks_bin = {
        'SG - COTE D IVOIRE': '463741',
        'SG - BENIN': '404927',
        'SG - BURKINA FASO': '410282',
        'SG - CAMEROUN': '439972',
        'SG - GUINEE EQUATORIALE': '410655',
        'SG - MADAGASCAR': '489316',
        'SG - SENEGAL': '441358',
        'SG - TCHAD': '458250',
        'SG - CONGO': '464012',
        'SG - GUINEE CONAKRY': '486059'
    }

    #print(visa_banks_bin)


    data = result_df.copy()


    # Créer un dictionnaire à partir de la liste des transactions extraites
    generated_transactions = {trans['Numero de BIN']: trans for trans in transaction_list if 'error' not in trans}

    # Pre-calculer (faire la somme) les lignes ayant comme TYPES :  ACHAT et CREDIT VOUCHER groupées par filiale
    grouped_filiale = data.groupby('FILIALE').agg(
        achat_transactions=pd.NamedAgg(column='NbreTotaleDeTransactions', aggfunc=lambda x: x[data['Type'] == 'ACHAT'].sum()),
        cv_transactions=pd.NamedAgg(column='NbreTotaleDeTransactions', aggfunc=lambda x: x[data['Type'].isna() | (data['Type'] == '')].sum())
    ).reset_index()

    def safe_float(value):
        """Convert a value to float, handling 'None' and NoneType."""
        if value in (None, 'None'):
            return 0
        try:
            return float(value)
        except ValueError:
            return 0  # Default 0 is la conversion est failed

    def compare_transactions(row, transaction_data, grouped_data):
        filiale = row['FILIALE']
        transaction_type = row['Type']
        
        # Trouver le BIN correspondant par FILIALE
        bin_number = visa_banks_bin.get(filiale)
        if not bin_number:
            return 'Aucun BIN trouvé'
        
        # Extraire les donnèes de transactions par BIN
        trans_data = transaction_data.get(bin_number)
        if not trans_data:
            return 'Aucune donnèe pour BIN'
        
        # Extraire les informations de transactions du dictionnaire
        transaction_count = safe_float(trans_data.get('Nbr de transactions'))
        total_count = safe_float(trans_data.get('Nbr de transactions (CLEARING CURR EUR POUR MDG)'))
        manual_cash = safe_float(trans_data.get('CASH ADVANCE'))
        merchandise_credit = safe_float(trans_data.get('CREDIT VOUCHER'))
        rejects = safe_float(trans_data.get('total de rejets'))
        #xof_rejects = safe_float(trans_data.get('total de xof rejetées'))

        # Get sommes pré-calculées à travers les donnèes groupées
        filiale_data = grouped_data[grouped_data['FILIALE'] == filiale]
        if filiale_data.empty:
            return 'Filiale inconnue'

        achat_transactions = safe_float(filiale_data['achat_transactions'].values[0])
        cv_transactions = safe_float(filiale_data['cv_transactions'].values[0])

        #print(f"FILIALE: {filiale}")
        #print(f"ACHAT transactions (FILIALE): {achat_transactions}")
        #print(f"CREDIT VOUCHER transactions (FILIALE): {cv_transactions}")
        #print(f"Transaction count from settlement report: {transaction_count}")
        #print(f"Rejects from settlement report: {rejects}")
        #print(f"MERCHANDISE CREDIT from settlement report: {merchandise_credit}")

        #Le cas ou on a pas de rejets
        if rejects == 0:
            if transaction_type == 'ACHAT':
                filiale = row['FILIALE']

                # Checker si on a CREDIT VOUCHER (vide/NaN 'Type') pour la meme FILIALE
                cv_rows = data[(data['FILIALE'] == filiale) & (pd.isna(data['Type']) | (data['Type'] == ''))]

                # If no CREDIT VOUCHER exists for this FILIALE, apply additional checks
                if cv_rows.empty:
                
                    # Apply checks on transaction counts
                    if row['NbreTotaleDeTransactions'] == transaction_count + merchandise_credit or row['NbreTotaleDeTransactions'] == total_count:
                    
                        return 'ok'
                    else:
                    
                        return 'not ok (avec tr.(s) rejetée(s) a extraire)'

                else:
    
                    if row['NbreTotaleDeTransactions'] == transaction_count or row['NbreTotaleDeTransactions'] == total_count:
                    
                        return 'ok'
                    else:
                    
                        return 'not ok (avec tr.(s) rejetée(s) a extraire)'
                
                
            if transaction_type == 'ACHAT' and transaction_type == '' or  pd.isna(transaction_type):

                # grouper les lignes par filiale
                filiale = row['FILIALE']

                # lignes avec meme filiale pour ACHAT et CREDIT VOUCHER
                relevant_rows = data[(data['FILIALE'] == filiale) &
                                    ((data['Type'] == 'ACHAT') | pd.isna(data['Type']) | (data['Type'] == ''))]
                

                # Sommer 'NbreTotaleDeTransactions' pour les lignes
                total_transactions = relevant_rows['NbreTotaleDeTransactions'].sum()

                # formule de comparaison
                formule = total_transactions == transaction_count + merchandise_credit
                if formule:
                    return 'ok'
                else:
                    #print(total_transactions)
                    #print(transaction_count + merchandise_credit)
                    return 'not ok (avec tr.(s) rejetée(s) a extraire)'
                
            elif transaction_type == 'CASH ADVANCE':
                if row['NbreTotaleDeTransactions'] == manual_cash:
                    return 'ok'
                else:
                    return 'not ok (avec tr.(s) rejetée(s) a extraire)'
            
        # Le cas ou on a des rejets
        elif rejects != 0:
            try:
                formule_calcul = (achat_transactions + cv_transactions) == (transaction_count - rejects + merchandise_credit)
                print(f"Formule de calcul appliquée ?: {formule_calcul}\n")

                if formule_calcul  and transaction_type == 'ACHAT':
                    return 'not ok'
                elif formule_calcul and (transaction_type == '' or  pd.isna(transaction_type)):
                    return 'ok'
                elif not formule_calcul and (transaction_type == '' or  pd.isna(transaction_type)): 
                    return 'ok'
                elif not formule_calcul and transaction_type == 'ACHAT':
                    return 'not ok (avec tr.(s) rejetée(s) a extraire)'

            except ValueError:
                return 'nok'  # Gérer les erreurs de conversion

        else:
            return 'Type inconnu'
        
        data['Rapprochement'] = data.apply(compare_transactions, args=(generated_transactions, grouped_filiale), axis=1)
        return data

    #return result_df




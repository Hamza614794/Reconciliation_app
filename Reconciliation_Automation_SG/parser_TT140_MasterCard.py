import pandas as pd
import json
import re
import streamlit as st
from datetime import datetime , timedelta
import locale


currencies_settings = 'Reconciliation_Automation_SG/currency_codes.json'
countries_settings = 'Reconciliation_Automation_SG/countries_acronyms.json'


def extract_date_from_mastercard_file(file_contents):
    """
    Extract the date from the MasterCard file contents.
    
    Parameters:
        file_contents (str): Contents of the uploaded file.
        
    Returns:
        str: Extracted date in the format 'YY-MM-DD'.
        
    Raises:
        ValueError: If the date cannot be extracted from the file contents.
    """
    for line in file_contents.splitlines():
        if line.startswith("1IP727010-AA"):
            # Adjust the pattern to match the date format in the target line
            date_pattern = r"RUN DATE: (\d{2}/\d{2}/\d{2})"
            date_match = re.search(date_pattern, line)
            if date_match:
                # Extract and parse the date
                run_date = datetime.strptime(date_match.group(1), "%m/%d/%y").strftime("%y-%m-%d")
                # Convert the string to a datetime object
                date_object = datetime.strptime(run_date, "%y-%m-%d")

                # Add one day to the date
                day_after = date_object + timedelta(days=1)

                # Convert the datetime object back to a string in 'YY-MM-DD' format
                day_after = day_after.strftime("%y-%m-%d")
                return run_date , day_after
            else:
                raise ValueError(f"Could not extract date from the line: {line}")
    raise ValueError("Could not find the target line in the file to extract the date.")

def extract_rejections(mastercard_file, currencies_settings, countries_settings):
    if mastercard_file is None or len(mastercard_file) == 0:
        print("Rapport Mastercard vide ou inexistant.")
        return None

    df = pd.read_csv(mastercard_file, delimiter='\t', header=None)
    text = ' '.join(df[0].astype(str))

    # Define the regular expression patterns to find all "SOURCE AMOUNT:" and "SOURCE CURRENCY:" and their following values
    pattern_source_amount = re.compile(r"SOURCE AMOUNT:\s+(\*?\S+)")
    pattern_source_currency = re.compile(r"SOURCE CURRENCY:\s+(\*?\S+)")

    # Find all matches of the patterns in the text
    matches_source_amount = pattern_source_amount.findall(text)


    formatted_amounts = [amount.replace(',', '') for amount in matches_source_amount]

    
    # Convert each formatted amount string to float
    matches_source_amount_float = [float(amount_str) for amount_str in formatted_amounts]
    #print(matches_source_amount_float)

    matches_source_currency = pattern_source_currency.findall(text)

    # Load the JSON file containing currency codes and names
    with open(currencies_settings, 'r') as f:
        currencies_data = json.load(f)

    # Create a dictionary mapping currency codes to their names
    currency_code_to_name = currencies_data

    # Map each found currency code to its name
    mapped_currencies = {currency.strip('*'): currency_code_to_name.get(currency.strip('*'), 'Not found') for currency in matches_source_currency}

    # Create a DataFrame to store the extracted values
    data = {
        "Montant": matches_source_amount_float,
        "Devise": [mapped_currencies.get(currency.strip('*'), 'Not found') for currency in matches_source_currency]
    }
    df_extracted_values = pd.DataFrame(data)

    # Load the JSON file containing country acronyms and names
    with open(countries_settings, 'r') as f:
        countries_data = json.load(f)

    # Initialize a list to store country names
    country_names = []

    # Find all occurrences of "D0043"
    occurrences_d0043 = [i for i, token in enumerate(text.split()) if token in ["D0043", "*D0043"]]

    # Iterate over the occurrences and find the element after the sixth occurrence of "D0043"
    for index_sixth_d0043 in occurrences_d0043[5:]:
        # Check if there is a token after the element following the sixth "D0043"
        if index_sixth_d0043 + 2 < len(text.split()):
            element_after_sixth_d0043 = text.split()[index_sixth_d0043 + 2]

            # Get the country name for the acronym if it exists
            country_name = countries_data.get(element_after_sixth_d0043.strip('*'))

            # Add the country name to the list if it exists
            if country_name:
                country_names.append(country_name)

    # Create a DataFrame for PAYS transaction with only the country names
    # Prefix the country names with "SG - "
    df_pays = pd.DataFrame({'FILIALE': ["SG - " + country for country in country_names]})

    # Define a regular expression pattern to find the transaction date from "D0012 S01 240522" format
    pattern_transaction_date = re.compile(r"D0012\s+S01\s+(\*?\d{6})")

    # Find all matches of the pattern in the text
    matches_transaction_date = pattern_transaction_date.findall(text)

    # Convert the extracted dates to the proper format (assuming the date format is YYMMDD)
    formatted_dates = [f"20{date[0:2]}-{date[2:4]}-{date[4:6]}" for date in matches_transaction_date]

    # Create a DataFrame to store the extracted transaction date
    df_transaction_date = pd.DataFrame({'Date Transaction': formatted_dates})

    # Define a regular expression pattern to find all groups of "D0031 Sxx" and their values
    pattern_arn = re.compile(r"(D0031 S\d+\s+\*?\d+)")

    # Find all matches of the pattern in the text
    matches_arn = pattern_arn.findall(text)

    # Initialize variables
    arns = []
    current_arn_values = []

    # Iterate over the matches to group values for each ARN
    for match in matches_arn:
        # Extract the value from the match
        value = match.split()[-1].strip('*')
        # Check if we are starting a new group (new ARN) based on D0031 S01
        if "D0031 S01" in match:
            # If we have values collected for the current ARN, concatenate them and store the ARN
            if current_arn_values:
                arns.append(''.join(current_arn_values))
                current_arn_values = []
        # Add the value to the current ARN values
        current_arn_values.append(value)

    # Don't forget to add the last collected ARN values
    if current_arn_values:
        arns.append(''.join(current_arn_values))

    # Create a DataFrame for ARNs de transactions
    df_arns = pd.DataFrame({'ARN': arns})

    # Define a regular expression pattern to find all "D0038" and its following value
    pattern_authorization = re.compile(r"D0038\s+(\*?\S+)")

    # Find all matches of the pattern in the text
    matches_authorization = pattern_authorization.findall(text)

    # Create a DataFrame for Autorisation(s) found
    df_authorization = pd.DataFrame({'Autorisation': [auth.strip('*') for auth in matches_authorization]})

    # Regular expression pattern to match error code and description
    pattern = r"(\d{4})\s+([A-Z0-9\s\/]+[A-Z0-9\.])"

    # Identifier for the section containing error codes and descriptions
    identifier = "CODE    DESCRIPTION                                                                                MESSAGE #   ELEMENT ID"

    # Identifier for the end of a section
    end_identifier = "MESSAGE DETAILS"

    # Read the file
    with open(mastercard_file, 'r') as file:
        content = file.read()

    # Initialize list to store descriptions
    descriptions = []

    # Find the first section containing error codes and descriptions
    start = content.find(identifier)
    if start == -1:
        #print("Section containing error codes and descriptions not found in the file.")
        return None

    while start != -1:
        # Find the end of the section
        end = content.find(end_identifier, start)
        if end == -1:
            end = len(content)

        # Extract the section content
        section_content = content[start:end]

        # Find all matches in the section content
        section_matches = re.findall(pattern, section_content)

        # Concatenate descriptions within the section
        section_descriptions = " ".join(desc for code, desc in section_matches)

        # Append to descriptions list
        descriptions.append(section_descriptions)

        # Find the next section containing error codes and descriptions
        start = content.find(identifier, end)

    # Create a DataFrame from the descriptions
    df_motif = pd.DataFrame(descriptions, columns=["Motif"])

    # Remove leading and trailing whitespace from each description and replace multiple spaces with a single space
    df_motif['Motif'] = df_motif['Motif'].str.strip().replace(r'\s+', ' ', regex=True)
    # Create the DataFrame
    df_reseau = pd.DataFrame({"RESEAU": ["MASTERCARD INTERNATIONAL"] * len(df_extracted_values)})
    # Concatenate all the DataFrames
    df_rejected = pd.concat([df_pays,df_reseau,df_arns,df_authorization,df_transaction_date,df_extracted_values, df_motif], axis=1)

    return df_rejected

def calculate_rejected_summary(mastercard_file_path):
    df_rejected = extract_rejections(mastercard_file_path, currencies_settings, countries_settings)
    if df_rejected is None or df_rejected.empty:
        #print("Empty DataFrame or None received from extract_rejections. Cannot proceed.")
        return None
    df_rejected = extract_rejections(mastercard_file_path, currencies_settings, countries_settings)
    df_rejected['Montant'] = df_rejected['Montant'].replace(r'[/$,]', '', regex=True).astype(float)


    # Group by FILIALE and calculate the number of transactions and sum of amounts
    summary = df_rejected.groupby('FILIALE').agg(
        NbrTotalDeRejets=('Montant', 'size'),
        MontantDeRejets=('Montant', 'sum')
    ).reset_index()

    # Rename columns to have spaces instead of underscores
    summary.columns = ['FILIALE', 'Nbre Total de Rejets', 'Montant de Rejets']

    # Convert 'Nbre Total de Rejets' to integer
    summary['Nbre Total de Rejets'] = summary['Nbre Total de Rejets'].astype(int)

    # Print the summary DataFrame
    #print("Rejected Summary DataFrame:")
    #st.dataframe(summary)

    return summary


def extract_total_nbr_transactions_mastercard(file_path):
    if file_path is None or len(file_path) == 0:
        #print("Empty file path received. Cannot proceed.")
        return None
    df = pd.read_csv(file_path, delimiter='\t', header=None , engine='python')

    pres_total_rows = df[df.apply(lambda row: row.astype(str).str.contains('FIRST PRES.  TOTAL').any(), axis=1)]

    def sum_values(row):
        numbers = [int(s) for s in row.split() if s.isdigit()]
        return sum(numbers)

    sum_of_values = pres_total_rows[0].apply(sum_values).sum()
    #print("Number of transactions in the MasterCard file:", sum_of_values)

    return sum_of_values


def parse_t140_MC(mastercard_file_path):
    if mastercard_file_path is None or len(mastercard_file_path) == 0:
        #print("Empty file path received. Cannot proceed.")
        return None, None, None

    nbr_total_MC = extract_total_nbr_transactions_mastercard(mastercard_file_path)
    summary_df = calculate_rejected_summary(mastercard_file_path)
    rejeted_df = extract_rejections(mastercard_file_path, currencies_settings, countries_settings)
    return nbr_total_MC, summary_df, rejeted_df

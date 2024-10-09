from pymongo import MongoClient
import toml
import pandas as pd
from datetime import datetime, timedelta
import streamlit as st

# Load MongoDB connection parameters from secrets.toml
secrets = toml.load(".streamlit/secrets.toml")
mongo_url = secrets["mongo"]["url"]
mongo_database = "Results"


# Connect to MongoDB
client = MongoClient(mongo_url)
db = client[mongo_database]
collection_recon_results = db['Reconciliation_results']
collection_results_summary = db['Reconciliation_summary']
collection_results_rejects = db['Reconciliation_rejects']
collection_results_recycles = db['Reconciliation_recycles']

def insert_reconciliated_data(df_result):
    """
    Insert reconciliated DataFrame data into MongoDB.

    Parameters:
    df (pd.DataFrame): DataFrame containing the reconciliated data.
    """
    data_to_insert = df_result.to_dict("records")
    collection_recon_results.insert_many(data_to_insert)
    return 'Reconciliated data archived in database'


def insert_rejection_summary(df_summary):
    """
    Insert summary DataFrame data into MongoDB.

    Parameters:
    df (pd.DataFrame): DataFrame containing the reconciliated data.
    """
    data_to_insert = df_summary.to_dict("records")
    collection_results_summary.insert_many(data_to_insert)
    return 'Reconciliation summary archived in database'



def insert_rejected_transactions(df_rejects, run_date):
    """
    Insert rejected transactions DataFrame data into MongoDB and add a rejection date.

    Parameters:
    df_rejects (pd.DataFrame): DataFrame containing the rejected transactions data.
    run_date (str): The date to be used as the rejection date.
    """
    try:
        # Add the rejected_date column with the values of run_date
        df_rejects['rejected_date'] = run_date

        # Convert DataFrame to dictionary records for MongoDB insertion
        data_to_insert = df_rejects.to_dict("records")

        # Insert data into MongoDB collection
        collection_results_rejects.insert_many(data_to_insert)
        #print(run_date)

        # Return success message
        return 'Rejected transactions archived in database successfully'
    except Exception as e:
        # Return error message if insertion fails
        return f'Error archiving rejected transactions in database: {str(e)}'


def search_results_by_transaction_date(run_date):
    """
    Search for records in the Reconciliation_results collection by Transaction_Date.

    Parameters:
    transaction_date (str): The Transaction_Date to search for in %Y-%m-%d format.

    Returns:
    pd.DataFrame: DataFrame containing the search results.
    """
    try:
        # Query the MongoDB collection for records with the specified Transaction_Date
        results_recon = collection_recon_results.find({'Date': run_date})

        # Convert the results to a DataFrame
        df_results_recon = pd.DataFrame(list(results_recon))

        return df_results_recon
    except Exception as e:
        # Return an empty DataFrame if the search fails
        st.write(f'Error searching for records by Transaction_Date: {str(e)}')


def search_rejects_by_transaction_date(run_date):
    """
    Search for records in the Reconciliation_results collection by Transaction_Date.

    Parameters:
    transaction_date (str): The Transaction_Date to search for in %Y-%m-%d format.

    Returns:
    pd.DataFrame: DataFrame containing the search results.
    """
    try:
        # Query the MongoDB collection for records with the specified Transaction_Date
        rejects_recon = collection_results_rejects.find({'rejected_date': run_date})

        # Convert the results to a DataFrame
        df_rejects_recon = pd.DataFrame(list(rejects_recon))

        return df_rejects_recon
    except Exception as e:
        # Return an empty DataFrame if the search fails
        st.write(f'Error searching for records by Transaction_Date: {str(e)}')

def search_by_rapprochement(rapprochment):
    """
    Search for records in the Reconciliation_results collection by etat rapporchement.

    Parameters:
    rapprochement

    Returns:
    pd.DataFrame: DataFrame containing the search results.
    """
    try:
        # Query the MongoDB collection for records with the specified Transaction_Date
        results = collection_recon_results.find({'Rapprochement': rapprochment})

        # Convert the results to a DataFrame
        df_results = pd.DataFrame(list(results))

        return df_results
    except Exception as e:
        # Return an empty DataFrame if the search fails
        st.write(f'Error searching for records by Etat de rapprochement: {str(e)}')

def count_rapprochement():
    """
    Count the number of records in the Reconciliation_results collection grouped by etat de rapprochement.

    Returns:
    pd.DataFrame: DataFrame containing the count of records for each rapprochement value.
    """
    try:
        # Use MongoDB aggregation framework to count the number of records grouped by the normalized Rapprochement field
        pipeline = [
            {
                "$project": {
                    "Rapprochement": {"$toUpper": "$Rapprochement"}
                }
            },
            {
                "$group": {
                    "_id": "$Rapprochement",
                    "count": {"$sum": 1}
                }
            }
        ]
        results = collection_recon_results.aggregate(pipeline)

        # Convert the aggregation results to a DataFrame
        df_counts = pd.DataFrame(list(results))
        
        # Rename the columns for clarity
        df_counts.rename(columns={"_id": "Rapprochement", "count": "Count"}, inplace=True)

        return df_counts
    except Exception as e:
        # Print error message if the count fails
        st.write(f'Error counting records by Etat de rapprochement: {str(e)}')
        return pd.DataFrame()
    
def count_rapprochement_by_filiale():
    """
    Count the number of Rapprochement states grouped by Filiale.

    Returns:
    pd.DataFrame: DataFrame containing the count of Rapprochement states for each Filiale.
    """
    try:
        pipeline = [
            {
                "$project": {
                    "FILIALE": 1,
                    "Rapprochement": {"$toUpper": "$Rapprochement"}  # Convert to uppercase to avoid case sensitivity issues
                }
            },
            {
                "$group": {
                    "_id": {
                        "FILIALE": "$FILIALE",
                        "Rapprochement": {"$toUpper": "$Rapprochement"}
                    },
                    "count": {"$sum": 1}
                }
            },
            {
                "$sort": {"_id.FILIALE": 1, "_id.Rapprochement": 1}  # Sort by Filiale and then Rapprochement
            }
        ]
        results = collection_recon_results.aggregate(pipeline)
        df_counts = pd.DataFrame(list(results))

        # Flatten the DataFrame for easier plotting
        df_counts['FILIALE'] = df_counts['_id'].apply(lambda x: x['FILIALE'])
        df_counts['Rapprochement'] = df_counts['_id'].apply(lambda x: x['Rapprochement'])
        df_counts.drop(columns=['_id'], inplace=True)

        return df_counts
    except Exception as e:
        st.write(f'Error counting records by Rapprochement and Filiale: {str(e)}')
        return pd.DataFrame()

def sum_montants_by_filiale(filter_last_30_days=False):
    """
    Calculate the sum of Montant Total de Transactions for each Filiale and concatenate it with Devise.
    Optionally filter transactions to include only those from the last 30 days.

    Args:
    filter_last_30_days (bool): If True, filter transactions to include only those from the last 30 days.

    Returns:
    pd.DataFrame: DataFrame containing the sum of Montant Total de Transactions for each Filiale, concatenated with Devise.
    """
    try:
        pipeline = []

        if filter_last_30_days:
            # Calculate the date 30 days ago from today
            thirty_days_ago = datetime.now() - timedelta(days=30)
            thirty_days_ago_str = thirty_days_ago.strftime('%Y-%m-%d')
            pipeline.append({
                "$match": {
                    "Date": { "$gte": thirty_days_ago_str }
                }
            })

        pipeline.extend([
            {
                "$project": {
                    "FILIALE": 1,
                    "Montant de Transactions (Couverture)": {
                        "$replaceAll": {  # Remove commas from the montant string
                            "input": "$Montant de Transactions (Couverture)",
                            "find": ",",
                            "replacement": ""
                        }
                    },
                    "Devise": 1  # Include Devise field
                }
            },
            {
                "$project": {
                    "FILIALE": 1,
                    "Montant de Transactions (Couverture)": {
                        "$trim": {  # Remove any extra spaces from the montant string
                            "input": "$Montant de Transactions (Couverture)"
                        }
                    },
                    "Devise": 1  # Pass Devise field to the next stage
                }
            },
            {
                "$project": {
                    "FILIALE": 1,
                    "Montant de Transactions (Couverture)": {
                        "$substrCP": [  # Extract numeric part of the montant string
                            "$Montant de Transactions (Couverture)",
                            0,
                            { "$subtract": [ { "$strLenCP": "$Montant de Transactions (Couverture)" }, 3 ] }  # Remove the last three characters (e.g., ".00")
                        ]
                    },
                    "Devise": 1  # Pass Devise field to the next stage
                }
            },
            {
                "$project": {
                    "FILIALE": 1,
                    "Montant de Transactions (Couverture)": {  # Convert string to float
                        "$toDouble": "$Montant de Transactions (Couverture)"
                    },
                    "Devise": 1  # Pass Devise field to the next stage
                }
            },
            {
                "$group": {
                    "_id": "$FILIALE",
                    "total_montant": { "$sum": "$Montant de Transactions (Couverture)" },  # Sum up the montants
                    "Devise": { "$first": "$Devise" }  # Get the Devise for each Filiale
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "FILIALE": "$_id",
                    "Montant de Transactions (Couverture)": "$total_montant",
                    "Devise": "$Devise"
                }
            },
            {
                "$sort": { "Montant de Transactions (Couverture)": -1 }  # Sort by total montant in descending order
            }
        ])

        results = collection_recon_results.aggregate(pipeline)
        df_montants = pd.DataFrame(list(results))

        return df_montants

    except Exception as e:
        st.write(f'Error calculating sum of Montant Total de Transactions by Filiale: {str(e)}')
        return pd.DataFrame()



def count_rejected_by_filiale(last_30_days=False):
    """
    Count the number of rejected transactions grouped by Filiale.

    Parameters:
    last_30_days (bool): If True, counts the number of rejected transactions in the last 30 days.

    Returns:
    pd.DataFrame: DataFrame containing the count of rejected transactions for each Filiale.
    """
    try:
        pipeline = []

        if last_30_days:
            # Calculate the date 30 days ago from today
            thirty_days_ago = datetime.now() - timedelta(days=30)
            thirty_days_ago_str = thirty_days_ago.strftime('%y-%m-%d')
            #print("30 jours")
            #print(thirty_days_ago_str)
            pipeline.append({
                "$match": {
                    "rejected_date": {"$gte": thirty_days_ago_str}
                }
            })

        pipeline.extend([
            {
                "$group": {
                    "_id": "$FILIALE",
                    "Rejected Count": {"$sum": 1}  # Count the number of rejected transactions
                }
            },
            {
                "$sort": {"Rejected Count": -1}  # Sort by the number of rejected transactions in descending order
            }
        ])

        results = collection_results_rejects.aggregate(pipeline)
        df_rejected_counts = pd.DataFrame(list(results))
        df_rejected_counts.rename(columns={'_id': 'FILIALE', 'Rejected Count': 'Nombre de Rejets'}, inplace=True)

        return df_rejected_counts
    except Exception as e:
        st.write(f'Error counting rejected transactions by Filiale: {str(e)}')
        return pd.DataFrame()
    
def total_transactions_by_filiale():
    """
    Calculate the total montant de transactions for each Filiale.

    Returns:
    pd.DataFrame: DataFrame containing the total montant de transactions for each Filiale.
    """
    try:
        pipeline = [
            {
                "$project": {
                    "FILIALE": 1,
                    "Montant de Transactions (Couverture)": {
                        "$replaceAll": {  # Remove commas from the montant string
                            "input": "$Montant de Transactions (Couverture)",
                            "find": ",",
                            "replacement": ""
                        }
                    }
                }
            },
            {
                "$project": {
                    "FILIALE": 1,
                    "Montant de Transactions (Couverture)": {
                        "$trim": {  # Remove any extra spaces from the montant string
                            "input": "$Montant de Transactions (Couverture)"
                        }
                    }
                }
            },
            {
                "$project": {
                    "FILIALE": 1,
                    "Montant de Transactions (Couverture)": {  # Convert string to float
                        "$toDouble": "$Montant de Transactions (Couverture)"
                    }
                }
            },
            {
                "$group": {
                    "_id": "$FILIALE",
                    "total_transactions": { "$sum": "$Montant de Transactions (Couverture)" }  # Sum up the montants
                }
            },
            {
                "$sort": { "total_transactions": -1 }  # Sort by total transactions in descending order
            }
        ]

        results = collection_recon_results.aggregate(pipeline)
        df_transactions = pd.DataFrame(list(results))
        df_transactions.rename(columns={'_id': 'FILIALE', 'total_transactions': 'Total Transactions'}, inplace=True)

        return df_transactions

    except Exception as e:
        st.write(f'Error calculating total transactions by Filiale: {str(e)}')
        return pd.DataFrame()

def count_rejects_by_filiale():
    """
    Count the number of rejected transactions grouped by Filiale.

    Returns:
    pd.DataFrame: DataFrame containing the count of rejected transactions for each Filiale.
    """
    try:
        pipeline = [
            {
                "$group": {
                    "_id": "$FILIALE",
                    "rejected_count": { "$sum": 1 }  # Count the number of rejected transactions
                }
            },
            {
                "$sort": { "rejected_count": -1 }  # Sort by the number of rejected transactions in descending order
            }
        ]

        results = collection_results_rejects.aggregate(pipeline)
        df_rejected_counts = pd.DataFrame(list(results))
        df_rejected_counts.rename(columns={'_id': 'FILIALE', 'rejected_count': 'Rejected Count'}, inplace=True)

        return df_rejected_counts

    except Exception as e:
        st.write(f'Error counting rejected transactions by Filiale: {str(e)}')
        return pd.DataFrame()

def total_transactions_by_filiale():
    """
    Calculate the total number of transactions for each Filiale.

    Returns:
    pd.DataFrame: DataFrame containing the total number of transactions for each Filiale.
    """
    try:
        pipeline = [
            {
                "$group": {
                    "_id": "$FILIALE",
                    "total_transactions_count": { "$sum": "$Nbre Total De Transactions" }  # Sum of Nbre Total De Transactions
                }
            },
            {
                "$sort": { "total_transactions_count": -1 }  # Sort by the total number of transactions in descending order
            }
        ]

        results = collection_recon_results.aggregate(pipeline)
        df_transactions = pd.DataFrame(list(results))
        df_transactions.rename(columns={'_id': 'FILIALE', 'total_transactions_count': 'Total Transactions Count'}, inplace=True)

        return df_transactions

    except Exception as e:
        st.write(f'Error calculating total transactions by Filiale: {str(e)}')
        return pd.DataFrame()



def taux_de_rejets_by_filiale():
    """
    Calculate the taux de rejets for each Filiale.

    Returns:
    pd.DataFrame: DataFrame containing the taux de rejets, number of total transactions, and number of rejected transactions for each Filiale.
    """
    try:
        # Get total transactions count and rejected transactions count data
        df_total_transactions = total_transactions_by_filiale()
        df_rejected_counts = count_rejects_by_filiale()

        # Ensure columns are correctly named and contain the data we need
        #print("Total Transactions DataFrame:")
        #print(df_total_transactions)
        #print("Rejected Transactions DataFrame:")
        #print(df_rejected_counts)

        # Merge the dataframes on 'FILIALE'
        df_merged = pd.merge(df_total_transactions, df_rejected_counts, on='FILIALE', how='left')

        # Check if merge was successful
        #print("Merged DataFrame:")
        #print(df_merged)

        # Calculate taux de rejets
        df_merged['Taux de Rejets (%)'] = (df_merged['Rejected Count'] / df_merged['Total Transactions Count']) * 100

        # Convert 'Nbre de Rejets' to integers
        df_merged['Rejected Count'] = df_merged['Rejected Count'].fillna(0).astype(int)

        # Format 'Taux de Rejets (%)' as a percentage string
        df_merged['Taux de Rejets (%)'] = df_merged['Taux de Rejets (%)'].apply(lambda x: f'{x:.2f}%')

        # Rename columns for clarity
        df_merged.rename(columns={
            'Total Transactions Count': 'Nbre Total De Transactions',
            'Rejected Count': 'Nbre de Rejets'
        }, inplace=True)

        # Print DataFrame for debugging
        #print("DataFrame with Taux de Rejets:")
        #print(df_merged)

        return df_merged[['FILIALE', 'Nbre Total De Transactions', 'Nbre de Rejets', 'Taux de Rejets (%)']]

    except Exception as e:
        st.write(f'Error calculating taux de rejets by Filiale: {str(e)}')
        return pd.DataFrame()

def montants_rejetes_par_filiale(filter_last_30_days=False):
    """
    Calculate the sum of Montant Total de Transactions for each Filiale and concatenate it with Devise.
    Optionally filter transactions to include only those from the last 30 days.

    Args:
    filter_last_30_days (bool): If True, filter transactions to include only those from the last 30 days.

    Returns:
    pd.DataFrame: DataFrame containing the sum of Montant Total de Transactions for each Filiale, concatenated with Devise.
    """
    try:
        pipeline = []

        if filter_last_30_days:
            # Calculate the date 30 days ago from today
            thirty_days_ago = datetime.now() - timedelta(days=30)
            thirty_days_ago_str = thirty_days_ago.strftime('%y-%m-%d')
            pipeline.append({
                "$match": {
                    "rejected_date": { "$gte": thirty_days_ago_str }
                }
            })

        pipeline.extend([
            {
                "$group": {
                    "_id": "$FILIALE",
                    "Montant": { "$sum": "$Montant" },  # Sum up the montants
                    "Devise": { "$first": "$Devise" }  # Get the Devise for each Filiale
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "FILIALE": "$_id",
                    "Montant": "$Montant",
                    "Devise": "$Devise"
                }
            },
            {
                "$sort": { "Montant": -1 }  # Sort by total montant in descending order
            }
        ])

        results = collection_results_rejects.aggregate(pipeline)
        df_montants = pd.DataFrame(list(results))

        return df_montants

    except Exception as e:
        st.write(f'Error calculating sum of rejected Montant Total de Transactions by Filiale: {str(e)}')
        return pd.DataFrame()


def search_rejects():
    """
    Search for records in the Reconciliation_rejects.

    Returns:
    pd.DataFrame: DataFrame containing the search rejects.
    """
    try:
        # Query the MongoDB collection for records with the specified Transaction_Date
        rejects_recon = collection_results_rejects.find()

        # Convert the results to a DataFrame
        df_rejects_recon = pd.DataFrame(list(rejects_recon))

        return df_rejects_recon
    except Exception as e:
        # Return an empty DataFrame if the search fails
        st.write(f'Error searching for records by Transaction_Date: {str(e)}')


def insert_recycles_data(df_recycles):
    """
    Insert recycles DataFrame data into MongoDB.

    Parameters:
    df (pd.DataFrame): DataFrame containing the reconciliated data.
    """
    data_to_insert = df_recycles.to_dict("records")
    collection_results_recycles.insert_many(data_to_insert)
    return 'Recycled data archived in database'


def get_recycles():
    """
    get recycles

    """
    try:
        # Query the MongoDB collection for records with the specified Transaction_Date
        recycles_recon = collection_results_recycles.find()

        # Convert the results to a DataFrame
        df_recycles_recon = pd.DataFrame(list(recycles_recon))

        return df_recycles_recon
    except Exception as e:
        # Return an empty DataFrame if the search fails
        st.write(f'Error getting recycles: {str(e)}')

def delete_recycled_transactions(df_rejects):
    """
    Delete transactions from 'Reconciliation_rejects' collection that are present in the selected DataFrame.
    delete by transaction ARN

    Parameters:
    - df_rejects: DataFrame containing the transactions to be deleted.
    """
    try:
        # Get the MongoDB collection for rejects
        collection_rejects = db['Reconciliation_rejects']

        # Convert the DataFrame to a list of dicts
        transactions_to_delete = df_rejects.to_dict(orient='records')

        # Delete the transactions
        for transaction in transactions_to_delete:
            collection_rejects.delete_one({
                'ARN': transaction['ARN']
            })
    except Exception as e:
        st.error(f"Erreur lors de la suppression des transactions recyclées: {str(e)}")

import streamlit as st
import json
import pandas as pd
from datetime import datetime
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode
from Reconciliation_Automation_SG.processing_bank_sources import *
import database_actions as da
import time

def get_rejects():
    with open('Reconciliation_Automation_SG/currency_codes.json', 'r') as f:
        currency_codes = json.load(f)

    currency_codes_reversed = {v: k for k, v in currency_codes.items()}

    # Assuming da.search_rejects() returns a DataFrame
    df_rejects = da.search_rejects()
    st.write("### Transactions rejetées")
    
    # Drop the '_id' column
    df_rejects.drop(columns=['_id'], inplace=True)

    # Rename 'FILIALE BANQUE' to 'Filiale Banque'
    if 'FILIALE' in df_rejects.columns:
        df_rejects.rename(columns={'FILIALE': 'BANQUE'}, inplace=True)
    
    # Rename 'rejected_date' to 'Date Traitement' and format date to %Y-%m-%d
    if 'rejected_date' in df_rejects.columns:
        # Convert 'rejected_date' from 'dd-mm-yy' to 'yyyy-mm-dd'
        df_rejects['Date Traitement'] = pd.to_datetime(df_rejects['rejected_date'], format='%y-%m-%d').apply(
            lambda x: f"{x.year}-{x.month:02d}-{x.day}"  # Format to yyyy-mm-dd
        )
        df_rejects.drop(columns=['rejected_date'], inplace=True)

    if 'Devise' in df_rejects.columns:
        df_rejects['Devise'] = df_rejects['Devise'].map(currency_codes_reversed)

    # Reorder the columns
    column_order = [
        'BANQUE', 'RESEAU', 'ARN', 'Autorisation', 'Date Transaction', 
        'Montant', 'Devise', 'Date Traitement', 'Date Retraitement', 'Motif'
    ]
    
    # Ensure 'Date Retraitement' column is present before reordering
    if 'Date Retraitement' not in df_rejects.columns:
        df_rejects['Date Retraitement'] = pd.NaT
    
    df_rejects = df_rejects[column_order]
    
    # Configure the grid options
    gb = GridOptionsBuilder.from_dataframe(df_rejects)
    gb.configure_selection('multiple', use_checkbox=True)
    grid_options = gb.build()
    
    # Display the DataFrame with a fixed height
    grid_response = AgGrid(
        df_rejects, 
        gridOptions=grid_options,
        update_mode=GridUpdateMode.SELECTION_CHANGED,
        height=250,  # Adjust the height as needed
        width='100%',
    )
    
    # Get the selected rows
    selected_rows = grid_response['selected_rows']
    
    # Convert selected rows to a DataFrame
    selected_df = pd.DataFrame(selected_rows)
    
    # Display the selected rows
    if not selected_df.empty:
        st.write("### Transaction(s) sélectionnée(s)")
        st.write(selected_df)
    else:
        st.write("Aucune transaction sélectionnée à recycler")
    
    return selected_df  # Return the selected DataFrame

def format_dates(df_rejects):
    """
    Convert date formats in the DataFrame from 'yyyy-mm-ddThh:mm:ss.sss+00:00' 
    to 'yyyy-mm-dd' for 'Date Traitement' and 'Date Retraitement' columns.

    Parameters:
    - df_rejects: The DataFrame containing 'Date Traitement' and 'Date Retraitement' columns.

    Returns:
    - The DataFrame with the date columns formatted to 'yyyy-mm-dd'.
    """
    if 'Date Transaction' in df_rejects.columns:
        df_rejects['Date Transaction'] = pd.to_datetime(df_rejects['Date Traitement'], format='%Y-%m-%dT%H:%M:%S.%f+00:00').dt.strftime('%Y-%m-%d')
    # Check if the required columns exist in the DataFrame
    if 'Date Traitement' in df_rejects.columns:
        # Convert 'Date Traitement' from 'yyyy-mm-ddThh:mm:ss.sss+00:00' to 'yyyy-mm-dd'
        df_rejects['Date Traitement'] = pd.to_datetime(df_rejects['Date Traitement'], format='%Y-%m-%dT%H:%M:%S.%f+00:00').dt.strftime('%Y-%m-%d')

    if 'Date Retraitement' in df_rejects.columns:
        # Convert 'Date Retraitement' from 'yyyy-mm-ddThh:mm:ss.sss+00:00' to 'yyyy-mm-dd'
        df_rejects['Date Retraitement'] = pd.to_datetime(df_rejects['Date Retraitement'], format='%Y-%m-%dT%H:%M:%S.%f+00:00').dt.strftime('%Y-%m-%d')

    return df_rejects

def recycle_transactions(selected_df):
    # Save the DataFrame to the database
    da.insert_recycles_data(selected_df)
    da.delete_recycled_transactions(selected_df)
    st.success("Transaction(s) recyclée(s) avec succès !")
    time.sleep(3)

def main():
    st.header("♻️ :violet[Ajouter transactions recyclées]", divider='rainbow')
    st.sidebar.image("assets/Logo_hps_0.png", use_column_width=True)
    st.sidebar.divider()
    st.sidebar.page_link("app.py", label="**Accueil**", icon="🏠")
    st.sidebar.page_link("pages/results_recon.py", label="**:alarm_clock: Historique**")
    st.sidebar.page_link("pages/Dashboard.py", label="  **📊 Tableau de bord**" )
    st.sidebar.page_link("pages/MasterCard_UI.py", label="**🔀 Réconciliation MasterCard**")
    st.sidebar.page_link("pages/calendar_view.py", label="**📆 Vue Agenda**")
    st.sidebar.page_link("pages/rejects_recycles.py", label="**♻️ Rejets recylés**")


    # Call the get_rejects function to display the DataFrame and get selected rows
    selected_df = get_rejects()
    
    # Add a date input widget
    search_date = st.date_input("**Sélectionnez une date de recyclage :**", value=datetime.today(), key="search_date")
    
    # Format the date in %Y-%m-%d format
    formatted_date_rejects = search_date.strftime('%Y-%m-%d')

    # Button to process the selected rows
    if st.button("**♻️ Recycler**", key="recycle_button", type="primary", use_container_width=True):

        if selected_df.empty:
            st.warning("Aucune transaction sélectionnée à recycler.")
        else:
            # Add the Date Retraitement column with the selected date
            selected_df['Date Retraitement'] = formatted_date_rejects
            st.write(selected_df)

            # Set a session state variable to show the confirmation
            st.session_state.confirm_recycle = True

    # Show the confirmation modal if the recycle button was clicked
    if st.session_state.get('confirm_recycle', False):
        st.warning("Êtes-vous sûr de vouloir recycler les transaction(s) sélectionnée(s) ?")
        col1, col2 = st.columns(2)
        with col1:
            if st.button("Oui, recycler"):
                selected_df['Date Retraitement'] = formatted_date_rejects
                recycle_transactions(selected_df)
                st.session_state.confirm_recycle = False  # Reset the confirmation state
                st.rerun()  # Rerun the app to clear the modal and the selected DataFrame
        with col2:
            if st.button("Non, annuler"):
                st.session_state.confirm_recycle = False  # Reset the confirmation state
                st.rerun()  # Rerun the app to clear the modal and the selected DataFrame

    if st.button("**♻️ Get Transactions recyclées**", key="get_recycle_button", type="primary", use_container_width=True):
        recycles_df = da.get_recycles()
        recycles_df.drop(columns=['_id'], inplace=True)
        st.write("### Rejets recyclés")
        st.write(recycles_df)
        download_file(recon=False, df=recycles_df, file_partial_name='recycles_rejects', button_label=":arrow_down: Téléchargez les rejets recyclés", run_date=0)

    uploaded_recycled_file = st.file_uploader(":arrow_down: **Chargez le fichier des transactions recyclées**", type=["xlsx"])
    if uploaded_recycled_file:
        recycled_file_path = save_uploaded_file(uploaded_recycled_file)
        df_recyc = pd.read_excel(recycled_file_path)
        format_dates(df_recyc)
        st.button(":floppy_disk: Stocker les rejets recyclées dans la base de donnèes" , on_click= lambda: da.insert_recycles_data(df_recyc) , key= "stocker_recycles_button1",type="primary" , use_container_width=True)

if __name__ == "__main__":
    main()

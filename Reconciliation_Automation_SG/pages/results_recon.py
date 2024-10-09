import streamlit as st
from datetime import datetime
from Reconciliation_Automation_SG.database_actions import *
from processing_bank_sources import *

def main():
    st.sidebar.image("assets/Logo_hps_0.png", use_column_width=True)
    st.sidebar.divider()
    st.sidebar.page_link("app.py", label="**Accueil**", icon="🏠")
    st.sidebar.page_link("pages/results_recon.py", label="**:alarm_clock: Historique**")
    st.sidebar.page_link("pages/Dashboard.py", label="  **📊 Tableau de bord**" )
    st.sidebar.page_link("pages/MasterCard_UI.py", label="**🔀 Réconciliation MasterCard**")
    st.sidebar.page_link("pages/calendar_view.py", label="**📆 Vue Agenda**")
    st.sidebar.page_link("pages/rejects_recycles.py", label="**♻️ Rejets recylés**")

    
    st.header("Historique de Réconciliation", divider='rainbow')
    st.write("  ")
    
    # Create date input for processing date
    search_date = st.date_input("**Sélectionnez une date de processing :**", value=datetime.today(), key="search_date")
    formatted_date = search_date.strftime('%Y-%m-%d')
    formatted_date_rejects = search_date.strftime('%y-%m-%d')
    
    # Search and display the results when the search button is clicked
    if st.button(":mag_right: **Search**", key="search_button", type="primary", use_container_width=True):
        df_search_recon_results = search_results_by_transaction_date(formatted_date)
        df_search_rejects_results = search_rejects_by_transaction_date(formatted_date_rejects)
        
        # Display reconciliation results
        if not df_search_recon_results.empty:
            st.write("Résultats de Réconciliation")
            df_search_recon_results.drop(columns=['_id'], inplace=True)
            st.dataframe(df_search_recon_results.style.apply(highlight_non_reconciliated_row, axis=1))
            excel_path_recon, file_name_recon = download_file(
                recon=True, 
                df=df_search_recon_results, 
                file_partial_name='results_reconciliation', 
                button_label=":arrow_down: Téléchargez les résultats de réconciliation", 
                run_date=formatted_date
            )
        else:
            st.warning("Aucun enregistrement trouvé pour la date sélectionnée.")
        

        # Display rejected transactions results
        if not df_search_rejects_results.empty:
            st.write("Transactions Rejetées")
            
            df_search_rejects_results.drop(columns=['_id'], inplace=True)
            df_search_rejects_results.drop(columns=['rejected_date'], inplace=True)
            st.dataframe(df_search_rejects_results)
            excel_path_rejects, file_name_rejects = download_file(
                recon=False, 
                df=df_search_rejects_results, 
                file_partial_name='results_rejected_transactions', 
                button_label=":arrow_down: Téléchargez les transactions rejetées", 
                run_date=formatted_date_rejects
            )
        else:
            st.warning("Aucun enregistrement trouvé pour les transactions rejetées à la date sélectionnée.")

if __name__ == "__main__":
    main()

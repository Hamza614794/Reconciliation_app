#import streamlit as st
import plotly.graph_objects as go
from Reconciliation_Automation_SG.parser_TT140_MasterCard import *
from Reconciliation_Automation_SG.processing_bank_sources import *
#from Reconciliation_Automation_SG.database_actions import *


def upload_all_sources():
    if uploaded_mastercard_file is not None:
        try:
            run_date, day_after = extract_date_from_mastercard_file(uploaded_mastercard_file.getvalue().decode("utf-8"))
            st.write("**La date du rapport MasterCard est :calendar:**", run_date)
            st.write("**Vous effectuerez la réconciliation pour la date :calendar:**", day_after)
        except Exception as e:
            st.error(f"Erreur lors de l'extraction de la date à partir du fichier Mastercard :{e}")

    df_cybersource = pd.DataFrame(columns=default_columns_cybersource)
    df_pos = pd.DataFrame(columns=default_columns_pos)
    df_sai_manuelle = pd.DataFrame(columns=default_columns_saisie_manuelle)

    try:
        if uploaded_cybersource_file:
            cybersource_file_path = save_uploaded_file(uploaded_cybersource_file)
            validate_file_name_and_date(uploaded_cybersource_file.name, 'CYBERSOURCE', date_to_validate=day_after)
            df_cybersource = reading_cybersource(cybersource_file_path)
            mastercard_transactions_cybersource = df_cybersource[df_cybersource['RESEAU'] == 'MASTERCARD INTERNATIONAL']
            total_transactions['Cybersource'] = mastercard_transactions_cybersource['NBRE_TRANSACTION'].sum()
    except Exception as e:
        st.error(f"Erreur lors du traitement du fichier Cybersource :{e}")

    try:
        if uploaded_pos_file:
            pos_file_path = save_uploaded_file(uploaded_pos_file)
            validate_file_name_and_date(uploaded_pos_file.name, 'POS', date_to_validate=day_after)
            df_pos = reading_pos(pos_file_path)
            mastercard_transactions_pos = df_pos[(df_pos['RESEAU'] == 'MASTERCARD INTERNATIONAL') &
                                                 (~df_pos['TYPE_TRANSACTION'].str.endswith('_MDS'))]
            total_transactions['POS'] = mastercard_transactions_pos['NBRE_TRANSACTION'].sum()
    except Exception as e:
        st.error(f"Erreur lors du traitement du fichier POS :{e}")

    try:
        if uploaded_sai_manuelle_file:
            sai_manuelle_file_path = save_uploaded_file(uploaded_sai_manuelle_file)
            validate_file_name_and_date(uploaded_sai_manuelle_file.name, 'SAIS_MANU', date_to_validate=day_after)
            df_sai_manuelle = reading_saisie_manuelle(sai_manuelle_file_path)
            mastercard_transactions_sai_manuelle = df_sai_manuelle[df_sai_manuelle['RESEAU'] == 'MASTERCARD INTERNATIONAL']
            total_transactions['Saisie Manuelle'] = mastercard_transactions_sai_manuelle['NBRE_TRANSACTION'].sum()
    except Exception as e:
        st.error(f"Erreur lors du traitement du fichier de Saisie Manuelle :{e}")

    return run_date, day_after,df_cybersource, df_sai_manuelle, df_pos

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



def handle_recon(filtered_cybersource_df, filtered_saisie_manuelle_df, filtered_pos_df):
    try:
        # Initialize session state variables
        if 'df_reconciliated' not in st.session_state:
            st.session_state.df_reconciliated = None
        if 'df_non_reconciliated' not in st.session_state:
            st.session_state.df_non_reconciliated = None
        if 'df_summary' not in st.session_state:
            st.session_state.df_summary = None
        if 'df_rejections' not in st.session_state:
            st.session_state.df_rejections = None


        if uploaded_mastercard_file:
            mastercard_file_path = save_uploaded_file(uploaded_mastercard_file)
            nbr_total_MC, rejected_summary, rejected_df = parse_t140_MC(mastercard_file_path)
            col1, col2, col3 = st.columns(3)
            col1.metric("**Nombre total de transactions dans le fichier Mastercard :**", value=nbr_total_MC)

            if uploaded_recycled_file:
                recycled_file_path = save_uploaded_file(uploaded_recycled_file)
                df_recyc = pd.read_excel(recycled_file_path)
                st.write("La date du filtrage : ", filtering_date)
                df_recycled, merged_df, total_nbre_transactions = merging_with_recycled(
                    recycled_file_path, filtered_cybersource_df, filtered_saisie_manuelle_df, filtered_pos_df, filtering_date)
                st.header("Transactions à recycler")
                st.dataframe(df_recycled , use_container_width=True)
                df_recycled.rename(columns={'FILIALE': 'BANQUE'}, inplace=True)
                #st.button(":floppy_disk: Stocker les rejets recyclées dans la base de donnèes" , on_click= lambda: insert_recycles_data(df_recycled) , key= "stocker_recycles_button1",type="primary" , use_container_width=True)
                
                #st.write(len(df_recycled))
                #st.write(merged_df)
                st.write("### Nombre de transactions des sources avec rej. recyc.", total_nbre_transactions)

            else:
                merged_df, total_nbre_transactions = merging_sources_without_recycled(
                    filtered_cybersource_df, filtered_saisie_manuelle_df, filtered_pos_df)
                st.write("### Nombre de transactions des sources sans rej. recyc.", total_nbre_transactions)
                st.warning("Le fichier de transactions à recycler n'a pas été chargé."
                           " La réconciliation sera effectuée sans les transactions recyclées.")
            col2.metric("**Nombre total de transactions dans les fichiers :**", value=total_nbre_transactions)
            #st.write(merged_df)
            col3.metric("___Difference___", value=abs(nbr_total_MC - total_nbre_transactions),
                        help="La différence nette des transactions entre les deux côtés est")

            if st.button('Réconcilier', type="primary", use_container_width=True):
                if nbr_total_MC == total_nbre_transactions:
                    st.header('Résulat de la réconciliation')
                    st.session_state.df_reconciliated = handle_exact_match_csv(merged_df , run_date=run_date)
                    st.success("Réconciliation faite sans écart")
                    st.divider()

                else:
                    st.session_state.df_non_reconciliated = handle_non_match_reconciliation(mastercard_file_path, merged_df , run_date=run_date)
                    st.session_state.df_summary = calculate_rejected_summary(mastercard_file_path)
                    st.session_state.df_rejections = extract_rejections(mastercard_file_path, currencies_settings, countries_settings)
                    
                    st.warning("Réconciliation faite avec un écart")
                    st.divider()

            # Always display the dataframes stored in session state
            if st.session_state.df_reconciliated is not None:
                st.header('Résulat de la réconciliation')
                st.dataframe(st.session_state.df_reconciliated)
                col4 = st.columns(1)[0]
                with col4:
                    download_file(recon=True, df=st.session_state.df_reconciliated, file_partial_name='results_recon_MC', button_label=":arrow_down: Téléchargez les résultats de réconciliation", run_date=run_date)
                
            if st.session_state.df_non_reconciliated is not None:
                st.header('Résultat de la Réconciliation')
    
                # Add a selectbox to filter by the Rapprochement column
                filter_options = ['TOUT'] + list(st.session_state.df_non_reconciliated['Rapprochement'].unique())
                filter_value = st.selectbox('Filter Rapprochement par état', options=filter_options)

                # Apply the filter based on the selected value
                if filter_value != 'TOUT':
                    df_to_show = st.session_state.df_non_reconciliated[
                        st.session_state.df_non_reconciliated['Rapprochement'].str.lower() == filter_value.lower()
                    ]
                else:
                    df_to_show = st.session_state.df_non_reconciliated

                # Display the dataframe
                st.dataframe(df_to_show.style.apply(highlight_non_reconciliated_row, axis=1))
                col4= st.columns(3)[0]
                with col4:
                    download_file(recon=True, df=st.session_state.df_non_reconciliated, file_partial_name='results_recon_MC', button_label=":arrow_down: Téléchargez les résultats de réconciliation", run_date=run_date)
                #with col5:
                    #st.button(":floppy_disk: Stocker le résultat de réconciliation " , on_click= lambda: insert_reconciliated_data(st.session_state.df_non_reconciliated) , key= "stocker_button2",type="primary" , use_container_width=True)


                st.header('Résumé des rejets')
                st.dataframe(st.session_state.df_summary , use_container_width=True)
                col7 = st.columns(2)[0]
                with col7 :
                    download_file(recon=False, df=st.session_state.df_summary, file_partial_name='rejected_summary_MC', button_label=":arrow_down: Téléchargez le résumé des rejets", run_date=run_date)
                #with col9 :
                   # st.button(":floppy_disk: Stocker le résumé des rejets " , on_click= lambda: insert_rejection_summary(st.session_state.df_summary) , key= "stocker_button3",type="primary" , use_container_width=True)
              
                st.divider()

                st.header('Transactions Rejetées')
         
                st.dataframe(st.session_state.df_rejections , use_container_width=True)
                

                col10 = st.columns(3)[0]
                with col10:
                    download_file(recon=False, df=st.session_state.df_rejections, file_partial_name='rejected_transactions_MC', button_label=":arrow_down: Téléchargez les rejets", run_date=run_date)
                #with col12:
                    #st.button(":floppy_disk: Stocker les rejets " , on_click= lambda: insert_rejected_transactions(st.session_state.df_rejections, run_date) , key= "stocker_button4",type="primary" , use_container_width=True)

        else:
            st.warning("Veuillez charger tous les fichiers nécessaires pour continuer.")
    except Exception as e:
        st.error(f"Erreur lors du traitement du fichier Mastercard ")
        st.write(e)
def main():
    global uploaded_mastercard_file, uploaded_cybersource_file, uploaded_pos_file, uploaded_sai_manuelle_file, filtering_date, uploaded_recycled_file
    st.sidebar.image("assets/Logo_hps_0.png", use_column_width=True)
    st.sidebar.divider()
    st.sidebar.page_link("app.py", label="**Accueil**", icon="🏠")
    #st.sidebar.page_link("pages/results_recon.py", label="**:alarm_clock: Historique**")
    #st.sidebar.page_link("pages/Dashboard.py", label="  **📊 Tableau de bord**" )
    st.sidebar.page_link("pages/MasterCard_UI.py", label="**🔀 Réconciliation MasterCard**")
    #st.sidebar.page_link("pages/calendar_view.py", label="**📆 Vue Agenda**")
    #st.sidebar.page_link("pages/rejects_recycles.py", label="**♻️ Rejets recylés**")
    
    st.header("📤 :violet[Réconciliation MASTERCARD]", divider='rainbow')
    uploaded_mastercard_file = st.file_uploader(":arrow_down: **Chargez le fichier Mastercard**", type=["001"])
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

    global run_date , day_after
    st.divider()
    global default_columns_cybersource, default_columns_saisie_manuelle, default_columns_pos, total_transactions

    default_columns_cybersource = ['NBRE_TRANSACTION', 'MONTANT_TOTAL', 'CUR', 'FILIALE', 'RESEAU', 'TYPE_TRANSACTION']
    default_columns_saisie_manuelle = ['NBRE_TRANSACTION', 'MONTANT_TOTAL', 'CUR', 'FILIALE', 'RESEAU']
    default_columns_pos = ['FILIALE', 'RESEAU', 'TYPE_TRANSACTION', 'DATE_TRAI', 'CUR', 'NBRE_TRANSACTION', 'MONTANT_TOTAL']

    if uploaded_mastercard_file :

        total_transactions = {'Cybersource': 0, 'POS': 0, 'Saisie Manuelle': 0}

        try:
            run_date , day_after, df_cybersource, df_sai_manuelle, df_pos = upload_all_sources()
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
            handle_recon(filtered_cybersource_df, filtered_saisie_manuelle_df, filtered_pos_df)
        except Exception as e:
            st.error(f"Impossible de continuer avec la réconciliation")
            st.write(e)

    # Handling session state variables based on file uploads
    if not uploaded_mastercard_file and not uploaded_cybersource_file and not uploaded_pos_file and not uploaded_sai_manuelle_file:
        # Reset session state variables if no files are uploaded
        st.session_state.df_reconciliated = None
        st.session_state.df_non_reconciliated = None
        st.session_state.df_summary = None
        st.session_state.df_rejections = None
        st.warning("Veuillez charger tous les fichiers nécessaires pour continuer.")

if __name__ == "__main__":
    main()

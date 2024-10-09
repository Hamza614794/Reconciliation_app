import streamlit as st

def main():
    global uploaded_mastercard_file, uploaded_cybersource_file, uploaded_pos_file, uploaded_sai_manuelle_file, filtering_date, uploaded_recycled_file
    st.sidebar.image("assets/Logo_hps_0.png", use_column_width=True)
    st.sidebar.divider()
    st.sidebar.page_link("app.py", label="**Accueil**", icon="🏠")
    st.sidebar.page_link("pages/results_recon.py", label="**:alarm_clock: Historique**")
    st.sidebar.page_link("pages/Dashboard.py", label="  **📊 Tableau de bord**" )
    st.sidebar.page_link("pages/VISA_UI.py", label="**🔀 Réconciliation VISA**")
    st.sidebar.page_link("pages/calendar_view.py", label="**📆 Vue Agenda**")
    st.sidebar.page_link("pages/rejects_recycles.py", label="**♻️ Rejets recylés**")
    
    st.header("📤 :violet[Réconciliation VISA]", divider='rainbow')

if __name__ == "__main__":
    main()

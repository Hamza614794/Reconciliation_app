import streamlit as st
import plotly.graph_objects as go
from Reconciliation_Automation_SG.database_actions import *

def pie_chart_etat_rapprochement():
    st.header("	:bar_chart:  Etat de rapprochement", divider='grey')

    # Get the counts of each Rapprochement
    df_counts = count_rapprochement()

    if df_counts.empty:
        st.write("No data available for état de rapprochement.")
        return

    # Extract labels and sizes
    labels = df_counts['Rapprochement'].tolist()
    sizes = df_counts['Count'].tolist()

    def create_interactive_pie_chart():
        fig = go.Figure(data=[go.Pie(labels=labels, values=sizes, hole=.3, textinfo='label+percent+value')])
        return fig

    fig = create_interactive_pie_chart()
    st.plotly_chart(fig)

def create_bar_chart_by_filiale():
    """
    Create a bar chart that counts the number of Rapprochement states by Filiale.

    Returns:
    fig: A Plotly bar chart figure.
    """
    df_counts = count_rapprochement_by_filiale()

    if df_counts.empty:
        print("No data available for état de rapprochement by Filiale.")
        return None

    # Initialize the bar chart figure
    fig = go.Figure()

    # Add bars for each Filiale
    for filiale in df_counts['FILIALE'].unique():
        df_filiale = df_counts[df_counts['FILIALE'] == filiale]
        fig.add_trace(go.Bar(
            x=df_filiale['Rapprochement'],
            y=df_filiale['count'],
            name=filiale
        ))

    # Update layout for better visuals
    fig.update_layout(
        barmode='group',
        title="Repartition de l'état de rapproch. par filiale",
        xaxis_title="Rapprochement",
        yaxis_title="Count",
        xaxis=dict(
            title='Rapprochement',
            tickmode='linear'
        ),
        yaxis=dict(
            title='Count'
        ),
        legend_title="FILIALE",
    )

    # Debugging: Check the figure content
    #print(fig)

    return fig

def display_bar_chart_by_filiale():
    st.header(":bar_chart: Etat de rapproch. par filiale", divider='grey')

    # Create the bar chart figure
    fig_bar = create_bar_chart_by_filiale()
    if fig_bar is not None:
        st.plotly_chart(fig_bar)
    else:
        st.write("No bar chart available.")

def display_table_montants_by_filiale():
    st.header(":bar_chart: Montants par filiale", divider='grey')

    # Add a combobox for filtering options
    filter_option = st.selectbox(
        'Sélectionner la période:',
        ('Total', 'Derniers 30 jours')
    )

    # Determine the filter based on the user's selection
    filter_last_30_days = filter_option == 'Derniers 30 jours'

    # Get the DataFrame
    df_montants = sum_montants_by_filiale(filter_last_30_days=filter_last_30_days)

    if df_montants.empty:
        st.write("No data available for Montant Total de Transactions by Filiale.")
    else:
        # Format the Montant de Transactions (Couverture) column for better readability
        df_montants['Montant de Transactions (Couverture)'] = df_montants['Montant de Transactions (Couverture)'].apply(lambda x: f'{x:,.2f}')
        
        # Display the DataFrame as a table
        st.table(df_montants)



def create_bar_chart_rejected_by_filiale(last_30_days=False):
    """
    Create a bar chart that shows the number of rejected transactions for each Filiale.

    Parameters:
    last_30_days (bool): If True, counts the number of rejected transactions in the last 30 days.

    Returns:
    fig: A Plotly bar chart figure.
    """
    df_rejected_counts = count_rejected_by_filiale(last_30_days)

    if df_rejected_counts.empty:
        #print("No data available for rejected transactions by Filiale.")
        return None

    # Initialize the bar chart figure
    fig = go.Figure()

    # Add bars for each Filiale
    for filiale in df_rejected_counts['FILIALE'].unique():
        df_filiale = df_rejected_counts[df_rejected_counts['FILIALE'] == filiale]
        fig.add_trace(go.Bar(
            x=df_filiale['FILIALE'],  # x-axis represents Filiale
            y=df_filiale['Nombre de Rejets'],  # y-axis represents the count of rejected transactions
            text=df_filiale['Nombre de Rejets'],
            textposition='auto',
            name=filiale,  # The name for this trace, which will appear in the legend
            legendgroup=filiale  # Grouping traces with the same FILIALE for distinct colors
        ))

    # Update layout for better visuals
    title = "Nombre de Transactions Rejetées par Filiale"
    if last_30_days:
        title += " (Derniers 30 jours)"

    fig.update_layout(
        title=title,
        xaxis_title="Filiale",  # Remove x-axis title
        yaxis_title="Nombre de Rejets",
        xaxis=dict(
            tickmode='linear',
            tickangle=-45,
            showticklabels=False  # Hide x-axis labels
        ),
        yaxis=dict(
            title="Nombre de Rejets"
        ),
        legend_title="FILIALE",
        showlegend=True,  # Ensure that the legend is shown
        barmode='group'  # Group the bars to show each Filiale
    )

    # Debugging: Check the figure content
    #print(fig)

    return fig


def display_bar_chart_rejects_by_filiale():
    st.header(":bar_chart: Rejets par filiale", divider='grey')
    
    # Add a selectbox to choose the time range
    time_range = st.selectbox(
        "Sélectionnez la période:",
        ["Total", "Derniers 30 jours"]
    )
    
    # Determine if the last 30 days option is selected
    last_30_days = True if time_range == "Derniers 30 jours" else False

    # Create the bar chart figure based on the selection
    if last_30_days:
        fig_bar = create_bar_chart_rejected_by_filiale(last_30_days=True)
    else:
        fig_bar = create_bar_chart_rejected_by_filiale(last_30_days=False)
    
    if fig_bar is not None:
        st.plotly_chart(fig_bar)
    else:
        st.write("No bar chart available.")


def create_table_taux_de_rejets_by_filiale():
    """
    Create a DataFrame that shows the taux de rejets for each Filiale.

    Returns:
    pd.DataFrame: DataFrame containing the taux de rejets for each Filiale.
    """
    df_taux_de_rejets = taux_de_rejets_by_filiale()

    if df_taux_de_rejets.empty:
        #print("No data available for Taux de Rejets by Filiale.")
        return None

    # Return the DataFrame
    return df_taux_de_rejets

def display_table_taux_de_rejets_by_filiale():
    st.header(":bar_chart: Taux de Rejets par Filiale", divider='grey')

    # Get the DataFrame
    df_taux_de_rejets = create_table_taux_de_rejets_by_filiale()

    if df_taux_de_rejets is None or df_taux_de_rejets.empty:
        st.write("No data available for Taux de Rejets by Filiale.")
    else:
        # Display the DataFrame as a table
        st.table(df_taux_de_rejets)
        

def display_table_rejected_montants_by_filiale():
    st.header(":bar_chart: Montants rejetés par filiale", divider='grey')

    # Add a combobox for filtering options with a unique key
    filter_option = st.selectbox(
        'Sélectionner la période:',
        ('Total', 'Derniers 30 jours'),
        key='rejected_montants_by_filiale_filter'  # Unique key
    )

    # Determine the filter based on the user's selection
    filter_last_30_days = filter_option == 'Derniers 30 jours'

    # Get the DataFrame
    df_montants_rejetes = montants_rejetes_par_filiale(filter_last_30_days=filter_last_30_days)

    if df_montants_rejetes.empty:
        st.write("No data available for Montant Total de Transactions by Filiale.")
    else:
        # Format the Montant de Transactions (Couverture) column for better readability
        df_montants_rejetes['Montant'] = df_montants_rejetes['Montant'].apply(lambda x: f'{x:,.2f}')
        
        # Display the DataFrame as a table
        st.table(df_montants_rejetes)


def main():
    st.sidebar.image("assets/Logo_hps_0.png", use_column_width=True)
    st.sidebar.divider()
    st.sidebar.page_link("app.py", label="**Accueil**", icon="🏠")
    st.sidebar.page_link("pages/results_recon.py", label="**:alarm_clock: Historique**")
    st.sidebar.page_link("pages/Dashboard.py", label="  **📊 Tableau de bord**" )
    st.sidebar.page_link("pages/MasterCard_UI.py", label="**🔀 Réconciliation MasterCard**")
    st.sidebar.page_link("pages/calendar_view.py", label="**📆 Vue Agenda**")
    st.sidebar.page_link("pages/rejects_recycles.py", label="**♻️ Rejets recylés**")

    
    st.header("Tableau de bord", divider='rainbow')
    st.write("  ")
    col1, col2  = st.columns(2)
    with col1:
        pie_chart_etat_rapprochement()
    with col2:
        display_bar_chart_by_filiale()
        display_table_montants_by_filiale()
        display_bar_chart_rejects_by_filiale()
        display_table_taux_de_rejets_by_filiale()
        display_table_rejected_montants_by_filiale()


if __name__ == "__main__":
    main()
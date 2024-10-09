import streamlit as st
from datetime import datetime, timedelta
import pandas as pd
from streamlit.components.v1 import html
from Reconciliation_Automation_SG.database_actions import search_results_by_transaction_date

# Sidebar setup
st.sidebar.image("assets/Logo_hps_0.png", use_column_width=True)
st.sidebar.divider()
st.sidebar.page_link("app.py", label="**Accueil**", icon="🏠")
st.sidebar.page_link("pages/results_recon.py", label="**:alarm_clock: Historique**")
st.sidebar.page_link("pages/Dashboard.py", label="  **📊 Tableau de bord**" )
st.sidebar.page_link("pages/MasterCard_UI.py", label="**🔀 Réconciliation MasterCard**")
st.sidebar.page_link("pages/calendar_view.py", label="**📆 Vue Agenda**")
st.sidebar.page_link("pages/rejects_recycles.py", label="**♻️ Rejets recylés**")



# Initialize session state for year and month if not already set
if 'current_year' not in st.session_state:
    st.session_state.current_year = datetime.now().year
if 'current_month' not in st.session_state:
    st.session_state.current_month = datetime.now().month
if 'clicked_date' not in st.session_state:
    st.session_state.clicked_date = None

# Define the list of months and years
months = [
    "Janvier", "Février", "Mars", "Avril", "Mai", "Juin",
    "Juillet", "Août", "Septembre", "Octobre", "Novembre", "Décembre"
]
years = list(range(2020, 2080))

# Ensure the current year and month are within the valid ranges
if st.session_state.current_year not in years:
    st.session_state.current_year = years[-1]  # Set to the last year in the list if out of bounds
if st.session_state.current_month not in range(1, 13):
    st.session_state.current_month = 12  # Set to December if out of bounds

# Calculate the correct indices for the selectbox
current_month_index = st.session_state.current_month - 1  # Zero-based index for selectbox
current_year_index = years.index(st.session_state.current_year)  # Index of the current year

# Create the month and year selectors
col1, col2 = st.columns([2, 1])

with col1:
    month = st.selectbox(
        'Sélectionnez le mois',
        months,
        index=current_month_index
    )

with col2:
    year = st.selectbox(
        'Sélectionnez l\'année',
        years,
        index=current_year_index
    )

# Check if month or year has changed
if month != months[current_month_index] or year != st.session_state.current_year:
    st.session_state.current_month = months.index(month) + 1
    st.session_state.current_year = year
    # Update query parameters and rerun the script
    #st.query_params(year=str(st.session_state.current_year), month=str(st.session_state.current_month))
    #year=str(st.session_state.current_year)
    #month=str(st.session_state.current_year)
    month = st.session_state.current_month
    year = st.session_state.current_year
    st.query_params['year'] = year
    st.query_params['month'] = month
    st.rerun()

# Function to fetch events for the current month
# Function to fetch events for the current month
def fetch_events(year, month):
    # Get the start and end dates of the current month
    start_date = datetime(year, month, 1)
    end_date = (start_date + timedelta(days=31)).replace(day=1)  # First day of the next month

    # Fetch all dates for the specified month
    date_range = pd.date_range(start=start_date, end=end_date - timedelta(days=1))

    # Generate the list of events
    calendar_events = []
    for single_date in date_range:
        # Convert to string in the format required for MongoDB
        date_str = single_date.strftime('%Y-%m-%d')
        df_results_recon = search_results_by_transaction_date(date_str)

        if not df_results_recon.empty:
            seen_reseaux = set()  # Track Réseau names to ensure uniqueness
            for _, row in df_results_recon.iterrows():
                reseau = row["Réseau"]
                if reseau not in seen_reseaux:
                    # Add Réseau as the event title
                    calendar_events.append({
                        "title": reseau,
                        "start": f"{date_str}",
                        "end": f"{date_str}",
                        "backgroundColor": "#ff5f00",
                        "borderColor": "#f79e1b",
                        "allDay": True,
                        "date": date_str  # Add the date for the event
                    })
                    seen_reseaux.add(reseau)  # Mark this Réseau as seen for the current date
    return calendar_events

# Fetch the events for the selected month
calendar_events = fetch_events(st.session_state.current_year, st.session_state.current_month)

# Function to generate the HTML body for the calendar
def generate_calendar_body(year, month, events):
    # Get the first and last day of the month
    first_day = datetime(year, month, 1)
    last_day = (first_day + timedelta(days=31)).replace(day=1) - timedelta(days=1)

    # Calculate the number of days in the month and the day of the week for the first day
    num_days = (last_day - first_day).days + 1
    start_day = first_day.weekday()  # Monday is 0, Sunday is 6

    # Create the calendar rows
    rows = []
    current_day = 1
    for week in range(6):  # Max 6 weeks in a month
        row = []
        for day in range(7):
            if (week == 0 and day < start_day) or current_day > num_days:
                row.append("<td></td>")
            else:
                day_str = f"{year}-{month:02d}-{current_day:02d}"
                event_list = [event for event in events if event["date"] == day_str]
                events_html = "".join(f"<div class='event' data-date='{event['date']}'>{event['title']}</div>" for event in event_list)
                row.append(f"<td>{current_day} {events_html}</td>")
                current_day += 1
        rows.append(f"<tr>{''.join(row)}</tr>")
    return "".join(rows)

# Create a simple calendar view using HTML and JavaScript
calendar_html = f"""
<!DOCTYPE html>
<html>
<head>
    <style>
        .calendar {{
            font-family: Arial, sans-serif;
            margin: 20px;
            padding: 20px;
            border: 1px solid #ddd;
            border-radius: 5px;
        }}
        .calendar .month {{
            text-align: center;
            margin-bottom: 20px;
        }}
        .calendar .month h2 {{
            margin: 0;
        }}
        .calendar table {{
            width: 100%;
            border-collapse: collapse;
        }}
        .calendar table th, .calendar table td {{
            padding: 10px;
            border: 1px solid #ddd;
            text-align: center;
        }}
        .calendar table th {{
            background: #f4f4f4;
        }}
        .calendar .event {{
            background-color: #ff5f00;
            color: #fff;
            padding: 2px 5px;
            border-radius: 3px;
            font-size: 0.75em;
            cursor: pointer;
        }}
    </style>
</head>
<body>
    <div class="calendar">
        <div class="month">
            <h2>{months[st.session_state.current_month - 1]} {st.session_state.current_year}</h2>
        </div>
        <table>
            <thead>
                <tr>
                    <th>Dim</th>
                    <th>Lun</th>
                    <th>Mar</th>
                    <th>Mer</th>
                    <th>Jeu</th>
                    <th>Ven</th>
                    <th>Sam</th>
                </tr>
            </thead>
            <tbody>
                {generate_calendar_body(st.session_state.current_year, st.session_state.current_month, calendar_events)}
            </tbody>
        </table>
    </div>
    <script>
        document.addEventListener('DOMContentLoaded', function() {{
            document.querySelectorAll('.event').forEach(function(element) {{
                element.addEventListener('click', function(event) {{
                    const date = event.target.getAttribute('data-date');
                    window.parent.postMessage({{type: 'calendar_click', date: date}}, '*');
                }});
            }});
        }});
    </script>
</body>
</html>
"""

# Render the HTML calendar
st.components.v1.html(calendar_html, height=600)  # Updated method for embedding HTML
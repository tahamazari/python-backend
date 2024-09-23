from flask import Flask, request, jsonify
from flask_cors import CORS
import pandas as pd
import pandasql as psql

app = Flask(__name__)
CORS(app)

# Load CSV files into DataFrames
events_df = pd.read_csv('events_info.csv')
companies_df = pd.read_csv('company_info.csv')
employees_df = pd.read_csv('company_employee_info.csv')
attendees_df = pd.read_csv('event_attendees.csv')

# Define DataFrames for pandasql
dfs = {
    'events_info': events_df,
    'company_info': companies_df,
    'employee_info': employees_df,
    'event_attendees': attendees_df
}

def build_query(filter_arguments):
    base_query = """
    SELECT DISTINCT ea.*, ca.*, emp.*, ea_att.company_url
    FROM events_info AS ea
    JOIN event_attendees AS ea_att ON ea.event_url = ea_att.event_url
    JOIN company_info AS ca ON ea_att.company_url = ca.company_url
    JOIN employee_info AS emp ON ca.company_url = emp.company_url
    WHERE 1=1
    """
    
    # Define column aliases
    column_aliases = {
        'event_city': 'ea',
        'event_name': 'ea',
        'event_country': 'ea',
        'event_industry': 'ea',  # Added industry alias
        'company_industry': 'ca',
        'company_name': 'ca',
        'company_url': 'ca',
        'company_revenue': 'ca',
        'company_country': 'ca',
        'person_first_name': 'emp',
        'person_last_name': 'emp',
        'person_email': 'emp',
        'person_city': 'emp',
        'person_country': 'emp',
        'person_seniority': 'emp',
        'person_department': 'emp'
    }
    
    # Group filters by key
    grouped_filters = {}
    
    for table, filters in filter_arguments.items():
        for f in filters:
            column = f['key']
            value = f['value']
            if column in grouped_filters:
                grouped_filters[column].append(value)
            else:
                grouped_filters[column] = [value]
    
    # Add conditions to the query
    conditions = []
    
    for column, values in grouped_filters.items():
        table_alias = column_aliases.get(column, 'ea')
        if len(values) == 1:
            conditions.append(f"{table_alias}.{column} = '{values[0]}'")
        else:
            in_clause = "', '".join(values)
            conditions.append(f"{table_alias}.{column} IN ('{in_clause}')")
    
    # Combine base query with conditions
    if conditions:
        base_query += " AND " + " AND ".join(conditions)
    
    return base_query

def split_results(df):
    # Define column prefixes
    events_cols = [col for col in df.columns if col.startswith('event_')]
    companies_cols = [col for col in df.columns if col.startswith('company_')]
    employees_cols = [col for col in df.columns if col.startswith('person_')] + ['company_url']
    
    # Create separate DataFrames for each table
    events_df = df[events_cols].drop_duplicates()
    companies_df = df[companies_cols].drop_duplicates()
    employees_df = df[employees_cols].drop_duplicates()
    
    return events_df, companies_df, employees_df

@app.route('/query2', methods=['POST'])
def get_data():
    filter_arguments = request.json.get('filter_arguments', {})
    
    if not filter_arguments:
        return jsonify({"error": "Invalid input"}), 400
    
    query = build_query(filter_arguments)
    
    try:
        # Execute SQL Query using pandasql
        joined_df = psql.sqldf(query, dfs)
        
        # Split the results into separate tables
        events_df, companies_df, employees_df = split_results(joined_df)
        
        # Convert DataFrames to lists of dictionaries
        events_result = events_df.to_dict(orient='records')
        companies_result = companies_df.to_dict(orient='records')
        employees_result = employees_df.to_dict(orient='records')
        
        return jsonify({
            'events_info': events_result,
            'company_info': companies_result,
            'employee_info': employees_result
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)

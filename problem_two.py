from flask import Flask, request, jsonify
from flask_cors import CORS
import psycopg2
from psycopg2 import sql

app = Flask(__name__)
CORS(app)

# Configure your PostgreSQL database connection
DATABASE_URL = 'postgres://postgres:1234@localhost:5432/byte_genie_2'

def get_db_connection():
    conn = psycopg2.connect(DATABASE_URL)
    return conn

def build_query(filter_arguments, output_columns):
    base_query = """
    SELECT DISTINCT {columns}
    FROM event_company_attributes eca
    JOIN event_attributes ea ON eca.event_url = ea.event_url
    JOIN company_attributes ca ON eca.company_url = ca.company_url
    JOIN employees_attributes emp ON ca.company_url = emp.company_url
    WHERE 1=1
    """
    
    # Define table aliases for each column
    column_aliases = {
        'event_city': 'ea',
        'event_name': 'ea',
        'event_country': 'ea',
        'company_industry': 'ca',
        'company_name': 'ca',
        'company_url': 'ca',
        'company_revenue': 'ca',  # Ensure this is correctly mapped to 'ca'
        'company_country': 'ca',  # Correct table alias for company_country
        'person_first_name': 'emp',
        'person_last_name': 'emp',
        'person_email': 'emp',
        'person_city': 'emp',
        'person_country': 'emp',
        'person_seniority': 'emp',
        'person_department': 'emp'
    }
    
    # Generate the columns string with table aliases
    columns_with_aliases = []
    for col in output_columns:
        table_alias = column_aliases.get(col, 'ea')
        columns_with_aliases.append(f"{table_alias}.{col}")
    
    base_query = base_query.format(
        columns=', '.join(columns_with_aliases)
    )
    
    # Add filters to the query
    conditions = []
    values = []
    
    for column, op, vals in filter_arguments:
        table_alias = column_aliases.get(column, 'ea')
        if op == 'includes':
            placeholders = ', '.join(['%s'] * len(vals))
            conditions.append(sql.SQL("{table_alias}.{column} IN ({placeholders})").format(
                table_alias=sql.Identifier(table_alias),
                column=sql.Identifier(column),
                placeholders=sql.SQL(placeholders)
            ))
            values.extend(vals)
    
    # Combine base query with conditions
    if conditions:
        base_query += " AND " + sql.SQL(" AND ").join(conditions).as_string(get_db_connection().cursor())
    
    return base_query, values

@app.route('/query', methods=['POST'])
def get_data():
    print(request.json.get('filter_arguments', []), request.json.get('output_columns', []))
    filter_arguments = request.json.get('filter_arguments', [])
    output_columns = request.json.get('output_columns', [])
    
    if not filter_arguments or not output_columns:
        return jsonify({"error": "Invalid input"}), 400
    
    query, values = build_query(filter_arguments, output_columns)
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute(query, values)
        rows = cursor.fetchall()
        
        columns = [desc[0] for desc in cursor.description]
        result = [dict(zip(columns, row)) for row in rows]
        
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        cursor.close()
        conn.close()

if __name__ == '__main__':
    app.run(debug=True)

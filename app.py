from flask import Flask, render_template, request, jsonify
from flask_sqlalchemy import SQLAlchemy
import sqlite3
import os
from sqlalchemy import text

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///linkedin_data.db'
db = SQLAlchemy(app)

def get_db_connection():
    conn = sqlite3.connect('linkedin_data.db')
    conn.row_factory = sqlite3.Row
    return conn

@app.route('/')
def index():
    # Get list of tables in the database
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()
    conn.close()
    return render_template('index.html', tables=tables)

@app.route('/table/<table_name>')
def view_table(table_name):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Get table info (columns)
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = cursor.fetchall()
    
    # Get all rows from the table
    cursor.execute(f"SELECT * FROM {table_name}")
    rows = cursor.fetchall()
    
    conn.close()
    return render_template('table.html', 
                         table_name=table_name,
                         columns=columns,
                         rows=rows)

@app.route('/search')
def search():
    query = request.args.get('q', '').lower()
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Get all tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()
    
    results = []
    for table in tables:
        table_name = table[0]
        cursor.execute(f"SELECT * FROM {table_name}")
        rows = cursor.fetchall()
        
        # Get column names
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = cursor.fetchall()
        
        # Check each row for the search query
        for row in rows:
            found = False
            for col in columns:
                col_name = col[1]
                value = row[col_name]
                if value and query in str(value).lower():
                    found = True
                    break
            
            if found:
                result = {
                    'table': table_name,
                    'row': dict(row)
                }
                results.append(result)
    
    conn.close()
    return render_template('search_results.html', results=results, query=query)

@app.route('/relationships')
def show_relationships():
    relationships = {
        'Email Addresses': {
            'related_tables': ['Connections', 'Messages', 'Invitations'],
            'description': 'Central table linking to connections, messages, and invitations'
        },
        'Connections': {
            'related_tables': ['Email Addresses', 'Messages', 'Invitations'],
            'description': 'Core table containing LinkedIn connections with email references'
        },
        'Messages': {
            'related_tables': ['Email Addresses', 'Connections'],
            'description': 'Message history linking to senders and recipients'
        },
        'Invitations': {
            'related_tables': ['Email Addresses'],
            'description': 'Invitation history linked to email addresses'
        },
        'Endorsements': {
            'related_tables': ['Email Addresses'],
            'description': 'Split into given and received endorsements, both linked to email addresses'
        }
    }
    
    return render_template('relationships.html', relationships=relationships)

if __name__ == '__main__':
    app.run(debug=True)

from flask import Flask, render_template_string, request
import pandas as pd
from sqlalchemy import create_engine
import configparser
from datetime import datetime

app = Flask(__name__)

config = configparser.ConfigParser()
config.read('config.ini')

# MySQL connection details
db_user = config['database_1']['user']
db_password = config['database_1']['password']
db_host = config['database_1']['host']
db_port = config['database_1']['port']
db_name = config['database_1']['database']


# Create database engine
database_url = f'mysql+mysqldb://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
engine = create_engine(database_url)

# Read data directly from MySQL
#data = pd.read_sql_table(table_name, con=engine)

#data = data[['SYMBOL',' OPEN_PRICE',' CLOSE_PRICE','PER_CHANGE']]

rows_per_page = 10

@app.route('/', methods=['GET', 'POST'])

def index():

    # Default table (today’s date, or any default you want)
    selected_date = request.args.get('date')
    if selected_date:
    # Convert from 'YYYY-MM-DD' to 'DDMMYYYY'
        try:
            selected_date_obj = datetime.strptime(selected_date, '%Y-%m-%d')
            table_name = selected_date_obj.strftime('%d%m%Y')
        except ValueError:
            table_name = datetime.now().strftime('%d%m%Y')  # fallback
    else:
        # Default to today's date if not provided
        selected_date_obj = datetime.now()
        selected_date = selected_date_obj.strftime('%Y-%m-%d')  # For input field
        table_name = selected_date_obj.strftime('%d%m%Y')

    # Get page number from URL (default to 1)
    page = request.args.get('page', 1, type=int)

    # Calculate offset
    offset = (page - 1) * rows_per_page

    try:
        # SQL query to fetch data (no PER_CHANGE calculation)
        query = f''' SELECT SYMBOL,` OPEN_PRICE`,` CLOSE_PRICE`, PER_CHANGE FROM `{table_name}` ORDER BY PER_CHANGE DESC LIMIT {rows_per_page} OFFSET {offset}'''
        data = pd.read_sql(query, con=engine)

        # Calculate total pages
        total_rows = pd.read_sql(f'SELECT COUNT(*) FROM `{table_name}`', con=engine).iloc[0, 0]
        total_pages = (total_rows // rows_per_page) + (1 if total_rows % rows_per_page > 0 else 0)

        error = None

    except Exception as e:
        data = pd.DataFrame()
        total_pages = 0
        error = f"Error fetching data for {selected_date}: {str(e)}"

    if not data.empty:
        table_html = """
        <table>
            <thead>
                <tr>
                    <th>Symbol</th>
                    <th>Open Price</th>
                    <th>Close Price</th>
                    <th>Percentage Change</th>
                    <th>View</th>
                </tr>
            </thead>
            <tbody>
        """
        for _, row in data.iterrows():
            table_html += f"""
                <tr>
                    <td>{row['SYMBOL']}</td>
                    <td>{row[' OPEN_PRICE']}</td>
                    <td>{row[' CLOSE_PRICE']}</td>
                    <td>{row['PER_CHANGE']}</td>
                    <td><a href="/stock/{row['SYMBOL']}?date={selected_date}" class="view-button">View</a></td>
                </tr>
            """
        table_html += "</tbody></table>"
    else:
        table_html = "No data available"

    # HTML template
    html_template = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Stock Data (4 Columns)</title>
        <style>
            table {border-collapse: collapse; width: 100%;}
            th, td {border: 1px solid #ddd; padding: 8px; text-align: center;}
            th {background-color: #f2f2f2;}
            .error {color: red;}

            .view-button {
            display: inline-block;
            padding: 6px 12px;
            background-color: #4CAF50;
            color: white;
            text-decoration: none;
            border-radius: 5px;
            font-size: 14px;
            }
            .view-button:hover {
                background-color: #45a049;
            }
        </style>
    </head>
    <body>
        <h2>Stock Market Data (4 Columns)</h2>

        <form method="get" action="/">
            <label for="date">Select Date:</label>
            <input type="date" id="date" name="date" required>
            <button type="submit">Go</button>
        </form>

        <br>    

        {% if error %}
            <div class="error">{{ error }}</div>
        {% else %}
            {{ table | safe }}

            <div>
                <h4>Page {{ page }} of {{ total_pages }}</h4>
                {% if page > 1 %}
                    <a href="?date={{ selected_date }}&page={{ page - 1 }}">Previous</a>
                {% endif %}
                {% if page < total_pages %}
                    <a href="?date={{ selected_date }}&page={{ page + 1 }}">Next</a>
                {% endif %}
            </div>
        {% endif %}
    </body>
    </html>
    """

    return render_template_string(
        html_template,
        table=table_html,
        page=page,
        total_pages=total_pages,
        selected_date=selected_date,
        error=error
    )

@app.route('/stock/<symbol>')
def stock_detail(symbol):
    selected_date = request.args.get('date')
    if selected_date:
        try:
            selected_date_obj = datetime.strptime(selected_date, '%Y-%m-%d')
            table_name = selected_date_obj.strftime('%d%m%Y')
        except ValueError:
            return f"Invalid date format: {selected_date}", 400
    else:
        return "No date provided.", 400

    try:
        # Query for that particular stock
        query = f'''SELECT * FROM `{table_name}` WHERE SYMBOL = %s'''
        stock_data = pd.read_sql(query, con=engine, params=[symbol])

        if stock_data.empty:
            return f"No data found for {symbol} on {selected_date}"

        # Render a simple page
        html = stock_data.to_html(index=False)

        return f"""
        <h2>Details for {symbol} on {selected_date}</h2>
        {html}
        <br>
        <a href="/?date={selected_date}">Back to list</a>
        """
    except Exception as e:
        return f"Error: {str(e)}"

if __name__ == '__main__':
    app.run(debug=True)

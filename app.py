from flask import Flask, render_template_string, request, redirect, url_for
import pandas as pd
from sqlalchemy import create_engine
import configparser
from datetime import datetime
import plotly.express as px
import plotly.io as pio

app = Flask(__name__)

config = configparser.ConfigParser()
config.read('config.ini')

# MySQL connection details
db_user = config['database_1']['user']
db_password = config['database_1']['password']
db_host = config['database_1']['host']
db_port = config['database_1']['port']
db_name = config['database_1']['database']
db_name_stock = config['database_2']['database']


# Create database engine
database_url = f'mysql+mysqldb://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
engine = create_engine(database_url)

database_url_stock = f'mysql+mysqldb://{db_user}:{db_password}@{db_host}:{db_port}/{db_name_stock}'
engine_stock = create_engine(database_url_stock)

rows_per_page = 10

@app.route('/', methods=['GET', 'POST'])

def index():

    sort_order = request.args.get('sort', 'desc')

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

    prev_date = (selected_date_obj - pd.Timedelta(days=1)).strftime('%Y-%m-%d')
    next_date = (selected_date_obj + pd.Timedelta(days=1)).strftime('%Y-%m-%d')

    # Get page number from URL (default to 1)
    page = request.args.get('page', 1, type=int)

    # Calculate offset
    offset = (page - 1) * rows_per_page

    try:
        # SQL query to fetch data (no PER_CHANGE calculation)
        query = f''' SELECT SYMBOL,` OPEN_PRICE`,` CLOSE_PRICE`, PER_CHANGE FROM `{table_name}` ORDER BY PER_CHANGE {'ASC' if sort_order == 'asc' else 'DESC'} LIMIT {rows_per_page} OFFSET {offset}'''
        data = pd.read_sql(query, con=engine)

        # Calculate total pages
        total_rows = pd.read_sql(f'SELECT COUNT(*) FROM `{table_name}`', con=engine).iloc[0, 0]
        total_pages = (total_rows // rows_per_page) + (1 if total_rows % rows_per_page > 0 else 0)

        error = None
        holiday_message = None

    except Exception as e:
        data = pd.DataFrame()
        total_pages = 0
        error = f"Error fetching data for {selected_date}: {str(e)}"

        # Check if error is table not found
        if "doesn't exist" in str(e):
            holiday_message = f"{selected_date} is a Holiday — No Trading!"
            error = None  # No need to show error
        else:
            error = f"Error fetching data for {selected_date}: {str(e)}"
            holiday_message = None

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
                    <th>Analysis</th>
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
                    <td><a href="/stock/{row['SYMBOL']}/analysis" class="view-button">View</a></td>
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
        <title>Stock Data</title>
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
            .date-navigation {
            margin-bottom: 15px;
            }
            .date-navigation a {
                margin: 0 10px;
                text-decoration: none;
                font-size: 20px;
                padding: 5px 10px;
                background-color: #008CBA;
                color: white;
                border-radius: 5px;
            }
            .date-navigation a:hover {
                background-color: #007bb5;
            }
            .holiday {
                color: green;
                font-size: 20px;
                margin-top: 20px;
            }
            .top-bar {
                display: flex;
                justify-content: space-between;
                align-items: center;
                margin-bottom: 20px;
            }
            .date-form,
            .search-form {
                display: flex;
                align-items: center;
            }
            .date-form label,
            .search-form label {
                margin-right: 8px;
            }
            .date-form input,
            .search-form input {
                padding: 8px;
                margin-right: 8px;
                border: 1px solid #ccc;
                border-radius: 5px;
            }
            .date-form button,
            .search-form button {
                padding: 8px 16px;
                background-color: #4CAF50;
                color: white;
                border: none;
                border-radius: 5px;
                cursor: pointer;
            }
            .date-form button:hover,
            .search-form button:hover {
                background-color: #45a049;
            }
        </style>
    </head>
    <body>
        <h2>Stock Market Data</h2>

        <div class="date-navigation">
            <a href="/?date={{ prev_date }}">&larr; Previous Day</a>
            <strong>{{ selected_date }}</strong>
            <a href="/?date={{ next_date }}">Next Day &rarr;</a>
        </div>

        <div class="top-bar">
            <form method="get" action="/" class="date-form">
                <label for="date">Select Date:</label>
                <input type="date" id="date" name="date" value="{{ selected_date }}" required>
                <button type="submit">Go</button>
            </form>

            <div style="margin-bottom: 20px;">
                <a href="/?date={{ selected_date }}&sort=asc" class="view-button">Sort by Growth ↑</a>
                <a href="/?date={{ selected_date }}&sort=desc" class="view-button">Sort by Growth ↓</a>
            </div>
            
            <form method="get" action="/stock" class="search-form">
                <input type="text" id="symbol" name="symbol" placeholder="Enter stock symbol..." required>
                <button type="submit">Search</button>
            </form>
        </div>

        <br>   

        {% if holiday_message %}
            <div class="holiday">{{ holiday_message }}</div>
        {% endif %} 

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
        prev_date=prev_date,
        next_date=next_date,
        error=error
    )

@app.route('/stock/<symbol>')

def stock_detail(symbol):
    try:
        # Read data from table named after stock symbol (lowercase)
        stock_table_name = symbol.lower()

        page = request.args.get('page', 1, type=int)
        rows_per_page = 50
        offset = (page - 1) * rows_per_page

        query = f"""
        SELECT 
            ` DATE1`, 
            ` PREV_CLOSE`, 
            ` OPEN_PRICE`, 
            ` CLOSE_PRICE`, 
            ` TTL_TRD_QNTY`, 
            ` NO_OF_TRADES`, 
            `PER_CHANGE`
        FROM `{stock_table_name}`
        ORDER BY STR_TO_DATE(` DATE1`, '%%d-%%b-%%Y') DESC
        LIMIT {rows_per_page} OFFSET {offset}
        """

        # Query all data from stock table
        stock_data = pd.read_sql(query, con=engine_stock)

        total_rows_query = f"SELECT COUNT(*) FROM `{stock_table_name}`"
        total_rows = pd.read_sql(total_rows_query, con=engine_stock).iloc[0, 0]

        total_pages = (total_rows // rows_per_page) + (1 if total_rows % rows_per_page > 0 else 0)


        # Create nice table
        table_html = stock_data.to_html(index=False)

        error = None
    except Exception as e:
        table_html = ""
        total_pages = 0
        page = 1
        error = f"Error fetching data for stock {symbol.upper()}: {str(e)}"

    stock_html_template = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Stock Detail - {{ symbol.upper() }}</title>
        <style>
            table {border-collapse: collapse; width: 100%;}
            th, td {border: 1px solid #ddd; padding: 8px; text-align: center;}
            th {background-color: #f2f2f2;}
            .error {color: red;}
            .back-link, .analysis-button {
                display: inline-block;
                padding: 8px 16px;
                background-color: #008CBA;
                color: white;
                text-decoration: none;
                border-radius: 5px;
                margin-left: 20px; /* Space between buttons */
            }
            .back-link:hover, .analysis-button:hover {
                background-color: #007bb5;
            }
            .pagination {
                margin-top: 20px;
                text-align: center;
            }
            .pagination a {
                margin: 0 5px;
                padding: 8px 12px;
                text-decoration: none;
                background-color: #4CAF50;
                color: white;
                border-radius: 5px;
            }
            .pagination a:hover {
                background-color: #45a049;
            }

            /* Header Flexbox Layout */
            .header-container {
                display: flex;
                justify-content: space-between;
                align-items: center;
                margin-bottom: 20px;
            }

            .header-container h2 {
                margin: 0;
            }

            .content {
                padding-top: 20px;  /* Adding some space above content */
            }
        </style>
    </head>
    <body>

        <div class="button-container">
            <a class="back-link" href="/">Back to Home</a>
            <a class="analysis-button" href="/stock/{{ symbol.upper() }}/analysis">Analysis</a>
        </div>

        <h2>Details for Stock: {{ symbol.upper() }}</h2>

        {% if error %}
            <div class="error">{{ error }}</div>
        {% else %}
            {{ table | safe }}

            <div class="pagination">
                <h4>Page {{ page }} of {{ total_pages }}</h4>
                {% if page > 1 %}
                    <a href="/stock/{{ symbol }}?page={{ page - 1 }}">Previous</a>
                {% endif %}
                {% if page < total_pages %}
                    <a href="/stock/{{ symbol }}?page={{ page + 1 }}">Next</a>
                {% endif %}
            </div>
        {% endif %}

        <br>
        <a class="back-link" href="/">Back to Home</a>
    </body>
    </html>
    """

    return render_template_string(stock_html_template, symbol=symbol, table=table_html, error=error, page=page, total_pages=total_pages)

@app.route('/stock')
def stock_redirect():
    symbol = request.args.get('symbol', '').lower().strip()
    if symbol:
        return redirect(f'/stock/{symbol}')
    else:
        return redirect('/')
    
@app.route('/stock/<symbol>/analysis', methods=['GET'])

def stock_analysis(symbol):
    try:

        symbol = symbol.lower()

        query = f"""
        SELECT 
            ` DATE1`, 
            ` PREV_CLOSE`, 
            ` CLOSE_PRICE`, 
            ` TTL_TRD_QNTY`
        FROM `{symbol}`
        ORDER BY STR_TO_DATE(` DATE1`, '%%d-%%b-%%Y') ASC        
        """

        stock_data = pd.read_sql(query, con=engine_stock)

        #stock_data[' DATE1'] = pd.to_datetime(stock_data[' DATE1'], format='%d-%b-%Y', errors='coerce')

        # Drop rows with invalid dates (if any)
        #stock_data = stock_data.dropna(subset=[' DATE1'])

        if stock_data.empty:
            error_message = f"No data available for stock {symbol}."
            return render_template_string("<h3>{{ error }}</h3>", error=error_message)

        #stock_data = stock_data.sort_values(' DATE1')

        if len(stock_data) < 30:
            error_message = f"Not enough data available for stock {symbol} to perform 30-day analysis."
            return render_template_string("<h3>{{ error }}</h3>", error=error_message)

        # 1. Volume analysis
        last_7_days = stock_data.tail(7)
        last_30_days = stock_data.tail(30)

        if len(last_7_days) < 7 or len(last_30_days) < 30:
            error_message = f"Not enough data for 7-day or 30-day volume analysis for stock {symbol}."
            return render_template_string("<h3>{{ error }}</h3>", error=error_message)
        
        weekly_avg_volume = last_7_days[' TTL_TRD_QNTY'].mean()
        monthly_avg_volume = last_30_days[' TTL_TRD_QNTY'].mean()
        today_volume = stock_data.iloc[-1][' TTL_TRD_QNTY']
        volume_surge_week = today_volume / weekly_avg_volume
        volume_surge_month = today_volume / monthly_avg_volume

        # 2. Price Growth Analysis
        start_price_week = last_7_days.iloc[0][' PREV_CLOSE']
        end_price_week = last_7_days.iloc[-1][' CLOSE_PRICE']
        week_growth = ((end_price_week - start_price_week) / start_price_week) * 100

        start_price_month = last_30_days.iloc[0][' PREV_CLOSE']
        end_price_month = last_30_days.iloc[-1][' CLOSE_PRICE']
        month_growth = ((end_price_month - start_price_month) / start_price_month) * 100

        # 3. High/Low price analysis
        highest_close = last_30_days[' CLOSE_PRICE'].max()
        lowest_close = last_30_days[' CLOSE_PRICE'].min()

        # 4. Up/Down days
        up_days = (last_30_days[' CLOSE_PRICE'] > last_30_days[' PREV_CLOSE']).sum()
        down_days = (last_30_days[' CLOSE_PRICE'] < last_30_days[' PREV_CLOSE']).sum()

        # Return the analysis results
        analysis_result = {
            'weekly_avg_volume': round(weekly_avg_volume, 2),
            'monthly_avg_volume': round(monthly_avg_volume, 2),
            'today_volume': round(today_volume, 2),
            'volume_surge_week': round(volume_surge_week, 2),
            'volume_surge_month': round(volume_surge_month, 2),
            'week_growth': round(week_growth, 2),
            'month_growth': round(month_growth, 2),
            'highest_close': round(highest_close, 2),
            'lowest_close': round(lowest_close, 2),
            'up_days': up_days,
            'down_days': down_days
        }
    except Exception as e:
        error_message = f"Error fetching data for stock {symbol}: {str(e)}"
        return render_template_string("<h3>{{ error }}</h3>", error=error_message)

    # Data visualization using Plotly
    fig1 = px.line(stock_data, x=' DATE1', y=' CLOSE_PRICE', title=f'{symbol.upper()} Price Trend')
    fig1_html = pio.to_html(fig1, full_html=False)

    fig2 = px.bar(stock_data, x=' DATE1', y=' TTL_TRD_QNTY', title=f'{symbol.upper()} Volume Trend')
    fig2_html = pio.to_html(fig2, full_html=False)

    # Return the stock page with the analysis
    return render_template_string('''
        <html>
            <head>
                <style>
                    .home-button {
                        position: fixed;
                        top: 20px;
                        right: 20px;
                        padding: 10px 15px;
                        background-color: #007BFF;
                        color: white;
                        font-size: 16px;
                        border: none;
                        border-radius: 5px;
                        cursor: pointer;
                    }
                    .home-button:hover {
                        background-color: #0056b3;
                    }
                </style>
            </head>
            <body>
                <button class="home-button" onclick="window.location.href='/'">Home</button>
                                  
                <h1>{{ symbol.upper() }} - Stock Analysis</h1>
                <div>
                    <h3>Price Growth (7 Days): {{ analysis_result['week_growth'] }}%</h3>
                    <h3>Price Growth (30 Days): {{ analysis_result['month_growth'] }}%</h3>
                    <h3>Volume Surge (7 Days): {{ analysis_result['volume_surge_week'] }}x</h3>
                    <h3>Volume Surge (30 Days): {{ analysis_result['volume_surge_month'] }}x</h3>
                    <h3>Highest Close in 30 Days: ₹{{ analysis_result['highest_close'] }}</h3>
                    <h3>Lowest Close in 30 Days: ₹{{ analysis_result['lowest_close'] }}</h3>
                    <h3>Up Days: {{ analysis_result['up_days'] }}</h3>
                    <h3>Down Days: {{ analysis_result['down_days'] }}</h3>
                </div>
                <h3>Charts</h3>
                <div>{{ fig1_html | safe }}</div>
                <div>{{ fig2_html | safe }}</div>
            </body>
        </html>
    ''', symbol=symbol, analysis_result=analysis_result, fig1_html=fig1_html, fig2_html=fig2_html)


if __name__ == '__main__':
    app.run(debug=True)

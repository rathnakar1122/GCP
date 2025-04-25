import functions_framework
from google.cloud import bigquery
from google.auth import exceptions

@functions_framework.http
def get_top_rows(request):
    """HTTP Cloud Function to fetch top 10 rows from a BigQuery table"""

    # Extract the table name from the request (query parameter)
    request_args = request.args
    if request_args and 'table' in request_args:
        table_name = request_args['table']
    else:
        return 'Error: Please provide a "table" parameter in the request.', 400

    # Initialize BigQuery client
    client = bigquery.Client()

    # Build the SQL query to fetch top 10 rows from the specified table
    query = f"SELECT * FROM `{table_name}` LIMIT 10"
    
    try:
        # Run the query
        query_job = client.query(query)
        results = query_job.result()  # Fetch the results
        
        # Prepare the output
        rows = [dict(row) for row in results]  # Convert rows to a list of dictionaries
        return {"rows": rows}, 200
    
    except exceptions.GoogleAuthError as e:
        return f"Authentication Error: {e}", 500
    except Exception as e:
        return f"Error: {str(e)}", 500

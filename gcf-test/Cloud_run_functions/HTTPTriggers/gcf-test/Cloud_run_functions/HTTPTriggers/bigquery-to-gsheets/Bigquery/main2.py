import functions_framework
from google.cloud import bigquery

@functions_framework.http
def get_top_rows(request):
    """HTTP Cloud Function to get top 10 rows from a BigQuery table."""
    # Extract table_name from the query parameters
    table_name = request.args.get('table_name')

    if not table_name:
        return 'Error: table_name is required', 400

    # Initialize BigQuery client
    client = bigquery.Client()

    # Construct the SQL query
    query = f"SELECT * FROM `{table_name}` LIMIT 10"

    # Run the query
    query_job = client.query(query)

    # Fetch the results
    results = query_job.result()

    # Convert results to a list of dictionaries
    rows = [dict(row) for row in results]

    # Return the result as a JSON response
    return {'data': rows}

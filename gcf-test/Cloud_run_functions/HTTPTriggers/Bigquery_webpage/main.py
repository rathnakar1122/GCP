import functions_framework
from google.cloud import bigquery
from flask import Response

@functions_framework.http
def http_trigger(request):
    # Parse the request for query parameters
    request_json = request.get_json(silent=True)
    request_args = request.args

    # Check if 'name' is provided in JSON or query parameters
    if request_json and 'name' in request_json:
        name = request_json['name']
    elif request_args and 'name' in request_args:
        name = request_args['name']
    else:
        return Response(
            "<h3>Error:</h3> <p>You have not provided any user name to fetch the data.</p>", 
            status=400, 
            content_type='text/html'
        )

    # Call the function to retrieve customer info
    return retrieve_customer_info(name)

def retrieve_customer_info(name):
    try:
        # Initialize BigQuery client
        client = bigquery.Client()

        # Query to fetch customer details
        query = """
            SELECT * 
            FROM `rathnakar-18m85a0320-hiscox.prod.sample_table`
            WHERE customer_name = @name
        """

        # Configure query parameters
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("name", "STRING", name)
            ]
        )

        # Run the query
        query_job = client.query(query, job_config=job_config)
        rows = query_job.result()

        # Prepare the HTML response
        html_response = f"<h3>Customer Details for: {name}</h3><table border='1'>"
        html_response += "<tr>"

        # Add table headers dynamically
        field_names = [field.name for field in query_job.schema]
        for field_name in field_names:
            html_response += f"<th>{field_name}</th>"
        html_response += "</tr>"

        # Add table rows
        for row in rows:
            html_response += "<tr>"
            for field_name in field_names:
                html_response += f"<td>{row[field_name]}</td>"
            html_response += "</tr>"
        
        html_response += "</table>"

        # Return the HTML response
        return Response(html_response, content_type='text/html')

    except Exception as e:
        # Handle errors and return as HTML response
        return Response(
            f"<h3>Error:</h3><p>{str(e)}</p>", 
            status=500, 
            content_type='text/html'
        )




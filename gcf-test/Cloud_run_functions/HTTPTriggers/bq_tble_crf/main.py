import functions_framework
from google.cloud import bigquery 

@functions_framework.http
def http_trigger(request):
    request_json = request.get_json(silent=True)
    request_args = request.args

    if request_json and 'name' in request_json:
        name = request_json['name']
        return retrieve_customer_info(name)
    elif request_args and 'name' in request_args:
        name = request_args['name']
        return retrieve_customer_info(name)
    else:
        name = 'OOOPS... YOU HAVE NOT PROVIDED ANY USER NAME'
        return 'Hello {}!'.format(name)
    
def retrieve_customer_info(name):
    # bigquery connection
    client = bigquery.Client()
    # Perform a query.
    QUERY = """
    SELECT * FROM `pacific-ethos-441506-a1.cnn_cps_raw_dataset.employee` 
    WHERE CustomerName = @FirstName;
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("FirstName", "STRING", name)
        ]
    )
    query_job = client.query(QUERY, job_config=job_config)  # API request
    rows = query_job.result()  # Waits for query to finish

    if rows.total_rows > 0:
        # Assuming you want to return some information about the customer.
        result = "Found customer(s):\n"
        for row in rows:
            result += f"Customer Name: {row.CustomerName}, Other Info: {row.OtherColumn}\n"  # Customize as needed
        return result
    else:
        return 'No customer found with the name: {}'.format(name)

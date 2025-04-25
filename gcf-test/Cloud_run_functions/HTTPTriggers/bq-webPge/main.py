import functions_framework
from google.cloud import bigquery
from flask import Response

@functions_framework.http
def bq_table_data(request):
    request_json = request.get_json(silent=True)
    request_args = request.args

    if request_json and 'name' in request_json:
        name = request_json['name']
        return retrive_customer_info(name)
    elif request_args and 'name' in request_args:
        name = request_args['name']
        return retrive_customer_info(name)
    else:
        name = 'OOOPS... YOU HAVE NOT PROVIDED ANY USER NAME'
        return f'<p>Hello {name}!</p>'

def retrive_customer_info(name):
    # BigQuery connection
    client = bigquery.Client()
    QUERY = """
        SELECT EmployeeID, FirstName, LastName, Email, PhoneNumber, JobTitle, Department, 
               Salary, HireDate, ManagerID
        FROM pacific-ethos-441506-a1.cnn_cps_raw_dataset.employee
        WHERE FirstName = @FirstName;
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("FirstName", "STRING", name)
        ]
    )
    query_job = client.query(QUERY, job_config=job_config)  # API request
    rows = query_job.result()  # Waits for query to finish

    # Generate HTML response
    html = """
    <html>
        <head>
            <title>Customer Information</title>
            <style>
                table {
                    width: 80%;
                    border-collapse: collapse;
                    margin: 20px auto;
                    font-family: Arial, sans-serif;
                }
                th, td {
                    border: 1px solid #ddd;
                    padding: 8px;
                    text-align: center;
                }
                th {
                    background-color: #f4f4f4;
                    font-weight: bold;
                }
                tr:nth-child(even) {
                    background-color: #f9f9f9;
                }
            </style>
        </head>
        <body>
            <h2 style="text-align:center;">Customer Information</h2>
            <table>
                <thead>
                    <tr>
                        <th>EmployeeID</th>
                        <th>FirstName</th>
                        <th>LastName</th>
                        <th>Email</th>
                        <th>PhoneNumber</th>
                        <th>JobTitle</th>
                        <th>Department</th>
                        <th>Salary</th>
                        <th>HireDate</th>
                        <th>ManagerID</th>
                    </tr>
                </thead>
                <tbody>
    """

    # Append rows to the table
    for row in rows:
        html += f"""
        <tr>
            <td>{row.EmployeeID}</td>
            <td>{row.FirstName}</td>
            <td>{row.LastName}</td>
            <td>{row.Email}</td>
            <td>{row.PhoneNumber}</td>
            <td>{row.JobTitle}</td>
            <td>{row.Department}</td>
            <td>{row.Salary}</td>
            <td>{row.HireDate}</td>
            <td>{row.ManagerID}</td>
        </tr>
        """

    html += """
                </tbody>
            </table>
        </body>
    </html>
    """

    return Response(html, content_type='text/html')
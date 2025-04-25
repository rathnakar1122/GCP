from flask import Flask, request, jsonify
from google.cloud import bigquery

app = Flask(__name__)

@app.route("/", methods=["GET"])
def fetch_top_records():
    """
    Cloud Run function to fetch and return the top 3 records from a BigQuery table.

    Query Parameters:
        table_name: Fully qualified BigQuery table name in the format `project.dataset.table`.

    Returns:
        JSON response with top 3 records or an error message.
    """
    table_name = request.args.get("table_name")
    
    if not table_name:
        return jsonify({"error": "Please provide a table name as the 'table_name' query parameter."}), 400

    try:
        # Initialize the BigQuery client
        client = bigquery.Client()

        # Query to fetch the top 3 records
        query = f"SELECT * FROM `{table_name}` LIMIT 3"

        # Execute the query
        query_job = client.query(query)
        results = query_job.result()

        # Fetch results
        records = [dict(row) for row in results]

        if records:
            return jsonify({"table_name": table_name, "top_3_records": records}), 200
        else:
            return jsonify({"message": f"No records found in table '{table_name}'."}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# Entry point for Cloud Run
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)

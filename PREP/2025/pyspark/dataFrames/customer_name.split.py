#import pandas as pd
#print(dir(pd))

#importnent :
#read_csv, read_excel, read_gbq, read_json, read_orc, read_parquet, read_sql, read_sql_query, read_sql_table, read_xml

import pandas as pd

# Input and output file paths
input_file = r"gs://crf-event/customer.csv"
output_file_path = r"gs://crf-event/customer_output.csv"

# Read the CSV file into a DataFrame
dataframe = pd.read_csv(input_file)

# Remove leading/trailing spaces from column names (in case there's an issue with extra spaces)
dataframe.columns = dataframe.columns.str.strip()

# Debug: Print the column names to check if 'cust_name' exists
print("Columns in the CSV file:", dataframe.columns)

# Check if 'cust_name' exists in the DataFrame
if 'cust_name' in dataframe.columns:
    # Split the 'cust_name' column into 'first_name', 'second_name', and 'last_name'
    dataframe[['first_name', 'second_name', 'last_name']] = dataframe['cust_name'].str.split(expand=True, n=2)

    # Drop the original 'cust_name' column
    dataframe = dataframe.drop(columns=['cust_name'])

    # Save the updated DataFrame to a new CSV file
    dataframe.to_csv(output_file_path, index=False)
    print(f"File saved successfully at: {output_file_path}")
else:
    print("Error: 'cust_name' column not found in the input CSV file. Please check the file.")

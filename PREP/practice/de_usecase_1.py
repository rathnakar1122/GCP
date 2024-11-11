# import pandas as pd
# import logging

# logging.basicConfig(level=logging.ERROR)

# # 1. Read the CSV file into a Pandas DataFrame.
# file_path = r'C:\ENV\Virual_Env\DE_class\regular_use_cases\sales_data.csv'
# df = pd.read_csv(file_path)

# # Strip any whitespace from column names
# df.columns = df.columns.str.strip()

# # Calculate the total sales for each product based on the QuantitySold and Price columns.
# df['total_sales'] = df['QuantitySold'] * df['Price']

# print(df)


import pandas as pd

file_path = r'C:\ENV\PREP\DE_class\regular_use_cases\sales_data.csv'
try:
    df = pd.read_csv(file_path)
    print(df)
except FileNotFoundError:
    print(f"Error: The file at '{file_path}' was not found.")
except pd.errors.EmptyDataError:
    print("Error: The file is empty.")
except pd.errors.ParserError:
    print("Error: There was an issue parsing the file.")
except Exception as e:
    print(f"An unexpected error occurred: {e}")

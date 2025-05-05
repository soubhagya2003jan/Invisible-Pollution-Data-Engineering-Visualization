from tableauhyperapi import HyperProcess, Connection, Telemetry
import pandas as pd

# Path to your .hyper file
hyper_file = "TheAirWeBreathe.hyper"

# Choose the schema you want to use (either "public" or "Extract")
schema_name = "public"  # or "Extract"

# Start HyperProcess
with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
    # Connect to the .hyper file
    with Connection(endpoint=hyper.endpoint, database=hyper_file) as connection:
        # Get the table names from the specified schema
        table_names = connection.catalog.get_table_names(schema=schema_name)
        
        # If there are tables, proceed to extract data from the first one
        if table_names:
            table_name = table_names[0]  # Pick the first table
            query = f"SELECT * FROM {schema_name}.{table_name}"  # Specify schema and table
            rows = connection.execute_list_query(query)
            
            # Get column names
            column_names = [col.name for col in connection.catalog.get_table_definition(table_name).columns]
            
            # Convert to DataFrame
            df = pd.DataFrame(rows, columns=column_names)
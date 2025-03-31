# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "f80cb5d5-5742-4adc-a99c-f30bee868bb7",
# META       "default_lakehouse_name": "Housing_Bronze",
# META       "default_lakehouse_workspace_id": "85987632-562b-4a33-86f0-957bbdc9bc53",
# META       "known_lakehouses": [
# META         {
# META           "id": "f80cb5d5-5742-4adc-a99c-f30bee868bb7"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# <div style = "background:black;color:gold;font-family:serif;text-align:center;"><h1>Fred API Bronze Layer Processing </h1><h3> By Brandon Hart</h3> <h3> Alchemy Analytics & AI</h3></div>

# MARKDOWN ********************

# <h2> Import Dependencies </h2>

# CELL ********************

import sempy.fabric as fabric
import requests
import json
from datetime import datetime, timedelta,timezone

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# <h2> Create and Call a Function to Determine Environment</h2>

# CELL ********************

def get_environment() -> str:
    """
    This function finds the current workspace name and returns the substring before an underscore which by convention is the current enviornment.

    Args:
        None.
      
    Returns:
        str: The substring before an underscore of the current workspace which by convention is the current enviornment.

    Raises:
        N/A.

    """
    #Determine the environment

    # Get the id of this notebooks workspace
    workspace_id = fabric.get_notebook_workspace_id()  
    # You can also get this with fabric.get_workspace_id()

    # From all workspaces, filter by Id and then just get the Name
    workspaces = fabric.list_workspaces()
    workspace_row = workspaces[workspaces.Id == workspace_id]
    workspace_name = workspace_row['Name'][0]
    # Find the location of the underscore and return prefix
    n = workspace_name.index("_")
    return workspace_name[:n].lower()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

env = get_environment()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# <h2> Get Current Date to use as parameters and Filename timestamp </h2>

# MARKDOWN ********************


# CELL ********************

end_dt = datetime.now(tz=timezone.utc)
start_dt = (end_dt - timedelta(weeks = 4))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(start_dt.strftime("%x"),end_dt.strftime("%x"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

kv_uri = f"https://{env}-alchemy-kv.vault.azure.net/"
endpoint = "https://api.stlouisfed.org/fred/series/observations"
api_key = mssparkutils.credentials.getSecret(kv_uri, 'Fred-API')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# <h1>  Establish Parameters with Default Values </h1> 

# MARKDOWN ********************


# PARAMETERS CELL ********************

start = start_dt.strftime("%Y-%m-%d") # YYYY-MM-DD
end = end_dt.strftime("%Y-%m-%d") # YYYY-MM-DD
series_id = "MORTGAGE30US"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# <H1> Make API Call </h1>

# CELL ********************

params = {   "series_id":series_id
            , "observation_start":start
            , "observation_end":end
            , "api_key":api_key
            ,"file_type":"json"}
params

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

response = requests.get(endpoint,params=params)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# <h1> Save Results to File </h1>

# CELL ********************

timestamp = end_dt.strftime("%Y%m%d%H%M%S") # YYYY-MM-DD

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check if the request was successful
if response.status_code == 200:
    # Get the JSON response
    data = response.json()
    
    # Specify the file name (and path if needed)
    file_path = f'/lakehouse/default/Files/{series_id}_{timestamp}.json'
    
    # Open the file in write mode ('w') and save the JSON data
    with open(file_path, 'w') as file:
        # The `json.dump` method serializes `data` as a JSON formatted stream to `file`
        # `indent=4` makes the file human-readable by adding whitespace
        json.dump(data, file, indent=4)
        
    print(file_path)
else:
    print("Failed to fetch data. Status code:", response.status_code)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mssparkutils.notebook.exit(str({"file_path":file_path}))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

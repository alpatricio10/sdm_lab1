# SDM Lab 1
This is the codebase for Semantic Data Management Lab Assignment 1 at UPC. It was developed by Adrian Patricio and Olha Baliasina.

## Important Files
- PartA.2_BaliasinaPatricio_Extraction_Async_Fetching_Fields.py: This script extracts all the needed information from the Semantic Scholar API
- PartA.2_BaliasinaPatricio_Extraction_References.py: This script extracts all the citations of the papers from Semantic Scholar API.
- PartA.2_BaliasinaPatricio_Preprocessing.py: This script processes the extracted data, generates missing information, and converts them into a CSV file suited for Neo4j LOAD CSV.
- PartA.2_BaliasinaPatricio_Upload.py: This script creates a graph database and uploads the data using the processed CSV files from the previous stage.
- PartA.3_BaliasinaPatricio.py: This script extends the database to add the additional information (i.e. review scores and affiliations).
- PartB_BaliasinaPatricio.py: Ths script contains the queries for Part B.
- PartC_BaliasinaPatricio.py: This script contains the queries for Part C - Recommender.
- PartD_BaliasinaPatricio.py: This script contains the graph algorithm queries for Part D.

## How to Run


## Notes
- The /data folder contains the output after the preprocessing step. This can then be used to upload the data into the graph database. 
- The /src/final_output folder contains the output after extracting all the needed information and citations from Semantic Scholar.
- The docker-compose file and GDS executable are added here for your reference. This was used to run Neo4J in WSL. Update the docker-compose file to the appropriate volume paths before running.

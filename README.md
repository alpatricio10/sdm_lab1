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
- Run PartA.2_BaliasinaPatricio_Extraction_References.py to retrieve a list of original papers (~560) with paperIDs, keywords, venue type and references. This outputs a CSV file called papers_combined.csv. This must be placed in the same directory as PartA.2_BaliasinaPatricio_Extraction_Async_Fetching_Fields.py, which should then be run to extract all the other required fields for papers. This step outputs 4 CSV files containing all the required information for the graph.
- Run PartA.2_BaliasinaPatricio_Preprocessing.py to process the data for loading into Neo4J. The CSV files from the previous step should be placed inside a final_output directory. This outputs separate CSVs corresponding to each node and edge to be uploaded into the graph.
- The CSV files from the previous step must be placed in the /import directory of Neo4J. In our case, we used a docker image where the /import directory is located within the src folder. Run PartA.2_BaliasinaPatricio_Upload.py to create and populate the graph database.
- Run PartA.3_BaliasinaPatricio.py to extend the graph with the additional required information in Part A.3.

## Notes
- The /data folder contains the output after the preprocessing step. This can then be used to upload the data into the graph database. 
- The /src/final_output folder contains the output after extracting all the needed information and citations from Semantic Scholar.
- The docker-compose file and GDS executable are added here for your reference. This was used to run Neo4J in WSL. Update the docker-compose file to the appropriate volume paths before running.
- Result samples are located in the /results directory.

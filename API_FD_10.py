import requests
from google.cloud import bigquery
import json
from pathlib import Path
import datetime

# ***********************************************************************************
# Environment variables, Filters and Folders
# ***********************************************************************************
# Set Freshdesk PROD environment variables
api_key = '7DdGXEYYL4ndy752'
domain = 'springeronlineservice'
password = 'x'

#GBQ: Set Project-Dataset-Table
project_id = 'bi-team-189611'
dataset_id = 'freshdesk'
table_id = 'tickets5'

client_ref = bigquery.Client(project=project_id)
dataset_ref = client_ref.dataset(dataset_id)
table_ref = dataset_ref.table(table_id)

# Filters
# Calculate D-1 + Time
x = datetime.datetime.now()
update_since_day = str(x.year) + '-' + str(x.month) + '-' + str(x.day - 1)
update_since_time = 'T16:00:01Z'
updated_since = update_since_day + update_since_time

# updated_since = '2019-11-22T00:00:01Z'  #Set Date&Time

# Set File paths/names
folder_fd = Path('C:/Dev/freshdesk/fd_out')  # This folder contains files extracted from Freshdesk
folder_gbq = Path('C:/Dev/freshdesk/gbq_in')  # This folder contains files ready to be loaded into GBQ

gbq_folder = "gs://freshdesk_gbq/"

#GBQ Table schema
tickets_schema = [
    bigquery.SchemaField('id',  'INTEGER', description='Ticket Id'),
    bigquery.SchemaField('created_at', 'TIMESTAMP', description='Ticket creation date'),
    bigquery.SchemaField('updated_at', 'TIMESTAMP', description='Ticket updated date'),
    bigquery.SchemaField('company_id', 'INTEGER', description='Company Id'),
    bigquery.SchemaField('priority', 'INTEGER', description='Priority'),
    bigquery.SchemaField('status', 'INTEGER', description='Status')
]

'''
# ***********************************************************************************
# This function convert a JSON file into a 'New line' JSON file requested by BigQuery
# ***********************************************************************************
# A source json file f_fd is converted into a target 'newline' JSON file
def Convert_JSON_to_newlineJSON(f_fd_in, j):
    with open(f_fd_in, "r", encoding="utf-8") as source_file:
        data = json.load(source_file)

    result = [json.dumps(record) for record in data]

    target_file_name = 'gbq_' + update_since_day + '_p_' + str(j) + '.json'
    f_gbq = folder_gbq / target_file_name
    with open(f_gbq, 'w') as target_file:
        for i in result:
            target_file.write(i + '\n')
    source_file.close()
    target_file.close()

# ***********************************************************************************
# Main
# ***********************************************************************************
print(x, ' - Phase1: Start extracting from Freshdesk')
i = 1  # Initialise page number
content_lenght = 10  # used to identify empty pages

# This loop use a Freshdesk API to extract previous day modified tickets page by page (one file for each page)
# The max number of daily pages 'i' is set to 100.
# The second condition is used to stop the loop when the latest file extracted is empty (content_lenght < 5 char)
while i <= 100 and content_lenght > 5:
    file_name = 'fd_' + update_since_day + '_p_' + str(i) + '.json'
    f_fd = folder_fd / file_name  # Full file name
    f_fd_out = open(f_fd, 'a', encoding="utf-8")  # 'a' = append mode, encoding utf-8 to avoid unicode related issues
    r = requests.get('https://' + domain + '.freshdesk.com/api/v2/tickets?per_page=100&page=' + str(
        i) + '&updated_since=' + updated_since, auth=(api_key, password))

    # The requests.get() returns a Response object. If no error is detected, the downloaded text will be available in r.text
    if r.status_code == 200:
        response = r.text  # The r.text holds a string of JSON formatted data.
        content_lenght = len(r.text)
        f_fd_out.write(response)  # Save the content into the output file (json).
    else:
        print("Failed to read ticket, errors are displayed below,")
        response = json.loads(r.content)
        print(response["errors"])
        print("x-request-id : " + r.headers['x-request-id'])
        print("Status Code : " + r.status_code)
        f_fd_out.close()
        break
    f_fd_out.close()
    i = i + 1

f_fd_out.close()
print(x, ' - Phase 1: Finished extracting from Freshdesk. Number of pages extracted: ', i - 2)
# print(x, ' - Tickets extracted successfully. Check files here: ', folder_fd)
print(x, ' - Phase 2: Start converting JSON file to nJSON')

# This loop converts each JSON file into a Newline JSON file
# The loop ends when j = number of pages extracted by the previous loop (i-2)
j = 1
while j <= i - 2:
    file_name = 'fd_' + update_since_day + '_p_' + str(j) + '.json'
    f_fd = folder_fd / file_name
    Convert_JSON_to_newlineJSON(f_fd, j)
    j = j + 1

print(x, ' - Phase 2: Finished converting JSON file into Newline JSON. Check files here: ', folder_gbq)
'''
print(x, ' - Phase 3: Start loading JSON files into GBQ')
# This loop loads each JSON file into GBQ (ticket table)
# The loop ends when j = number of pages extracted by the previous loop (i-2)
tickets_table = bigquery.Table(table_ref, schema=tickets_schema)
job_config = bigquery.LoadJobConfig()
job_config.schema = tickets_schema
job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
job_config.ignore_unknown_values = True
job_config.max_bad_records = 101
i = 69
j = 1
while j <= i - 2:
    json_file_in = 'gbq_' + update_since_day + '_p_' + str(j) + '.json'
    print(json_file_in)
    uri = gbq_folder + json_file_in
    load_job = client_ref.load_table_from_uri(
      uri,
      dataset_ref.table(table_id),
      location="EU",  # Location must match that of the destination dataset.
      job_config=job_config,
    )  # API request
    load_job.result()  # Waits for table load to complete.
    j = j + 1

destination_table = client_ref.get_table(dataset_ref.table(table_id))

print(x, " - Phase 3: Finished Loading into GBQ {} rows.".format(destination_table.num_rows))
print('Test')

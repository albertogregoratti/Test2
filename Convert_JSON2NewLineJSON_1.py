import json
from pathlib import Path

data_folder = Path("C:/Dev/freshdesk/")
json_file_in = data_folder / "Test13.json"
json_file_out = data_folder / "Test13_out.json"

'''  
data = [json.loads(line) for line in open(json_file_in, 'r')]

result = [json.dumps(record) for record in data]

with open(json_file_out, 'w', encoding="utf-8") as write_file:
    for i in result:
        write_file.write(i+'\n')
'''
with open(json_file_in, "r", encoding="utf-8") as read_file:
    data = json.load(read_file)
    
result = [json.dumps(record) for record in data]

with open(json_file_out, 'w') as write_file:
    for i in result:
        write_file.write(i+'\n')

print('Finished!')

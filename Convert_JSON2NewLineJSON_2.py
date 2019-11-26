import json
from pathlib import Path
import json
from pathlib import Path

data_folder = Path("C:/Dev/freshdesk/")
json_file_in = data_folder / "Test10.json"
json_file_out = data_folder / "Test10_out.json"

#with open(json_file_in, "r", encoding="utf-8") as read_file:
#    data = json.load(read_file)
    
with open(json_file_in, "r", encoding="utf-8") as read_file:
    data = json.load(read_file)
result = [json.dumps(record) for record in data]
with open(json_file_out, 'w') as obj:
    for i in result:
        obj.write(i+'\n')
print('Finished!')


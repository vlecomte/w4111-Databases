# How my code works and why

## CSVDataTable
- find: I just go through all the rows and for each one, check whether it matches the template and add it to the results
- delete: replace the row list with a filtered version
- update: for each row, check if it matches the template and update the appropriate fields if that is the case
- insert: literally just add a row to the list
- *_by_key: just call the *_by_template version with a template that assigns values to each of the fields in the key
- project (in "other_utils.py"): projects a full row down to the desired output fields

## RDBDataTable
- find, delete, update, insert: just write out the SQL query using the helpers in "dbutils.py", then execute it
- _tuple_to_dict: transform the raw tuple result of SELECT into a dictionary using the field names obtained through
                  SHOW COLUMNS
- *_by_key: see CSVDataTable

## Tests
Exactly identical for the two versions, only the table being used changes. Power of polymorphism!
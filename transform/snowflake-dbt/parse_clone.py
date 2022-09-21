import json
from fire import Fire

ci_project_db = 'CI_CLONING_TESTING'

def parse_json(input):
    # distinct and convert string into list based on lines
    input_set = set([i for i in input.split('\n')])
    input_list = list(input_set)

    for i in input_list:
        i = i.replace('"', "")
        output_db = i.replace("PARMSTRONG", ci_project_db)
        clone_statement = f'CREATE TABLE {output_db} CLONE {i} COPY GRANTS;'
        clone_statement = f'CREATE TABLE {output_db} '#CLONE {i} COPY GRANTS;'
        print(clone_statement)

    # table_name = f"{input.get('database')}.{input.get('schema')}.{input.get('name')}"
    # json_string = json.dumps(table_name)
    # with open('json_data.json', 'w') as outfile:
    #     json.dump(json_string, outfile)


def create_schemas(input):
    # distinct and convert string into list based on lines
    input_set = set([i for i in input.split('\n')])
    input_list = list(input_set)

    for i in input_list:
        i = i.replace('"', "")

        output_db = i.replace("PARMSTRONG", ci_project_db)
        i = i.replace('PARMSTRONG_', '')
        clone_statement = f'CREATE OR REPLACE SCHEMA {output_db} CLONE {i};'
        print(clone_statement)

if __name__ == "__main__":
    Fire(
        {
            "parse_json": parse_json,
            "create_schemas": create_schemas,
        }
    )



""" 
studentsList = []
with open('output.json') as f:
    for jsonObj in f:
        studentDict = json.loads(jsonObj)
        studentsList.append(studentDict)

for student in studentsList:
    print(f"{student.get('database')}.{student.get('schema')}.{student.get('name')}")
"""
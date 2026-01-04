import json


def main():

    schema = json.load(open('tpcds.dbschema.json'))
    for table in schema['tables']:
        table_name = table['name']
        keys = []
        if 'primary key' in table:
            pk_columns = table['primary key']
            if 'column' in pk_columns:
                keys.append(pk_columns['column'])
            if 'columns' in pk_columns:
                keys.extend(pk_columns['columns'])

        print(f"'{table_name}': {keys},")


if __name__ == "__main__":
    main()
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

        if 'foreign keys' in table:
            for fk in table['foreign keys']:
                if 'column' in fk:
                    keys.append(fk['column'])
                if 'columns' in fk:
                    keys.extend(fk['columns'])


        # remove keys that contain date_sk and time_sk columns
        keys = [key for key in keys if 'date_sk' not in key and 'time_sk' not in key]
        print(f"'{table_name}': {keys},")


if __name__ == "__main__":
    main()
import json

import duckdb

TEST_DIR = '/Users/paul/workspace/OLAPBench/test'

TPCH_PATH = '/duckdb/tpchSf1.csv'
TPCH_STRING_PATH = '/duckdb/tpchUuidSf1.csv'


def iterate_children(plan, depth=0):
    indent = '  ' * depth
    operator_type = plan.get('_label', 'Unknown')
    print(f"{indent}- {operator_type}")
    for child in plan.get('_children', []):
        iterate_children(child, depth + 1)

def main():
    data = duckdb.sql(f"SELECT plan FROM '{TEST_DIR}{TPCH_PATH}' OFFSET 1;")
    for (plan,) in data.fetchall():
        plan_parsed = json.loads(plan)
        plan = plan_parsed['queryPlan']
        # pretty print the plan
        pretty_plan = json.dumps(plan, indent=2)
        # save to example.json
        with open('example.json', 'w') as f:
            f.write(pretty_plan)
        print("Query Plan:")
        iterate_children(plan)
        exit()








if __name__ == "__main__":
    main()
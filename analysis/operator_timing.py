import json
import duckdb
import csv
import os
from collections import defaultdict

TEST_DIR = '/Users/paul/workspace/OLAPBench/test'

TPCH_PATH = '/duckdb/tpchSf1IdType_int64_sorted.csv'
# TPCH_STRING_PATH = '/duckdb/tpchSf1IdType_int64_random.csv'
TPCH_STRING_PATH = '/duckdb/tpchSf1IdType_uuid_v4.csv'
OUTPUT_CSV = 'analysis/operator_timings.csv'


def extract_operator_timings(plan, timings):
    """
    Recursively traverse the plan tree and extract operator timings.

    Args:
        plan: The query plan node
        timings: Dictionary to accumulate timings by operator type
    """
    if not isinstance(plan, dict):
        return

    # Get the operator label
    operator_label = plan.get('_label', 'Unknown')

    # Parse system_representation to get timing info
    attrs = plan.get('_attrs', {})
    if not isinstance(attrs, dict):
        return

    system_repr = attrs.get('system_representation', '[]')

    try:
        # Parse system_representation if it's a string
        if isinstance(system_repr, str):
            system_data = json.loads(system_repr)
        else:
            system_data = system_repr

        if isinstance(system_data, list) and len(system_data) > 0:
            operator_info = system_data[0]
            if isinstance(operator_info, dict):
                operator_type = operator_info.get('operator_type', operator_info.get('operator_name', operator_label))
                operator_timing = operator_info.get('operator_timing', 0.0)
                operator_type += f" ({operator_label})"
                # Accumulate timing for this operator type
                if operator_timing > 0:
                    timings[operator_type].append(operator_timing)
    except (json.JSONDecodeError, KeyError, IndexError, TypeError) as e:
        pass

    # Recursively process children
    children = plan.get('_children', [])
    if isinstance(children, list):
        for child in children:
            extract_operator_timings(child, timings)


def analyze_operator_timings(csv_path, dbms_filter=None):
    """
    Analyze operator timings for a given CSV file, optionally filtered by DBMS.

    Args:
        csv_path: Path to the CSV file
        dbms_filter: Optional DBMS name to filter by (e.g., 'duckdb', 'clickhouse')

    Returns:
        Dictionary with operator timing statistics
    """
    all_timings = defaultdict(list)
    query_count = 0

    try:
        # Build query with optional DBMS filter
        where_clause = "WHERE state = 'success'"
        if dbms_filter:
            where_clause += f" AND dbms = '{dbms_filter}'"

        data = duckdb.sql(f"SELECT plan, query FROM '{TEST_DIR}{csv_path}' {where_clause};")

        for row in data.fetchall():
            plan_str = row[0] if row else None
            if not plan_str:
                continue

            try:
                plan_parsed = json.loads(plan_str)
                plan = plan_parsed.get('queryPlan') if isinstance(plan_parsed, dict) else None

                if plan:
                    query_timings = defaultdict(list)
                    extract_operator_timings(plan, query_timings)

                    # Add to overall timings
                    for op_type, timings in query_timings.items():
                        all_timings[op_type].extend(timings)

                    query_count += 1
            except (json.JSONDecodeError, KeyError) as e:
                continue
    except Exception as e:
        print(f"Error processing {csv_path}: {e}")
        return None

    # Calculate statistics
    stats = {}
    for op_type, timings in all_timings.items():
        if timings:
            total_time = sum(timings)
            avg_time = total_time / len(timings)
            min_time = min(timings)
            max_time = max(timings)
            count = len(timings)

            stats[op_type] = {
                'total_time': total_time,
                'avg_time': avg_time,
                'min_time': min_time,
                'max_time': max_time,
                'count': count,
                'percentage': 0.0  # Will calculate after
            }

    # Calculate percentages
    total_all_time = sum(s['total_time'] for s in stats.values())
    if total_all_time > 0:
        for op_type in stats:
            stats[op_type]['percentage'] = (stats[op_type]['total_time'] / total_all_time) * 100

    return {
        'dbms': dbms_filter,
        'query_count': query_count,
        'operator_stats': stats,
        'total_time': total_all_time
    }


def get_available_dbms(csv_path):
    """
    Get list of unique DBMS values from a CSV file.

    Args:
        csv_path: Path to the CSV file

    Returns:
        List of unique DBMS names
    """
    try:
        data = duckdb.sql(f"SELECT DISTINCT dbms FROM '{TEST_DIR}{csv_path}' WHERE state = 'success' ORDER BY dbms;")
        return [row[0] for row in data.fetchall()]
    except Exception as e:
        print(f"Error getting DBMS list from {csv_path}: {e}")
        return []


def extract_all_operator_timings_to_csv(output_path):
    """
    Extract all operator timings from both CSV files and write to a single CSV.

    Output CSV format:
    system_name, benchmark, query, operator, operator_time
    """
    datasets = [
        (TPCH_PATH, 'TPCH_SF1'),
        (TPCH_STRING_PATH, 'TPCH_UUID_SF1'),
    ]

    rows = []

    for csv_path, benchmark_name in datasets:
        try:
            data = duckdb.sql(f"SELECT dbms, query, plan FROM '{TEST_DIR}{csv_path}' WHERE state = 'success';")

            for row in data.fetchall():
                system_name = row[0]
                query_name = row[1]
                plan_str = row[2]

                if not plan_str:
                    continue

                try:
                    # Parse the plan
                    plan_parsed = json.loads(plan_str)
                    plan = plan_parsed.get('queryPlan') if isinstance(plan_parsed, dict) else None

                    if plan:
                        # Extract all operators and their timings
                        operator_timings = []
                        extract_operator_timings_with_details(plan, operator_timings)

                        # Add each operator timing as a row
                        for operator_type, operator_time in operator_timings:
                            rows.append({
                                'system_name': system_name,
                                'benchmark': benchmark_name,
                                'query': query_name,
                                'operator': operator_type,
                                'operator_time': operator_time
                            })

                except (json.JSONDecodeError, KeyError) as e:
                    continue

        except Exception as e:
            print(f"Error processing {csv_path}: {e}")
            continue

    # Write to CSV
    if rows:
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        with open(output_path, 'w', newline='') as csvfile:
            fieldnames = ['system_name', 'benchmark', 'query', 'operator', 'operator_time']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            writer.writeheader()
            for row in rows:
                writer.writerow(row)

        print(f"\nExported {len(rows)} operator timing records to {output_path}")
    else:
        print("\nNo operator timing data to export.")


def extract_operator_timings_with_details(plan, timings_list):
    """
    Recursively traverse the plan tree and extract operator timings as a list.

    Args:
        plan: The query plan node
        timings_list: List to accumulate (operator_type, timing) tuples
    """
    if not isinstance(plan, dict):
        return

    # Get the operator label
    operator_label = plan.get('_label', 'Unknown')

    # Parse system_representation to get timing info
    attrs = plan.get('_attrs', {})
    if not isinstance(attrs, dict):
        return

    system_repr = attrs.get('system_representation', '[]')

    try:
        # Parse system_representation if it's a string
        if isinstance(system_repr, str):
            system_data = json.loads(system_repr)
        else:
            system_data = system_repr

        if isinstance(system_data, list) and len(system_data) > 0:
            operator_info = system_data[0]
            if isinstance(operator_info, dict):
                operator_type = operator_info.get('operator_type', operator_info.get('operator_name', operator_label))
                operator_timing = operator_info.get('operator_timing', 0.0)
                operator_type += f" ({operator_label})"

                # Add to list if timing is positive
                if operator_timing > 0:
                    timings_list.append((operator_type, operator_timing))
    except (json.JSONDecodeError, KeyError, IndexError, TypeError) as e:
        pass

    # Recursively process children
    children = plan.get('_children', [])
    if isinstance(children, list):
        for child in children:
            extract_operator_timings_with_details(child, timings_list)


def print_operator_stats(results):
    """
    Print operator timing statistics in a readable format.
    """
    dbms = results['dbms'] or 'Unknown'
    query_count = results['query_count']
    stats = results['operator_stats']
    total_time = results['total_time']

    print(f"\nQueries Analyzed: {query_count}")
    print(f"Total Time Across All Operators: {total_time:.6f} seconds")
    print(f"{'-'*80}")

    # Sort by total time descending
    sorted_ops = sorted(stats.items(), key=lambda x: x[1]['total_time'], reverse=True)

    print(f"{'Operator Type':<30} {'Total (s)':<12} {'Avg (s)':<12} {'Min (s)':<12} {'Max (s)':<12} {'Count':<8} {'%':<8}")
    print(f"{'-'*110}")

    for op_type, stat in sorted_ops:
        print(f"{op_type:<30} {stat['total_time']:<12.6f} {stat['avg_time']:<12.6f} "
              f"{stat['min_time']:<12.6f} {stat['max_time']:<12.6f} {stat['count']:<8} {stat['percentage']:<8.2f}")


def compare_operator_performance(baseline_results, comparison_results):
    """
    Compare operator performance between two datasets.

    Args:
        baseline_results: Results from the baseline dataset (TPCH_PATH)
        comparison_results: Results from the comparison dataset (TPCH_STRING_PATH)
    """
    baseline_stats = baseline_results['operator_stats']
    comparison_stats = comparison_results['operator_stats']

    dbms_name = baseline_results['dbms'] or comparison_results['dbms']

    print(f"\n{'='*80}")
    print(f"OPERATOR PERFORMANCE COMPARISON: {dbms_name.upper() if dbms_name else 'UNKNOWN'}")
    print(f"TPC-H SF1 (Baseline) vs TPC-H UUID SF1")
    print(f"{'='*80}\n")

    # Get all operator types from both datasets
    all_operators = set(baseline_stats.keys()) | set(comparison_stats.keys())

    comparisons = []

    for op_type in all_operators:
        baseline = baseline_stats.get(op_type)
        comparison = comparison_stats.get(op_type)

        if baseline and comparison:
            baseline_avg = baseline['avg_time']
            comparison_avg = comparison['avg_time']

            # Calculate difference and percentage change
            diff = comparison_avg - baseline_avg
            pct_change = ((comparison_avg - baseline_avg) / baseline_avg * 100) if baseline_avg > 0 else 0

            # Calculate slowdown factor
            slowdown_factor = comparison_avg / baseline_avg if baseline_avg > 0 else 0

            comparisons.append({
                'operator': op_type,
                'baseline_avg': baseline_avg,
                'comparison_avg': comparison_avg,
                'diff': diff,
                'pct_change': pct_change,
                'slowdown_factor': slowdown_factor,
                'baseline_total': baseline['total_time'],
                'comparison_total': comparison['total_time']
            })
        elif comparison and not baseline:
            comparisons.append({
                'operator': op_type,
                'baseline_avg': 0,
                'comparison_avg': comparison['avg_time'],
                'diff': comparison['avg_time'],
                'pct_change': float('inf'),
                'slowdown_factor': float('inf'),
                'baseline_total': 0,
                'comparison_total': comparison['total_time']
            })
        elif baseline and not comparison:
            comparisons.append({
                'operator': op_type,
                'baseline_avg': baseline['avg_time'],
                'comparison_avg': 0,
                'diff': -baseline['avg_time'],
                'pct_change': -100,
                'slowdown_factor': 0,
                'baseline_total': baseline['total_time'],
                'comparison_total': 0
            })

    # Sort by absolute difference (most impacted operators first)
    comparisons.sort(key=lambda x: abs(x['diff']), reverse=True)

    # Summary statistics
    print("SUMMARY:")
    print(f"Total time baseline: {baseline_results['total_time']:.6f}s")
    print(f"Total time UUID: {comparison_results['total_time']:.6f}s")
    total_slowdown = comparison_results['total_time'] / baseline_results['total_time'] if baseline_results['total_time'] > 0 else 0
    print(f"Overall slowdown: {total_slowdown:.2f}x ({(total_slowdown-1)*100:+.2f}%)")

    # Top 5 most impacted operators by slowdown factor
    print(f"\nTop 10 Most Impacted Operators (by slowdown factor):")
    valid_comparisons = [c for c in comparisons if c['slowdown_factor'] != float('inf') and c['slowdown_factor'] > 0]
    valid_comparisons.sort(key=lambda x: x['slowdown_factor'], reverse=True)

    for i, comp in enumerate(valid_comparisons[:10], 1):
        print(f"  {i}. {comp['operator']}: {comp['slowdown_factor']:.2f}x slower "
              f"({comp['baseline_avg']:.6f}s → {comp['comparison_avg']:.6f}s)")


def main():
    """
    Main function to analyze operator timings across all datasets, per DBMS.
    """
    # First, export all operator timings to CSV
    print("Exporting operator timings to CSV...")
    extract_all_operator_timings_to_csv(OUTPUT_CSV)

    # Then get all unique DBMS from both CSV files
    print("\nDetecting available database systems...")
    dbms_in_tpch = set(get_available_dbms(TPCH_PATH))
    dbms_in_uuid = set(get_available_dbms(TPCH_STRING_PATH))
    all_dbms = sorted(dbms_in_tpch | dbms_in_uuid)

    if not all_dbms:
        print("No database systems found in CSV files.")
        return

    print(f"Found {len(all_dbms)} database system(s): {', '.join(all_dbms)}\n")

    # Analyze each DBMS separately
    for dbms in all_dbms:
        print(f"\n{'='*80}")
        print(f"ANALYZING: {dbms.upper()}")
        print(f"{'='*80}")

        # Analyze baseline (TPCH_PATH)
        baseline_results = None
        if dbms in dbms_in_tpch:
            baseline_results = analyze_operator_timings(TPCH_PATH, dbms)

        # Analyze UUID variant (TPCH_STRING_PATH)
        uuid_results = None
        if dbms in dbms_in_uuid:
            uuid_results = analyze_operator_timings(TPCH_STRING_PATH, dbms)

        # Compare if both datasets exist for this DBMS
        if baseline_results and uuid_results:
            compare_operator_performance(baseline_results, uuid_results)
        elif baseline_results and not uuid_results:
            print(f"\nNote: {dbms} only available in TPC-H SF1, not in UUID variant.")
        elif uuid_results and not baseline_results:
            print(f"\nNote: {dbms} only available in TPC-H UUID SF1, not in baseline.")


if __name__ == "__main__":
    main()

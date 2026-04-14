#!/usr/bin/env python3

import argparse
import csv
import logging
import os
import sys
from typing import Any, List, Optional, Tuple

import pyodbc

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    """Parses command-line arguments for database connection and query parameters.

    Returns:
        Parsed arguments with database connection info and query parameters.
    """
    parser = argparse.ArgumentParser(description="SQL Server query tool.")
    env = os.environ.get

    db_group = parser.add_argument_group("Database Connection")
    db_group.add_argument("-s", "--server", default=env("COPATHBI_SERVER"))
    db_group.add_argument("-d", "--database", default=env("COPATHBI_DATABASE"))
    db_group.add_argument("-u", "--username", default=env("COPATHBI_USER"))
    db_group.add_argument("-p", "--password", default=env("COPATHBI_PASSWORD"))

    query_group = parser.add_argument_group("Query parameters")
    query_group.add_argument("-t", "--table", default="FranklinOrder", help="Table name.")
    query_group.add_argument("-c", "--columns", nargs="+", help="Specific columns. Default: all (*).")
    query_group.add_argument("-f", "--filter-col", default="SpcNum", help="Column for IN filter.")
    query_group.add_argument("-v", "--filter-values", nargs="+", help="Values for the filter column.")
    query_group.add_argument("-w", "--where", help="Additional custom WHERE clause.")
    query_group.add_argument("-lc", "--list-columns", action="store_true", help="List table columns and exit.")
    query_group.add_argument("-o", "--output", default="query_results.csv", help="CSV output path.")

    return parser.parse_args()


def build_connection_string(
    server: Optional[str] = None,
    database: Optional[str] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
    driver: str = "{ODBC Driver 18 for SQL Server}",
) -> str:
    """Constructs a pyodbc connection string from individual parameters.

    Args:
        server: SQL Server host name.
        database: Database name.
        username: Username.
        password: Password.
        driver: ODBC driver name.

    Returns:
        Connection string.
    """
    if all([server, database, username, password]):
        return (
            f"DRIVER={driver};SERVER={server};DATABASE={database};"
            f"UID={username};PWD={password};TrustServerCertificate=yes;"
        )
    else:
        raise ValueError("All connection parameters (server, db, user, pass) must be provided.")


def get_table_columns(conn_string: str, table_name: str) -> List[str]:
    """List all columns in a table.

    Args:
        conn_string: Database connection string.
        table_name: Name of the table.

    Returns:
        List of column names.
    """
    query = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ?"
    with pyodbc.connect(conn_string) as conn, conn.cursor() as cursor:
        cursor.execute(query, table_name)
        return [row[0] for row in cursor.fetchall()]


def execute_and_stream_to_csv(
    conn_string: str, query: str, output_path: str, params: Optional[List[Any]] = None
) -> int:
    """Executes a query and streams results directly to CSV to save memory.

    Args:
        conn_string: Database connection string.
        query: SQL query.
        output_path: Path to output CSV file.
        params: Query parameters.

    Returns:
        Number of rows written to the output file.
    """
    logger.debug("Executing query: %s", query)
    row_count = 0
    with pyodbc.connect(conn_string) as conn, conn.cursor() as cursor:
        cursor.execute(query, params or [])
        if not cursor.description:
            return 0

        columns = [col[0] for col in cursor.description]
        with open(output_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(columns)
            for row in cursor:
                writer.writerow(row)
                row_count += 1
    return row_count


def build_query(args: argparse.Namespace) -> Tuple[str, List[Any]]:
    """Constructs the SQL query and parameter list from arguments.

    Args:
        args: Command-line arguments.

    Returns:
        Tuple containing the SQL query string and a list of parameters.
    """
    cols = list(args.columns) if args.columns else ["*"]
    if "*" not in cols and args.filter_col not in cols:
        cols.insert(0, args.filter_col)

    select_list = ", ".join(f"[{c}]" if c != "*" else c for c in cols)
    query = f"SELECT {select_list} FROM [{args.table}]"
    clauses, params = [], []

    if args.filter_values:
        placeholders = ",".join("?" * len(args.filter_values))
        clauses.append(f"[{args.filter_col}] IN ({placeholders})")
        params.extend(args.filter_values)

    if args.where:
        clauses.append(f"({args.where})")

    if clauses:
        query += f" WHERE {' AND '.join(clauses)}"

    return query, params


def main() -> None:
    """Main function to parse arguments, build query, execute it, and handle results."""
    args = parse_args()
    try:
        conn_string = build_connection_string(
            server=args.server,
            database=args.database,
            username=args.username,
            password=args.password,
        )
        logger.debug("Database connection string built.")

        if args.list_columns:
            columns = get_table_columns(conn_string, args.table)
            print("\n".join(columns))
            return

        query, params = build_query(args)
        row_count = execute_and_stream_to_csv(conn_string, query, args.output, params)

        if row_count == 0:
            logger.info("No results found.")
        else:
            logger.info("Successfully saved %d rows to %s", row_count, args.output)

    except Exception as e:
        logger.error("Error: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()

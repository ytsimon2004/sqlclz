import sqlite3
import textwrap
from pathlib import Path
from typing import NamedTuple

try:
    from diagrams import Diagram, Cluster, Edge
    from diagrams.generic.database import SQL

    DIAGRAMS_AVAILABLE = True
except ImportError:
    Diagram = None
    Cluster = None
    Edge = None
    SQL = None
    DIAGRAMS_AVAILABLE = False

__all__ = ['generate_diagram',
           'DatabaseInfo',
           'database_info']


def generate_diagram(database_file: Path | str,
                     output_filename: str = 'sqlite_schema',
                     **diagram_kwargs):
    """
    Generate a database schema diagram from a SQLite database file.

    :param database_file: Path to the SQLite database file
    :param output_filename: Output filename (without extension)
    :param diagram_kwargs: Additional keyword arguments passed to the Diagram constructor.
                          Can override defaults like 'direction', 'show', 'outformat', etc.
    :raises ImportError: If diagrams package is not installed
    """
    if not DIAGRAMS_AVAILABLE:
        raise ImportError(
            'The "diagrams" package is required to generate diagrams. '
            'Install it with: pip install diagrams'
        )

    info = database_info(database_file)
    nodes = {}

    diagram_defaults = {
        'name': 'SQLite Schema',
        'show': True,
        'direction': 'TB',
        'outformat': 'png',
        'filename': output_filename
    }
    diagram_defaults.update(diagram_kwargs)
    with Diagram(**diagram_defaults):
        for table_name in info.schema.keys():
            label = _make_label_from_info(table_name, info)
            with Cluster(label, graph_attr={
                'bgcolor': '#F0F6FF',
                'pencolor': '#7EA0E0',
                'style': 'rounded,filled',
                'fontname': 'Helvetica',
                'fontsize': '11',
            }):
                nodes[table_name] = SQL(table_name)

        for src, dst in info.relations:
            nodes[src] >> Edge(color='#607080', penwidth='1.5', arrowhead='vee') >> nodes[dst]


class DatabaseInfo(NamedTuple):
    """Database schema information."""
    schema: dict[str, list[tuple[str, str, bool]]]
    """Table schemas: {table_name: [(column_name, type, is_primary_key)]}"""
    primary_keys: dict[str, list[str]]
    """Primary keys: {table_name: [column_names]}"""
    relations: list[tuple[str, str]]
    """Foreign key relations: [(source_table, target_table)]"""


def database_info(database_file: Path | str) -> DatabaseInfo:
    """
    Extract schema information from a SQLite database.

    :param database_file: Path to the SQLite database file
    :return: DatabaseInfo containing schema, primary keys, and relations
    """
    conn = sqlite3.connect(database_file)
    cursor = conn.cursor()
    cursor.execute(
        'SELECT name FROM sqlite_master '
        "WHERE type='table' AND name NOT LIKE 'sqlite_%'"
    )

    tables = [t[0] for t in cursor.fetchall()]

    schema = {}
    primary_keys = {}
    relations = []

    for table in tables:
        cursor.execute(f'PRAGMA table_info({table})')
        cols = cursor.fetchall()
        schema[table] = [(c[1], c[2], c[5]) for c in cols]  # (name, type, is_pk)
        primary_keys[table] = [c[1] for c in cols if c[5] == 1]

        # Foreign key references (table-level only)
        cursor.execute(f'PRAGMA foreign_key_list({table})')
        for (_, _, target_table, *_rest) in cursor.fetchall():
            if target_table in tables:
                relations.append((table, target_table))

    conn.close()
    return DatabaseInfo(schema, primary_keys, relations)


def _make_label_from_info(table: str, info: DatabaseInfo) -> str:
    """Make label from DatabaseInfo object."""
    cols_str = '\n'.join([
        f"{'🔑' if is_pk else '•'} {name} : {typ}"
        for name, typ, is_pk in info.schema[table]
    ])
    return textwrap.dedent(f'''
    {table}
    -------------------
    {cols_str}
    ''').strip()

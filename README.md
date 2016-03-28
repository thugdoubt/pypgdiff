# pyPGdiff

Tools for generating on-the-fly migrations between PostgreSQL
schemas.

## Usage

    import psycopg2
    from pypgdiff.objects import Database, Schema
    
    conn = psycopg2.connect(database=x, user=y, password=z)
    
    # the source of truth
    source_schema = Schema(database=Database(conn=conn, name="source_schema")
    # the target to change
    target_schema = Schema(database=Database(conn=conn, name="target_schema")
    
    # changeset will be sorted
    changes = source_schema | target_schema
    for change in changes:
        sql = change.sql

## Configuration

Diffs can be configured with the Config context manager:

    from pypgdiff import Config
    with Config(prompt_for_defaults=True):
      changes = source_schema | target_schema

The following config options are available:

-   **no\_alter\_sequences**
      ~ Do not include ALTER SEQUENCE changes. This is primarily useful
        to suppress sequence restarts, which are probably not useful.
        Default: False


-   **normalize\_constraints**
      ~ When comparing constraints, use an automagically generated name
        to avoid generating unnecessary constraint queries. Default:
        False


-   **prompt\_for\_defaults**
      ~ When altering a column to be not NULL, prompt the user for a
        defailt value to use. Default: False






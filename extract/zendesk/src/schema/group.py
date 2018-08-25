from shared_modules.elt.schema import Schema, Column, DBType


PG_SCHEMA = 'zendesk'
PG_TABLE = 'groups'
PRIMARY_KEY = 'id'
URL = "{}/groups.json"
incremental = False


def describe_schema(args) -> Schema:
    table_name = args.table_name or PG_TABLE
    table_schema = args.schema or PG_SCHEMA

    # curry the Column object
    def column(column_name, data_type, *,
               is_nullable=True,
               is_mapping_key=False):
        return Column(table_schema=table_schema,
                      table_name=table_name,
                      column_name=column_name,
                      data_type=data_type.value,
                      is_nullable=is_nullable,
                      is_mapping_key=is_mapping_key)

    return Schema(table_schema, [
        column("id",                     DBType.Long, is_mapping_key=True),
        column("name",                   DBType.String),
        column("updated_at",             DBType.Date),
        column("url",                    DBType.String),
        column("created_at",             DBType.Date),
        column("deleted",                DBType.Boolean),
    ])


def table_name(args):
    return args.table_name or PG_TABLE

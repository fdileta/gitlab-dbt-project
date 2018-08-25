import argparse

from shared_modules.elt.cli import parser_db_conn, parser_date_window, parser_output, parser_logging
from shared_modules.elt.utils import setup_logging, setup_db
from shared_modules.elt.db import DB
from shared_modules.elt.schema import schema_apply
from shared_modules.elt.error import with_error_exit_code
from export import extract
from enum import Enum

import schema.brand as brand
import schema.group as group
import schema.group_membership as group_membership
import schema.organization as organization
import schema.organization_membership as organization_membership
import schema.tag as tag
import schema.ticket as ticket
import schema.ticket_event as ticket_event
import schema.ticket_field as ticket_field
import schema.user as user


def action_export(args):
    extract(args)


def action_schema_apply(args):
    schemas = [ticket.describe_schema(args),
               ticket_event.describe_schema(args),
               ticket_field.describe_schema(args),
               tag.describe_schema(args),
               group.describe_schema(args),
               group_membership.describe_schema(args),
               user.describe_schema(args),
               organization.describe_schema(args),
               organization_membership.describe_schema(args),
               brand.describe_schema(args),]
    with DB.default.open() as db:
        for schema in schemas:
            schema_apply(db, schema)


class Action(Enum):
    EXPORT = ('export', action_export)
    APPLY_SCHEMA = ('apply_schema', action_schema_apply)

    @classmethod
    def from_str(cls, name):
        return cls[name.upper()]

    def __str__(self):
        return self.value[0]

    def __call__(self, args):
        return self.value[1](args)


def parse():
    parser = argparse.ArgumentParser(
        description="Use the Zendesk API to retrieve ticket data.")

    parser_db_conn(parser)
    parser_date_window(parser)
    parser_output(parser)
    parser_logging(parser)

    parser.add_argument('action',
                        type=Action.from_str,
                        choices=list(Action),
                        default=Action.EXPORT,
                        help=("export: bulk export data into the output.\n"
                              "apply_schema: export the schema into a schema file."))

    return parser.parse_args()

@with_error_exit_code
def execute(args):
    args.action(args)

def main():
    args = parse()
    setup_logging(args)
    setup_db(args)
    execute(args)

main()

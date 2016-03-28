from pypgdiff import Changeset, Config, NOW

class BaseObject(object):
    def __or__(self, other):
        raise NotImplementedError()

    def __bool__(self):
        try:
            return self.name is not None
        except AttributeError:
            return True
    __nonzero__=__bool__

    def __ne__(self, other):
        return not self.__eq__(other)

    def scrub_schema_info(self, d):
        for col in ("table_catalog", "table_schema","ordinal_position",
                    "constraint_catalog", "constraint_schema",
                    "domain_catalog", "domain_schema",
                    "udt_catalog","udt_schema","dtd_identifier",
                    "schemaname", "tablespace"):
            if col in d:
                del d[col]
        return d

    @property
    def comparison_props(self):
        return self.props

class Database(BaseObject):
    def __init__(self, conn):
        from psycopg2.extras import DictCursor
        self.conn = conn
        self.curs = conn.cursor(cursor_factory=DictCursor)

    def execute(self, *args):
        return self.curs.execute(*args)

    def fetchall(self):
        return self.curs.fetchall()

class Schema(BaseObject):
    def __init__(self, database=None, name="public", cache=True, defaults={}):
        from collections import defaultdict
        self.db = database
        self.name = name
        self.cache = cache
        self.defaults = defaultdict(dict, defaults)

    def __or__(self, other):
        cs = Changeset()

        # compare sequences
        s1 = self.get_sequences()
        s2 = other.get_sequences()
        for name in set(s1.keys() + s2.keys()):
            cs += s1.get(name, Sequence(self, None)) | s2.get(name, Sequence(other, None))

        # compare tables
        t1 = self.get_tables()
        t2 = other.get_tables()
        for name in set(t1.keys() + t2.keys()):
            cs += t1.get(name, Table(self, None)) | t2.get(name, Table(other, None))

        # compare constraints
        c1 = self.get_constraints()
        c2 = other.get_constraints()
        for name in set(c1.keys() + c2.keys()):
            cs += c1.get(name, Constraint(self, None)) | c2.get(name, Constraint(other, None))

        # compare indexes
        i1 = self.get_indexes()
        i2 = other.get_indexes()
        for name in set(i1.keys() + i2.keys()):
            cs += i1.get(name, Index(self, None)) | i2.get(name, Index(other, None))

        return sorted(cs)

    def get_default(self, column):
        import datetime
        if column.table.name in self.defaults and column.name in self.defaults[column.name]:
            return self.defaults[column.table.name][column.name]
        if Config().prompt_for_defaults:
            print(" Column %s.%s (type %s) is becoming NOT NULL without a default!" % (column.table.name, column.name, column.props["data_type"]))
            print(" Please enter Python code for a one-off default value.")
            print(" The datetime module is available, so you can do e.g. datetime.date.today()")
            print(" You can also use NOW to represent NOW().")
            value = None
            while True:
                code = raw_input(" >>>> ")
                if not code:
                    break
                else:
                    try:
                        value = eval(code, {}, {"datetime": datetime, "NOW": NOW()})
                    except (SyntaxError, NameError) as e:
                        print(" ! Invalid input: %s" % e)
                    else:
                        break
            if value not in (None, ""):
                self.defaults[column.table.name][column.name] = value
                return self.defaults[column.table.name][column.name]
            else:
                return Undefined
        return Undefined

    def get_tables(self):
        if self.cache:
            try:
                return self._tables
            except AttributeError:
                pass
        self._tables = dict()
        self.db.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = %s AND table_type = 'BASE TABLE' AND table_name !~ '^pgsql_'", (self.name,))
        for table_name in [x[0] for x in self.db.fetchall()]:
            self._tables[table_name] = Table(self, table_name)
        return self._tables

    def get_sequences(self):
        if self.cache:
            try:
                return self._sequences
            except AttributeError:
                pass
        self._sequences = dict()

        # not all sequence information is available in the information_schema
        self.db.execute("SELECT sequence_name FROM information_schema.sequences WHERE sequence_schema = %s", (self.name,))
        for sequence_name in [x[0] for x in self.db.fetchall()]:
            self.db.execute("SELECT * FROM %s.%s" % (self.name, sequence_name))
            props = dict(filter(lambda x: x[0] not in ("sequence_catalog", "sequence_schema", "is_called", "log_cnt"), dict(self.db.fetchall()[0]).items()))
            self._sequences[sequence_name] = Sequence(self, sequence_name, **props)
        return self._sequences

    def get_constraints(self):
        # TODO: are constraint names unique per schema?
        if self.cache:
            try:
                return self._constraints
            except AttributeError:
                pass
        self._constraints = dict()

        # possibly normalize constraints
        def normalize_props(props):
            conf = Config()
            # default comparison key
            props["comparison_key"] = props["constraint_name"]
            # use a different comparison key based on the constraint
            if conf.normalize_constraints:
                if props["constraint_type"] in ("PRIMARY KEY","UNIQUE"):
                    props["comparison_key"] = "%s_%s_%s" % (props["constraint_type"],
                                                            props["table_name"],
                                                            "_".join(sorted(props["columns"])))
                elif props["constraint_type"] in ("FOREIGN KEY",):
                    props["comparison_key"] = "%s_%s_%s__to__%s" % (props["constraint_type"],
                                                                    props["table_name"],
                                                                    "_".join(sorted(map(lambda x: x["column_name"], props["from"]))),
                                                                    "_".join(sorted(map(lambda x: x["column_name"], props["to"]))))
                elif props["constraint_type"] in ("CHECK",):
                    props["comparison_key"] = "CHECK_%s_%s" % (props["table_name"], props["clause"])
                else:
                    raise Exception("Unknown normalization type: %s" % props["constraint_type"])
            return props

        # PRIMARY KEY / UNIQUE
        self.db.execute("SELECT * FROM information_schema.table_constraints WHERE constraint_schema = %s AND constraint_type IN ('PRIMARY KEY', 'UNIQUE')", (self.name,))
        for props in map(dict, [x for x in self.db.fetchall()]):
            self.scrub_schema_info(props)
            self.db.execute("SELECT column_name FROM information_schema.constraint_column_usage WHERE " +
                                "constraint_schema = %s AND table_schema = %s AND table_name = %s AND constraint_name = %s",
                                (self.name, self.name, props["table_name"], props["constraint_name"]))
            props["columns"] = set([x[0] for x in self.db.fetchall()])
            props = normalize_props(props)
            self._constraints[props["comparison_key"]] = Constraint(self, props["constraint_name"], **props)

        # FOREIGN KEY
        self.db.execute("SELECT * FROM information_schema.table_constraints WHERE constraint_schema = %s AND constraint_type IN ('FOREIGN KEY')", (self.name,))
        for props in map(dict, [x for x in self.db.fetchall()]):
            self.scrub_schema_info(props)
            self.db.execute("SELECT * FROM information_schema.constraint_column_usage WHERE " +
                                "constraint_schema = %s AND constraint_name = %s",
                                (self.name, props["constraint_name"]))
            props["to"] = map(self.scrub_schema_info, map(dict, [x for x in self.db.fetchall()]))

            self.db.execute("SELECT * FROM information_schema.key_column_usage WHERE " +
                                "constraint_schema = %s AND constraint_name = %s",
                                (self.name, props["constraint_name"]))
            props["from"] = map(self.scrub_schema_info, map(dict, [x for x in self.db.fetchall()]))

            props = normalize_props(props)
            self._constraints[props["comparison_key"]] = Constraint(self, props["constraint_name"], **props)

        # CHECK
        # NOTE: NOT NULL constraints will be implicit
        self.db.execute("SELECT * FROM information_schema.table_constraints WHERE constraint_schema = %s AND constraint_type IN ('CHECK') AND constraint_name !~ '_not_null$'", (self.name,))
        for props in map(dict, [x for x in self.db.fetchall()]):
            self.scrub_schema_info(props)
            self.db.execute("SELECT check_clause FROM information_schema.check_constraints WHERE " +
                                "constraint_schema = %s AND constraint_name = %s",
                                (self.name, props["constraint_name"]))
            # only one expression per constraint
            props["clause"] = self.db.fetchall()[0][0]

            props = normalize_props(props)
            self._constraints[props["comparison_key"]] = Constraint(self, props["constraint_name"], **props)

        return self._constraints

    def get_indexes(self):
        if self.cache:
            try:
                return self._indexes
            except AttributeError:
                pass
        self._indexes = dict()
        # TODO: gotta be a less grody way to do this
        self.db.execute("SELECT * FROM pg_indexes WHERE schemaname = %s AND indexname !~ '(_pkey|_key)$' AND indexdef !~ '^CREATE UNIQUE'", (self.name,))
        for props in map(dict, [x for x in self.db.fetchall()]):
            self.scrub_schema_info(props)
            self._indexes[props["indexname"]] = Index(self, props["indexname"], **props)
        return self._indexes

    def get_types(self):
        try:
            return self._type_info
        except AttributeError:
            pass
        self._type_info = dict()
        # TODO: need a proper way to convert type names to SQL types
        sql_types = {
            "int2"  : "smallint",
            "int4"  : "integer",
            "int8"  : "bigint",
            "bpchar": "character",
        }
        self.db.execute("SELECT pg_type.*, pg_type.oid FROM pg_type WHERE typname !~ '^pg_'")
        for info in map(dict, [x for x in self.db.fetchall()]):
            info["sql_type"] = sql_types.get(
                info["typname"].replace("_", ""),
                info["typname"]
            )
            self._type_info[info["typname"]] = self._type_info[info["oid"]] = info
        return self._type_info

    def type_info(self, udt_name):
        return self.get_types().get(udt_name)

    def expand_array(self, table_name, column_name):
        self.db.execute(
            "SELECT " +
                "(information_schema._pg_char_max_length(t.typelem, a.atttypmod))::information_schema.cardinal_number AS character_maximum_length, " +
                "(information_schema._pg_numeric_precision(t.typelem, a.atttypmod))::information_schema.cardinal_number AS numeric_precision, " +
                "(information_schema._pg_numeric_scale(t.typelem, a.atttypmod))::information_schema.cardinal_number AS numeric_scale " +
            "FROM " +
                "pg_attribute a " +
                    "LEFT JOIN pg_attrdef ad ON ((a.attrelid = ad.adrelid) AND (a.attnum = ad.adnum)) " +
                    "LEFT JOIN pg_type t ON (a.atttypid = t.oid), " +
                "pg_class c " +
            "WHERE " +
                "a.attrelid = c.oid AND " +
                "c.relname = %s AND " +
                "a.attname = %s",
            (table_name, column_name)
        )
        return dict(self.db.fetchall()[0])

################################################################################
## TABLES
################################################################################

class Column(BaseObject):
    def __init__(self, table, name, **props):
        self.table = table
        self.name = name
        self.props = props

    @property
    def safe_name(self):
        from pypgdiff.constants import PG_RESERVED_WORDS
        if self.name.upper() in PG_RESERVED_WORDS:
            return "\"%s\"" % self.name
        return self.name

    def mogrify(self, val):
        if type(val) in (int,):
            return val
        elif type(val) in (NOW,):
            return repr(val)
        else:
            # lol
            return self.table.schema.db.curs.mogrify("%s", (val,))

    def __eq__(self, other):
        return self.comparison_props == other.comparison_props

    def __or__(self, other):
        cs = Changeset()

        if not bool(self):
            # None -> Thing: Drop Thing
            from pypgdiff.changes import DropColumn
            cs += DropColumn(self, other)
        elif not bool(other):
            # Thing -> None: Add Thing
            from pypgdiff.changes import AddColumn
            cs += AddColumn(self, other)
        elif self == other:
            # Both columns exist and they match!
            pass
        else:
            # ..the hard part
            from pypgdiff.changes import AlterColumn
            cs += AlterColumn(self, other)

        return cs

    def expand_array(self):
        props = self.table.schema.expand_array(self.table.name, self.name)
        self.props.update(props)

class Table(BaseObject):
    def __init__(self, schema, name):
        self.schema = schema
        self.name = name

    def __eq__(self, other):
        c1 = self.get_columns()
        c2 = other.get_columns()
        for col in set(c1.keys() + c2.keys()):
            if c1.get(col, Column(self, None)) != c2.get(col, Column(other, None)):
                return False
        return True

    def __or__(self, other):
        cs = Changeset()

        if not bool(self):
            # None -> Thing : Drop Thing
            from pypgdiff.changes import DropTable
            cs += DropTable(self, other)
        elif not bool(other):
            # Thing -> None : Add Thing
            from pypgdiff.changes import CreateTable
            cs += CreateTable(self, other)
        elif self == other:
            # both tables exists and they match!
            pass
        else:
            # both tables exist, get our compare on
            from pypgdiff.changes import AlterTable
            c1 = self.get_columns()
            c2 = other.get_columns()
            _cs = Changeset()
            for name in set(c1.keys() + c2.keys()):
                _cs += c1.get(name, Column(self, None)) | c2.get(name, Column(other, None))
            cs += AlterTable(self, other, changeset=_cs)

        return cs

    def get_columns(self):
        from collections import OrderedDict
        try:
            return self._cols
        except AttributeError:
            pass
        self.schema.db.execute("SELECT * FROM information_schema.columns WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position ASC", (self.schema.name, self.name))
        self._cols = OrderedDict()
        for m in map(dict, self.schema.db.fetchall()):
            self.scrub_schema_info(m)
            self._cols[m["column_name"]] = Column(self, m["column_name"], **m)
        return self._cols

################################################################################
## SEQUENCES
################################################################################

class Sequence(BaseObject):
    def __init__(self, schema, name, **props):
        self.schema = schema
        self.name = name
        self.props = props

    def __eq__(self, other):
        if self.comparison_props != other.comparison_props:
            return False
        # everything but last_value matches!
        if self.props["last_value"] > other.props["last_value"]:
            return False
        return True

    def __or__(self, other):
        cs = Changeset()

        if not bool(self):
            # None -> Other: Drop
            from pypgdiff.changes import DropSequence
            cs += DropSequence(self, other)
        elif not bool(other):
            # Self -> None: Add
            from pypgdiff.changes import CreateSequence
            cs += CreateSequence(self, other)
        elif self == other:
            # Self == Other: Noop
            pass
        else:
            # Self != Other: Alter
            if not Config().no_alter_sequences:
                from pypgdiff.changes import AlterSequence
                cs += AlterSequence(self, other)

        return cs

    @property
    def comparison_props(self):
        ret = self.props.copy()
        for field in ("last_value", "sequence_name",):
            if field in ret:
                del ret[field]
        return ret

################################################################################
## CONSTRAINTS
################################################################################

class Constraint(BaseObject):
    def __init__(self, schema, name, **props):
        self.schema = schema
        self.name = name
        self.props = props

    def __eq__(self, other):
        return self.comparison_props == other.comparison_props

    def __or__(self, other):
        cs = Changeset()

        if not bool(self):
            # None -> Other: Drop
            from pypgdiff.changes import DropConstraint
            cs += DropConstraint(self, other)
        elif not bool(other):
            # Self -> None: Create
            from pypgdiff.changes import CreateConstraint
            cs += CreateConstraint(self, other)
        elif self == other:
            # Self == Other: Noop
            pass
        else:
            # Self != Other: Alter
            # NOTE: since you can't ALTER a constrant, this expands to a DROP/CREATE
            from pypgdiff.changes import DropConstraint, CreateConstraint
            cs += DropConstraint(self, other)
            cs += CreateConstraint(self, other)

        return cs

    @property
    def comparison_props(self):
        ret = self.props.copy()
        def strip_name(x):
            if x and "constraint_name" in x:
                del x["constraint_name"]
        if Config().normalize_constraints:
            strip_name(ret)
            for t in ("from", "to"):
                if t in ret:
                    map(strip_name, ret[t])
        return ret
################################################################################
## INDEXES
################################################################################

class Index(BaseObject):
    def __init__(self, schema, name, **props):
        import re
        self.schema = schema
        self.name = name
        self.props = props
        if "indexdef" in self.props:
            self.props["comparison_indexdef"] = re.sub("ON ([^\.]+\.)?", "ON ", self.props["indexdef"])

    def __eq__(self, other):
        return self.props.get("comparison_indexdef") == other.props.get("comparison_indexdef")

    def __or__(self, other):
        cs = Changeset()

        if not bool(self):
            # None -> Other: Drop
            from pypgdiff.changes import DropIndex
            cs += DropIndex(self, other)
        elif not bool(other):
            # Self -> None: Create
            from pypgdiff.changes import CreateIndex
            cs += CreateIndex(self, other)
        elif self == other:
            # Self == Other: Noop
            pass
        else:
            # Self != Other: Alter
            # NOTE: don't have enough info for a real ALTER, this will be a DROP/CREATE
            from pypgdiff.changes import DropIndex, CreateIndex
            cs += DropIndex(self, other)
            cs += CreateIndex(self, other)

        return cs

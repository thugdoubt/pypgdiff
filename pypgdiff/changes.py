class BaseChange(object):
    priority = 0

    def __init__(self, this, that, changeset=None):
        self.this = this
        self.that = that
        self.cs = changeset

    def __iter__(self):
        class _iterator(object):
            def __init__(self, obj):
                self.obj = obj
                self.index = 0
            def __iter__(self):
                return self
            def next(self):
                if self.index:
                    raise StopIteration
                else:
                    self.index += 1
                    return self.obj
        return _iterator(self)

    def __cmp__(self, other):
        return cmp(self.priority, other.priority)

    def __getitem__(self, index):
        return self.cs[index]

    def __len__(self):
        return len(self.cs)

    @property
    def sql(self):
        return self.__sql__()

################################################################################
## TABLES
################################################################################

class CreateTable(BaseChange):
    priority = 60
    def __sql__(self):
        cols = self.this.get_columns()
        ret  = "CREATE TABLE %s.%s (\n" % (
            self.that.schema.name,
            self.this.name
        )
        ret += ",\n".join(map(
            lambda col: "    %s" % CreateColumn(col, None).sql, cols.values()
        )) + "\n"
        ret += ");"
        return ret

class DropTable(BaseChange):
    priority = 20
    def __sql__(self):
        return "DROP TABLE %s.%s;" % (
            self.that.schema.name,
            self.that.name
        )

class AlterTable(BaseChange):
    priority = 70
    def __sql__(self):
        from itertools import chain
        ret = "ALTER TABLE %s.%s\n" % (
            self.that.schema.name,
            self.that.name
        )
        # sorry for the grossitude, but AlterColumn returns a list of changes,
        # while everything else returns a sssssstring
        ret += ",\n".join(map(
            lambda chg: "    %s" % chg,
                [q for q in chain(*[[z] if z[0:0] == '' else z for z in map(lambda f: f.__sql__(), self.cs)])]
        )) + "\n"
        ret += ";"
        return ret

class CreateColumn(BaseChange):
    priority = 70
    def __sql__(self):
        data_type = self.this.props["data_type"]
        is_array = False
        if data_type == "ARRAY":
            # resolve the data type
            type_info = self.this.table.schema.type_info(self.this.props["udt_name"])
            data_type = self.this.table.schema.type_info(type_info["typelem"])["sql_type"]
            is_array = True
            if data_type in ("character", "numeric"):
                # type precision isn't available for arrays in the information_schema view
                # manually expand it here
                self.this.expand_array()
        if data_type in ("character", "char", "character varying", "varchar") and self.this.props["character_maximum_length"] is not None:
            data_type += "(%s)" % self.this.props["character_maximum_length"]
        if data_type in ("numeric",) and self.this.props["numeric_precision"] is not None:
            data_type += "(%s)" % ",".join(map(str, filter(None, (self.this.props["numeric_precision"], self.this.props["numeric_scale"]))))
        if is_array:
            data_type += "[]"
        ret = "%s %s" % (
            self.this.safe_name,
            data_type,
        )
        if self.this.props["column_default"] is not None:
            ret += " DEFAULT %s" % self.this.props["column_default"]
        if self.this.props["is_nullable"] in ("NO",):
            ret += " NOT NULL"
        return ret

class AddColumn(BaseChange):
    priority = 72

    def __init__(self, *args, **kwargs):
        super(AddColumn, self).__init__(*args, **kwargs)
        try:
            # adding a NOT NULL column needs a default
            if not self.this.props["column_default"] and \
            self.this.props["is_nullable"] in ("NO",):
                value = self.this.table.schema.get_default(self.this)
                if value is not Undefined:
                    self.this.props["column_default"] = self.this.mogrify(value)
        except AttributeError:
            # grossitude for tests
            pass

    def __sql__(self):
        return "ADD COLUMN %s" % CreateColumn(self.this, None).sql

class DropColumn(BaseChange):
    priority = 71
    def __sql__(self):
        return "DROP COLUMN %s" % self.that.safe_name

class AlterColumn(BaseChange):
    priority = 73

    def __init__(self, *args, **kwargs):
        super(AlterColumn, self).__init__(*args, **kwargs)
        try:
            # if neither side has a default, and the column is becoming not nullable, collect one
            if not self.this.props["column_default"] and \
               not self.that.props["column_default"] and \
               self.this.props["is_nullable"] != self.that.props["is_nullable"] and \
               self.this.props["is_nullable"] in ("NO",):
                value = self.this.table.schema.get_default(self.this)
                if value is not Undefined:
                    self.this.props["column_default"] = self.this.mogrify(value)
        except AttributeError:
            # grossitude for tests
            pass

    def __sql__(self):
        ret = []
        # check type
        if self.this.props["data_type"] != self.that.props["data_type"]:
            ret += ["ALTER COLUMN %s TYPE %s" % (self.that.safe_name, self.this.props["data_type"])]
            if self.this.props["character_maximum_length"] is not None:
                ret[-1] += "(%s)" % self.this.props["character_maximum_length"]
        # check default
        if self.this.props["column_default"] != self.that.props["column_default"]:
            if self.this.props["column_default"]:
                ret += ["ALTER COLUMN %s SET DEFAULT %s" % (self.that.safe_name, self.this.props["column_default"])]
            else:
                ret += ["ALTER COLUMN %s DROP DEFAULT" % self.that.safe_name]
        # check nullable
        if self.this.props["is_nullable"] != self.that.props["is_nullable"]:
            if self.this.props["is_nullable"] in ("NO",):
                ret += ["ALTER COLUMN %s SET NOT NULL" % self.that.safe_name]
            else:
                ret += ["ALTER COLUMN %s DROP NOT NULL" % self.that.safe_name]
        return ret

################################################################################
## SEQUENCES
################################################################################

class CreateSequence(BaseChange):
    priority = 40
    def __sql__(self):
        import sys
        ret = "CREATE SEQUENCE %s.%s\n" % (
            self.that.schema.name,
            self.this.name
        )
        ret += "    START WITH %s\n" % self.this.props["start_value"]
        ret += "    INCREMENT BY %s\n" % self.this.props["increment_by"]
        if self.this.props["min_value"] == 1:
            ret += "    NO MINVALUE\n"
        else:
            ret += "    MINVALUE %s\n" % self.this.props["min_value"]
        if self.this.props["max_value"] == sys.maxint:
            # NOTE: this will fail if, e.g. 64-bit python run against 32-bit postgres
            ret += "    NO MAXVALUE\n"
        else:
            ret += "    MAXVALUE %s\n" % self.this.props["max_value"]
        ret += "    CACHE %s\n" % self.this.props["cache_value"]
        if self.this.props["is_cycled"]:
            ret += "    CYCLE\n"
        ret += ";"
        return ret

class DropSequence(BaseChange):
    priority = 30
    def __sql__(self):
        return "DROP SEQUENCE %s.%s;" % (
            self.that.schema.name,
            self.that.name
        )

class AlterSequence(BaseChange):
    priority = 50
    def __sql__(self):
        import sys
        ret = "ALTER SEQUENCE %s.%s\n" % (
            self.that.schema.name,
            self.that.name
        )
        # check start
        if self.this.props["start_value"] != self.that.props["start_value"]:
            # NOTE: this can cause problems if start_value < min_value
            #       but in theory we're always transitioning from one
            #       valid sequence to another
            ret += "    START WITH %s\n" % self.this.props["start_value"]
        # see if restart is required
        last = { self.this.props["last_value"],
                 self.that.props["last_value"] }
        if len(last) > 1:
            # last_value differs.. play it like young zaphod and use the highest
            # NOTE: adding 1 here because RESTART will set is_called to false
            #       resulting in duplicate value on first nextval()
            ret += "    RESTART %s\n" % (max(last) + 1,)
        # check increment
        if self.this.props["increment_by"] != self.that.props["increment_by"]:
            ret += "    INCREMENT BY %s\n" % self.this.props["increment_by"]
        # check minvalue
        if self.this.props["min_value"] != self.that.props["min_value"]:
            if self.this.props["min_value"] == 1:
                ret += "    NO MINVALUE\n"
            else:
                ret += "    MINVALUE %s\n" % self.this.props["min_value"]
        # check maxvalue
        if self.this.props["max_value"] != self.that.props["max_value"]:
            if self.this.props["max_value"] == sys.maxint:
                # NOTE: this will fail if, e.g. 64-bit python run against 32-bit postgres
                ret += "    NO MAXVALUE\n"
            else:
                ret += "    MAXVALUE %s\n" % self.this.props["max_value"]
        # check cache
        if self.this.props["cache_value"] != self.that.props["cache_value"]:
            ret += "    CACHE %s\n" % self.this.props["cache_value"]
        # check cycle
        if self.this.props["is_cycled"] != self.that.props["is_cycled"]:
            if self.this.props["is_cycled"]:
                ret += "    CYCLE\n"
            else:
                ret += "    NO CYCLE\n"
        ret += ";"
        return ret

################################################################################
## CONSTRAINTS
################################################################################

class CreateConstraint(BaseChange):
    @property
    def priority(self):
        try:
            if self.this.props["constraint_type"] in ("UNIQUE",):
                return 80
            if self.this.props["constraint_type"] in ("PRIMARY KEY",):
                return 81
            if self.this.props["constraint_type"] in ("FOREIGN KEY",):
                return 82
        except AttributeError:
            pass
        return 85

    def __sql__(self):
        ret = "ALTER TABLE %s.%s\n" % (
            self.that.schema.name,
            self.this.props["table_name"]
        )
        if self.this.props["constraint_type"] in ("PRIMARY KEY", "UNIQUE"):
            ret += "    ADD CONSTRAINT %s %s (%s)" % (
                self.this.name,
                self.this.props["constraint_type"],
                ", ".join(sorted(self.this.props["columns"]))
            )
        if self.this.props["constraint_type"] in ("FOREIGN KEY",):
            ret += "    ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s(%s)" % (
                self.this.name,
                ", ".join(sorted([x["column_name"] for x in self.this.props["from"]])),
                self.this.props["to"][0]["table_name"],
                ", ".join(sorted([x["column_name"] for x in self.this.props["to"]]))
            )
            if self.this.props["is_deferrable"] in ("YES",):
                ret += " DEFERRABLE"
            if self.this.props["initially_deferred"] in ("YES",):
                ret += " INITIALLY DEFERRED"
        if self.this.props["constraint_type"] in ("CHECK",):
            ret += "    ADD CONSTRAINT %s CHECK %s" % (
                self.this.name,
                self.this.props["clause"]
            )
        ret += ";"
        return ret

class DropConstraint(BaseChange):
    @property
    def priority(self):
        try:
            if self.that.props["constraint_type"] in ("FOREIGN KEY",):
                return 10
            if self.that.props["constraint_type"] in ("PRIMARY KEY",):
                return 11
            if self.that.props["constraint_type"] in ("UNIQUE",):
                return 12
        except AttributeError:
            pass
        return 15

    def __sql__(self):
        ret = "ALTER TABLE %s.%s\n" % (
            self.that.schema.name,
            self.that.props["table_name"]
        )
        ret += "    DROP CONSTRAINT %s;" % self.that.name
        return ret

################################################################################
## INDEXES
################################################################################

class CreateIndex(BaseChange):
    priority = 90
    def __sql__(self):
        import re
        # NOTE: index inherits schema of target table
        return re.sub(
            "ON ([^\.]+\.)?",
            "ON %s." % self.that.schema.name,
            self.this.props["indexdef"]
        ) + ";"

class DropIndex(BaseChange):
    priority = 0
    def __sql__(self):
        return "DROP INDEX %s.%s;" % (
            self.that.schema.name,
            self.that.name
        )

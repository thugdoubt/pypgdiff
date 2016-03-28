from tests.common import PgDiffTestCase

class SQLTableTestCase(PgDiffTestCase):
    def test_create_table(self):
        # table exists in schema 1 but not schema 2, add it
        from pypgdiff.objects import Database, Schema
        from pypgdiff.changes import CreateTable

        self.db1.cursor().execute("CREATE TABLE %s.foo (bar int, " % self.schema1 +
                                  "baz int NOT NULL DEFAULT 7, " +
                                  "baf char(1) DEFAULT 'test', " +
                                  "bat varchar(32) DEFAULT 'tab' " +
                                  ")")

        s1 = Schema(database=Database(conn=self.db1), name=self.schema1)
        s2 = Schema(database=Database(conn=self.db2), name=self.schema2)

        cs = s1 | s2

        # should have one change, and it's an CreateTable
        self.assertEqual(1, len(cs))
        self.assertEqual(CreateTable, type(cs[0]))
        self.assertEqual(
            "CREATE TABLE %s.foo (\n" % self.schema2 +
            "    bar integer,\n" +
            "    baz integer DEFAULT 7 NOT NULL,\n" +
            "    baf character(1) DEFAULT 'test'::bpchar,\n" +
            "    bat character varying(32) DEFAULT 'tab'::character varying\n" +
            ");",
            cs[0].sql
        )

    def test_drop_table(self):
        # table exists in schema 2 but not schema 1, drop it
        from pypgdiff.objects import Database, Schema
        from pypgdiff.changes import DropTable

        self.db2.cursor().execute("CREATE TABLE %s.foo (bar int)" % self.schema2)

        s1 = Schema(database=Database(conn=self.db1), name=self.schema1)
        s2 = Schema(database=Database(conn=self.db2), name=self.schema2)

        cs = s1 | s2

        # should have one change, and it's a DropTable
        self.assertEqual(1, len(cs))
        self.assertEqual(DropTable, type(cs[0]))
        self.assertEqual(
            "DROP TABLE %s.foo;" % self.schema2,
            cs[0].sql
        )

    def test_alter_table(self):
        # table exists in both schemas and they differ, alter it
        from pypgdiff.objects import Database, Schema
        from pypgdiff.changes import AlterTable
        from pypgdiff import Changeset

        s1 = Schema(database=Database(conn=self.db1), name=self.schema1)
        s2 = Schema(database=Database(conn=self.db2), name=self.schema2)

        self.db1.cursor().execute("CREATE TABLE %s.foo (bar varchar(32) NOT NULL DEFAULT 'bar', bat text)" % self.schema1)
        self.db2.cursor().execute("CREATE TABLE %s.foo (bar int, baz int)" % self.schema2)

        # test extending a changeset
        cs = Changeset()
        cs += s1 | s2

        # ch-ch-ch-changes!
        self.assertEqual(1, len(cs))
        self.assertEqual(AlterTable, type(cs[0]))
        self.assertEqual(
            "ALTER TABLE %s.foo\n" % self.schema2 +
            "    DROP COLUMN baz,\n" +
            "    ADD COLUMN bat text,\n" +
            "    ALTER COLUMN bar TYPE character varying(32),\n" +
            "    ALTER COLUMN bar SET DEFAULT 'bar'::character varying,\n" +
            "    ALTER COLUMN bar SET NOT NULL\n" +
            ";",
            cs[0].sql
        )

        # and back the other direction
        cs = s2 | s1

        self.assertEqual(1, len(cs))
        self.assertEqual(AlterTable, type(cs[0]))
        self.assertEqual(
            "ALTER TABLE %s.foo\n" % self.schema1 +
            "    ADD COLUMN baz integer,\n" +
            "    DROP COLUMN bat,\n" +
            "    ALTER COLUMN bar TYPE integer,\n" +
            "    ALTER COLUMN bar DROP DEFAULT,\n" +
            "    ALTER COLUMN bar DROP NOT NULL\n" +
            ";",
            cs[0].sql
        )

    def test_array_types(self):
        # test ARRAY types
        from pypgdiff.objects import Database, Schema
        from pypgdiff.changes import CreateTable

        self.db1.cursor().execute("CREATE TABLE %s.foo (" % self.schema1 +
                                  "int_basic int[], " +
                                  "int_nested int[][], " +
                                  "int_restricted int[3], " +
                                  "text_basic text[], " +
                                  "text_nested text[][], " +
                                  "char_basic char[], " +
                                  "char_restricted char(5)[], " +
                                  "numeric_basic numeric[], " +
                                  "numeric_restricted numeric(3,2)[] " +
                                  ")")

        s1 = Schema(database=Database(conn=self.db1), name=self.schema1)
        s2 = Schema(database=Database(conn=self.db2), name=self.schema2)

        cs = s1 | s2

        # should have one change, and it's an CreateTable
        self.assertEqual(1, len(cs))
        self.assertEqual(CreateTable, type(cs[0]))
        self.assertEqual(
            "CREATE TABLE %s.foo (\n" % self.schema2 +
            "    int_basic integer[],\n" +
            "    int_nested integer[],\n" +
            "    int_restricted integer[],\n" +
            "    text_basic text[],\n" +
            "    text_nested text[],\n" +
            "    char_basic character(1)[],\n" +
            "    char_restricted character(5)[],\n" +
            "    numeric_basic numeric[],\n" +
            "    numeric_restricted numeric(3,2)[]\n" +
            ");",
            cs[0].sql
        )

class SQLSequenceTestCase(PgDiffTestCase):
    def test_create_sequence(self):
        # sequence exists in schema 1 but not schema 2, add it
        from pypgdiff.objects import Database, Schema
        from pypgdiff.changes import CreateSequence

        creates = (
            (   "CREATE SEQUENCE %s.foo\n" % self.schema1 +
                "    START WITH 1\n" +
                "    INCREMENT BY 1\n" +
                "    NO MINVALUE\n" +
                "    NO MAXVALUE\n" +
                "    CACHE 1\n;",

                "DROP SEQUENCE %s.foo" % self.schema1
            ),

            (   "CREATE SEQUENCE %s.bar\n" % self.schema1 +
                "    START WITH 114\n" +
                "    INCREMENT BY 7\n" +
                "    MINVALUE 107\n" +
                "    MAXVALUE 121\n" +
                "    CACHE 4\n" +
                "    CYCLE\n;",

                "DROP SEQUENCE %s.bar" % self.schema1
            ),
        )

        c1 = self.db1.cursor()
        for create, drop in creates:
            c1.execute(create)

            s1 = Schema(database=Database(conn=self.db1), name=self.schema1)
            s2 = Schema(database=Database(conn=self.db2), name=self.schema2)

            cs = s1 | s2

            # should have one change, and it's an CreateSequence
            self.assertEqual(1, len(cs))
            self.assertEqual(CreateSequence, type(cs[0]))
            self.assertEqual(create.replace(self.schema1, self.schema2), cs[0].sql)

            c1.execute(drop)

    def test_drop_sequence(self):
        # sequence exists in schema 2 but not schema 1, drop it
        from pypgdiff.objects import Database, Schema
        from pypgdiff.changes import DropSequence

        self.db2.cursor().execute("CREATE SEQUENCE %s.foo" % self.schema2)

        s1 = Schema(database=Database(conn=self.db1), name=self.schema1)
        s2 = Schema(database=Database(conn=self.db2), name=self.schema2)

        cs = s1 | s2

        # should have one change, and it's a DropSequence
        self.assertEqual(1, len(cs))
        self.assertEqual(DropSequence, type(cs[0]))
        self.assertEqual(
            "DROP SEQUENCE %s.foo;" % self.schema2,
            cs[0].sql
        )

    def test_alter_sequence(self):
        # sequence exists in both schemas and they differ, alter it
        from pypgdiff.objects import Database, Schema
        from pypgdiff.changes import AlterSequence
        from pypgdiff import Changeset, Config

        s1 = Schema(database=Database(conn=self.db1), name=self.schema1)
        s2 = Schema(database=Database(conn=self.db2), name=self.schema2)

        c1 = self.db1.cursor()
        c1.execute("CREATE SEQUENCE %s.foo\n" % self.schema1 +
                   "    START WITH 10\n" +
                   "    INCREMENT BY 10\n"
                   "    MINVALUE 10\n" +
                   "    MAXVALUE 100\n" +
                   "    CACHE 2\n" +
                   "    NO CYCLE\n;")
        # move last_value to 20
        for i in range(2):
            c1.execute("SELECT nextval('%s.foo')" % self.schema1)
        ###
        c2 = self.db2.cursor()
        c2.execute("CREATE SEQUENCE %s.foo\n" % self.schema2 +
                   "    START WITH 1\n" +
                   "    INCREMENT BY 1\n"
                   "    NO MINVALUE\n" +
                   "    NO MAXVALUE\n" +
                   "    CACHE 1\n" +
                   "    CYCLE\n;")
        # move last_value to 25 (RESTART=26)
        for i in range(25):
            c2.execute("SELECT nextval('%s.foo')" % self.schema2)

        # check no_alter_sequence config
        with Config(no_alter_sequences=True):
            cs = s1 | s2
            self.assertEqual(0, len(cs))

        cs = s1 | s2

        self.assertEqual(1, len(cs))
        self.assertEqual(AlterSequence, type(cs[0]))
        self.assertEqual(
            "ALTER SEQUENCE %s.foo\n" % self.schema2 +
            "    START WITH 10\n" +
            "    RESTART 26\n" +
            "    INCREMENT BY 10\n"
            "    MINVALUE 10\n" +
            "    MAXVALUE 100\n" +
            "    CACHE 2\n" +
            "    NO CYCLE\n;",
            cs[0].sql
        )

        # and back in the other direction
        cs = s2 | s1

        self.assertEqual(1, len(cs))
        self.assertEqual(AlterSequence, type(cs[0]))
        self.assertEqual(
            "ALTER SEQUENCE %s.foo\n" % self.schema1 +
            "    START WITH 1\n" +
            "    RESTART 26\n" +
            "    INCREMENT BY 1\n"
            "    NO MINVALUE\n" +
            "    NO MAXVALUE\n" +
            "    CACHE 1\n" +
            "    CYCLE\n;",
            cs[0].sql
        )

    def test_restart_sequence(self):
        # sequence should only restart when the source schema starts higher
        from pypgdiff.objects import Database, Schema
        from pypgdiff.changes import AlterSequence

        c1 = self.db1.cursor()
        c2 = self.db2.cursor()

        c1.execute("CREATE SEQUENCE %s.foo" % self.schema1)
        c2.execute("CREATE SEQUENCE %s.foo" % self.schema2)
        for i in range(2):
            c1.execute("SELECT nextval('%s.foo')" % self.schema1)

        s1 = Schema(database=Database(conn=self.db1), name=self.schema1)
        s2 = Schema(database=Database(conn=self.db2), name=self.schema2)
        cs = s1 | s2

        self.assertEqual(1, len(cs))
        self.assertEqual(AlterSequence, type(cs[0]))
        self.assertEqual(
            "ALTER SEQUENCE %s.foo\n" % self.schema2 +
            "    RESTART 3\n;",
            cs[0].sql
        )

        # don't restart pointlessly
        c1.execute("DROP SEQUENCE %s.foo" % self.schema1)
        c2.execute("DROP SEQUENCE %s.foo" % self.schema2)
        c1.execute("CREATE SEQUENCE %s.foo" % self.schema1)
        c2.execute("CREATE SEQUENCE %s.foo" % self.schema2)
        for i in range(2):
            c2.execute("SELECT nextval('%s.foo')" % self.schema2)

        s1 = Schema(database=Database(conn=self.db1), name=self.schema1)
        s2 = Schema(database=Database(conn=self.db2), name=self.schema2)
        cs = s1 | s2

        self.assertEqual(0, len(cs))

        # and back in the other direction
        cs = s2 | s1

        self.assertEqual(1, len(cs))
        self.assertEqual(AlterSequence, type(cs[0]))
        self.assertEqual(
            "ALTER SEQUENCE %s.foo\n" % self.schema1 +
            "    RESTART 3\n;",
            cs[0].sql
        )

class SQLConstraintTestCase(PgDiffTestCase):
    def test_create_primary_key_constraint(self):
        # constraint exists in schema 1 but not schema 2, create it
        from pypgdiff.objects import Database, Schema
        from pypgdiff.changes import CreateConstraint

        # NOTE: this will break if postgres ever changes its scheme for generating names for these constraints
        c1 = self.db1.cursor()
        c2 = self.db2.cursor()
        c1.execute("CREATE TABLE %s.foo (bar int NOT NULL PRIMARY KEY, baz int NOT NULL)" % self.schema1)
        c2.execute("CREATE TABLE %s.foo (bar int NOT NULL, baz int NOT NULL)" % self.schema2)

        s1 = Schema(database=Database(conn=self.db1), name=self.schema1, cache=False)
        s2 = Schema(database=Database(conn=self.db2), name=self.schema2, cache=False)

        cs = s1 | s2

        # should have one change, and it's a CreateConstraint
        self.assertEqual(1, len(cs))
        self.assertEqual(CreateConstraint, type(cs[0]))
        self.assertEqual("PRIMARY KEY", cs[0].this.props["constraint_type"])
        self.assertEqual(
            "ALTER TABLE %s.foo\n" % self.schema2 +
            "    ADD CONSTRAINT foo_pkey PRIMARY KEY (bar);",
            cs[0].sql
        )

        # try a composite key
        c1.execute("ALTER TABLE %s.foo DROP CONSTRAINT foo_pkey" % self.schema1)
        c1.execute("ALTER TABLE %s.foo ADD CONSTRAINT foo_pkey PRIMARY KEY (bar, baz)" % self.schema1)

        cs = s1 | s2

        # should have one change, and it's a CreateConstraint
        self.assertEqual(1, len(cs))
        self.assertEqual(CreateConstraint, type(cs[0]))
        self.assertEqual("PRIMARY KEY", cs[0].this.props["constraint_type"])
        self.assertEqual(
            "ALTER TABLE %s.foo\n" % self.schema2 +
            "    ADD CONSTRAINT foo_pkey PRIMARY KEY (bar, baz);",
            cs[0].sql
        )

    def test_drop_primary_key_constraint(self):
        # constraint exists in schema 2 but not schema 1, drop it
        from pypgdiff.objects import Database, Schema
        from pypgdiff.changes import DropConstraint

        self.db1.cursor().execute("CREATE TABLE %s.foo (bar int NOT NULL)" % self.schema1)
        self.db2.cursor().execute("CREATE TABLE %s.foo (bar int NOT NULL PRIMARY KEY)" % self.schema2)

        s1 = Schema(database=Database(conn=self.db1), name=self.schema1)
        s2 = Schema(database=Database(conn=self.db2), name=self.schema2)

        cs = s1 | s2

        # should have one change, and it's a DropConstraint
        self.assertEqual(1, len(cs))
        self.assertEqual(DropConstraint, type(cs[0]))
        self.assertEqual("PRIMARY KEY", cs[0].that.props["constraint_type"])
        self.assertEqual(
            "ALTER TABLE %s.foo\n" % self.schema2 +
            "    DROP CONSTRAINT foo_pkey;",
            cs[0].sql
        )

    def test_create_unique_constraint(self):
        # constraint exists in schema 1 but not schema 2, create it
        from pypgdiff.objects import Database, Schema
        from pypgdiff.changes import CreateConstraint

        self.db1.cursor().execute("CREATE TABLE %s.foo (bar int, baz int, CONSTRAINT bar_baz_uniq UNIQUE (bar, baz))" % self.schema1)
        self.db2.cursor().execute("CREATE TABLE %s.foo (bar int, baz int)" % self.schema2)

        s1 = Schema(database=Database(conn=self.db1), name=self.schema1)
        s2 = Schema(database=Database(conn=self.db2), name=self.schema2)

        cs = s1 | s2

        # should have one change, and it's a CreateConstraint
        self.assertEqual(1, len(cs))
        self.assertEqual(CreateConstraint, type(cs[0]))
        self.assertEqual("UNIQUE", cs[0].this.props["constraint_type"])
        self.assertEqual(2, len(cs[0].this.props["columns"]))
        self.assertEqual(
            "ALTER TABLE %s.foo\n" % self.schema2 +
            "    ADD CONSTRAINT bar_baz_uniq UNIQUE (bar, baz);",
            cs[0].sql
        )

    def test_drop_unique_constraint(self):
        # constraint exists in schema 2 but not schema 1, drop it
        from pypgdiff.objects import Database, Schema
        from pypgdiff.changes import DropConstraint

        self.db1.cursor().execute("CREATE TABLE %s.foo (bar int, baz int)" % self.schema1)
        self.db2.cursor().execute("CREATE TABLE %s.foo (bar int, baz int, CONSTRAINT bar_baz_uniq UNIQUE (bar, baz))" % self.schema2)

        s1 = Schema(database=Database(conn=self.db1), name=self.schema1)
        s2 = Schema(database=Database(conn=self.db2), name=self.schema2)

        cs = s1 | s2

        # should have one change, and it's a DropConstraint
        self.assertEqual(1, len(cs))
        self.assertEqual(DropConstraint, type(cs[0]))
        self.assertEqual("UNIQUE", cs[0].that.props["constraint_type"])
        self.assertEqual(
            "ALTER TABLE %s.foo\n" % self.schema2 +
            "    DROP CONSTRAINT bar_baz_uniq;",
            cs[0].sql
        )

    def test_create_foreign_key_constraint(self):
        # constraint exists in schema 1 but not schema 2, create it
        from pypgdiff.objects import Database, Schema
        from pypgdiff.changes import CreateConstraint

        c1 = self.db1.cursor()
        c2 = self.db2.cursor()

        c1.execute("CREATE TABLE %s.target (pk int NOT NULL PRIMARY KEY)" % self.schema1)
        c1.execute("CREATE TABLE %s.source (pk int NOT NULL PRIMARY KEY, fk int NOT NULL, " % self.schema1 +
                   "CONSTRAINT fk_foo FOREIGN KEY (fk) REFERENCES %s.target (pk)" % self.schema1 +
                   "DEFERRABLE INITIALLY DEFERRED)")
        c2.execute("CREATE TABLE %s.target (pk int NOT NULL PRIMARY KEY)" % self.schema2)
        c2.execute("CREATE TABLE %s.source (pk int NOT NULL PRIMARY KEY, fk int NOT NULL)" % self.schema2)

        s1 = Schema(database=Database(conn=self.db1), name=self.schema1)
        s2 = Schema(database=Database(conn=self.db2), name=self.schema2)

        cs = s1 | s2

        # should have one change, and it's a CreateConstraint
        self.assertEqual(1, len(cs))
        self.assertEqual(CreateConstraint, type(cs[0]))
        self.assertEqual("FOREIGN KEY", cs[0].this.props["constraint_type"])
        self.assertEqual(
            "ALTER TABLE %s.source\n" % self.schema2 +
            "    ADD CONSTRAINT fk_foo FOREIGN KEY (fk) REFERENCES target(pk) DEFERRABLE INITIALLY DEFERRED;",
            cs[0].sql
        )

    def test_drop_foreign_key_constraint(self):
        # constraint exists in schema 2 but not schema 1, drop it
        from pypgdiff.objects import Database, Schema
        from pypgdiff.changes import DropConstraint

        c1 = self.db1.cursor()
        c2 = self.db2.cursor()

        c1.execute("CREATE TABLE %s.target (pk int NOT NULL PRIMARY KEY)" % self.schema1)
        c1.execute("CREATE TABLE %s.source (pk int NOT NULL PRIMARY KEY, fk int NOT NULL)" % self.schema1)
        c2.execute("CREATE TABLE %s.target (pk int NOT NULL PRIMARY KEY)" % self.schema2)
        c2.execute("CREATE TABLE %s.source (pk int NOT NULL PRIMARY KEY, fk int NOT NULL, " % self.schema2 +
                   "CONSTRAINT fk_foo FOREIGN KEY (fk) REFERENCES %s.target (pk))" % self.schema2)

        s1 = Schema(database=Database(conn=self.db1), name=self.schema1)
        s2 = Schema(database=Database(conn=self.db2), name=self.schema2)

        cs = s1 | s2

        # should have one change, and it's a DropConstraint
        self.assertEqual(1, len(cs))
        self.assertEqual(DropConstraint, type(cs[0]))
        self.assertEqual("FOREIGN KEY", cs[0].that.props["constraint_type"])
        self.assertEqual(
            "ALTER TABLE %s.source\n" % self.schema2 +
            "    DROP CONSTRAINT fk_foo;",
            cs[0].sql
        )

    def test_create_check_constraint(self):
        # constraint exists in schema 1 but not schema 2, create it
        from pypgdiff.objects import Database, Schema
        from pypgdiff.changes import CreateConstraint

        self.db1.cursor().execute("CREATE TABLE %s.foo (bar int, CONSTRAINT c CHECK ((bar >= 0)))" % self.schema1)
        self.db2.cursor().execute("CREATE TABLE %s.foo (bar int)" % self.schema2)

        s1 = Schema(database=Database(conn=self.db1), name=self.schema1)
        s2 = Schema(database=Database(conn=self.db2), name=self.schema2)

        cs = s1 | s2

        # should have one change, and it's a CreateConstraint
        self.assertEqual(1, len(cs))
        self.assertEqual(CreateConstraint, type(cs[0]))
        self.assertEqual("CHECK", cs[0].this.props["constraint_type"])
        self.assertEqual(
            "ALTER TABLE %s.foo\n" % self.schema2 +
            "    ADD CONSTRAINT c CHECK ((bar >= 0));",
            cs[0].sql
        )

    def test_drop_check_constraint(self):
        # constraint exists in schema 2 but not schema 1, drop it
        from pypgdiff.objects import Database, Schema
        from pypgdiff.changes import DropConstraint

        self.db1.cursor().execute("CREATE TABLE %s.foo (bar int)" % self.schema1)
        self.db2.cursor().execute("CREATE TABLE %s.foo (bar int, CONSTRAINT c CHECK ((bar >= 0)))" % self.schema2)

        s1 = Schema(database=Database(conn=self.db1), name=self.schema1)
        s2 = Schema(database=Database(conn=self.db2), name=self.schema2)

        cs = s1 | s2

        # should have one change, and it's a DropConstraint
        self.assertEqual(1, len(cs))
        self.assertEqual(DropConstraint, type(cs[0]))
        self.assertEqual("CHECK", cs[0].that.props["constraint_type"])
        self.assertEqual(
            "ALTER TABLE %s.foo\n" % self.schema2 +
            "    DROP CONSTRAINT c;",
            cs[0].sql
        )

class SQLIndexTestCase(PgDiffTestCase):
    def test_create_index(self):
        # index exists in schema 1 but not schema 2, add it
        from pypgdiff.objects import Database, Schema
        from pypgdiff.changes import CreateIndex

        self.db1.cursor().execute("CREATE TABLE %s.foo (bar int, baz int)" % self.schema1)
        self.db1.cursor().execute("CREATE INDEX test_index ON %s.foo USING btree (baz)" % self.schema1)
        self.db2.cursor().execute("CREATE TABLE %s.foo (bar int, baz int)" % self.schema2)

        s1 = Schema(database=Database(conn=self.db1), name=self.schema1)
        s2 = Schema(database=Database(conn=self.db2), name=self.schema2)

        cs = s1 | s2

        # should have one change, and it's an CreateIndex
        self.assertEqual(1, len(cs))
        self.assertEqual(CreateIndex, type(cs[0]))
        self.assertEqual(
            "CREATE INDEX test_index ON %s.foo USING btree (baz);" % self.schema2,
            cs[0].sql
        )

    def test_drop_index(self):
        # index exists in schema 2 but not schema 1, drop it
        from pypgdiff.objects import Database, Schema
        from pypgdiff.changes import DropIndex

        self.db1.cursor().execute("CREATE TABLE %s.foo (bar int, baz int)" % self.schema1)
        self.db2.cursor().execute("CREATE TABLE %s.foo (bar int, baz int)" % self.schema2)
        self.db2.cursor().execute("CREATE INDEX test_index ON %s.foo USING btree (baz)" % self.schema2)

        s1 = Schema(database=Database(conn=self.db1), name=self.schema1)
        s2 = Schema(database=Database(conn=self.db2), name=self.schema2)

        cs = s1 | s2

        # should have one change, and it's an DropIndex
        self.assertEqual(1, len(cs))
        self.assertEqual(DropIndex, type(cs[0]))
        self.assertEqual(
            "DROP INDEX %s.test_index;" % self.schema2,
            cs[0].sql
        )

    def test_alter_index(self):
        # alter index should result in a DROP/CREATE
        from pypgdiff.objects import Database, Schema
        from pypgdiff.changes import CreateIndex, DropIndex

        self.db1.cursor().execute("CREATE TABLE %s.foo (bar int, baz int)" % self.schema1)
        self.db1.cursor().execute("CREATE INDEX test_index ON %s.foo USING btree (baz)" % self.schema1)
        self.db2.cursor().execute("CREATE TABLE %s.foo (bar int, baz int)" % self.schema2)
        self.db2.cursor().execute("CREATE INDEX test_index ON %s.foo USING btree (bar, baz)" % self.schema2)

        s1 = Schema(database=Database(conn=self.db1), name=self.schema1)
        s2 = Schema(database=Database(conn=self.db2), name=self.schema2)

        cs = s1 | s2

        # should have 2 changes, a DropIndex and CreateIndex
        self.assertEqual(2, len(cs))
        self.assertEqual(DropIndex, type(cs[0]))
        self.assertEqual(CreateIndex, type(cs[1]))
        self.assertEqual(
            "DROP INDEX %s.test_index;" % self.schema2,
            cs[0].sql
        )
        self.assertEqual(
            "CREATE INDEX test_index ON %s.foo USING btree (baz);" % self.schema2,
            cs[1].sql
        )

class SQLReservedTestCase(PgDiffTestCase):
    def test_reserved_quoting(self):
        # make sure special words get quoted
        from pypgdiff.objects import Database, Schema
        from pypgdiff.changes import CreateTable

        self.db1.cursor().execute("CREATE TABLE %s.foo (\"select\" int, " % self.schema1 +
                                  "\"order\" int NOT NULL DEFAULT 7, " +
                                  "\"create\" char(1) DEFAULT 'test', " +
                                  "\"unique\" varchar(32) DEFAULT 'tab' " +
                                  ")")

        s1 = Schema(database=Database(conn=self.db1), name=self.schema1)
        s2 = Schema(database=Database(conn=self.db2), name=self.schema2)

        cs = s1 | s2

        # should have one change, and it's an CreateTable
        self.assertEqual(1, len(cs))
        self.assertEqual(CreateTable, type(cs[0]))
        self.assertEqual(
            "CREATE TABLE %s.foo (\n" % self.schema2 +
            "    \"select\" integer,\n" +
            "    \"order\" integer DEFAULT 7 NOT NULL,\n" +
            "    \"create\" character(1) DEFAULT 'test'::bpchar,\n" +
            "    \"unique\" character varying(32) DEFAULT 'tab'::character varying\n" +
            ");",
            cs[0].sql
        )

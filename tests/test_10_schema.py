from tests.common import PgDiffTestCase

class SchemaTableTestCase(PgDiffTestCase):
    def test_create_table(self):
        # table exists in schema 1 but not schema 2, create it
        from pypgdiff.objects import Database, Schema
        from pypgdiff.changes import CreateTable

        self.db1.cursor().execute("CREATE TABLE %s.foo (bar int)" % self.schema1)

        s1 = Schema(database=Database(conn=self.db1), name=self.schema1)
        s2 = Schema(database=Database(conn=self.db2), name=self.schema2)

        cs = s1 | s2

        # should have one change, and it's an CreateTable
        self.assertEqual(1, len(cs))
        self.assertEqual(CreateTable, type(cs[0]))

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

    def test_alter_table(self):
        # table exists in both schemas and they differ, alter it
        from pypgdiff.objects import Database, Schema
        from pypgdiff.changes import AlterTable, AddColumn, DropColumn, AlterColumn
        from pypgdiff import Changeset

        s1 = Schema(database=Database(conn=self.db1), name=self.schema1, cache=False)
        s2 = Schema(database=Database(conn=self.db2), name=self.schema2, cache=False)

        ### No Changes
        self.db1.cursor().execute("CREATE TABLE %s.foo (bar int)" % self.schema1)
        self.db2.cursor().execute("CREATE TABLE %s.foo (bar int)" % self.schema2)

        cs = s1 | s2

        # should have 0 changes
        self.assertEqual(0, len(cs))

        ### DropColumn
        self.db1.cursor().execute("DROP TABLE %s.foo" % self.schema1)
        self.db2.cursor().execute("DROP TABLE %s.foo" % self.schema2)
        self.db1.cursor().execute("CREATE TABLE %s.foo (bar int)" % self.schema1)
        self.db2.cursor().execute("CREATE TABLE %s.foo (bar int, baz int)" % self.schema2)

        # test extending a changeset
        cs = Changeset()
        cs += s1 | s2

        # should have one change, and it's a AlterTable (DropColumn)
        self.assertEqual(1, len(cs))
        self.assertEqual(AlterTable, type(cs[0]))
        self.assertEqual(1, len(cs[0]))
        self.assertEqual(DropColumn, type(cs[0][0]))

        ### AddColumn
        self.db1.cursor().execute("DROP TABLE %s.foo" % self.schema1)
        self.db2.cursor().execute("DROP TABLE %s.foo" % self.schema2)
        self.db1.cursor().execute("CREATE TABLE %s.foo (bar int, baz int)" % self.schema1)
        self.db2.cursor().execute("CREATE TABLE %s.foo (bar int)" % self.schema2)

        cs = s1 | s2

        # should have one change, and it's a AlterTable (AddColumn)
        self.assertEqual(1, len(cs))
        self.assertEqual(AlterTable, type(cs[0]))
        self.assertEqual(1, len(cs[0]))
        self.assertEqual(AddColumn, type(cs[0][0]))

        ### AlterColumn
        self.db1.cursor().execute("DROP TABLE %s.foo" % self.schema1)
        self.db2.cursor().execute("DROP TABLE %s.foo" % self.schema2)
        self.db1.cursor().execute("CREATE TABLE %s.foo (bar int)" % self.schema1)
        self.db2.cursor().execute("CREATE TABLE %s.foo (bar char)" % self.schema2)

        cs = s1 | s2

        # should have one change, and it's a AlterTable (AlterColumn)
        self.assertEqual(1, len(cs))
        self.assertEqual(AlterTable, type(cs[0]))
        self.assertEqual(1, len(cs[0]))
        self.assertEqual(AlterColumn, type(cs[0][0]))

    def test_same_table(self):
        # table exists in both schemas and they match, no changes
        from pypgdiff.objects import Database, Schema

        self.db1.cursor().execute("CREATE TABLE %s.foo (bar int)" % self.schema1)
        self.db2.cursor().execute("CREATE TABLE %s.foo (bar int)" % self.schema2)

        s1 = Schema(database=Database(conn=self.db1), name=self.schema1)
        s2 = Schema(database=Database(conn=self.db2), name=self.schema2)

        cs = s1 | s2

        # should be no changes
        self.assertEqual(0, len(cs))

class SchemaSequenceTestCase(PgDiffTestCase):
    def test_create_sequence(self):
        # sequence exists in schema 1 but not schema 2, add it
        from pypgdiff.objects import Database, Schema
        from pypgdiff.changes import CreateSequence

        self.db1.cursor().execute("CREATE SEQUENCE %s.foo" % self.schema1)

        s1 = Schema(database=Database(conn=self.db1), name=self.schema1)
        s2 = Schema(database=Database(conn=self.db2), name=self.schema2)

        cs = s1 | s2

        # should have one change, and it's an CreateSequence
        self.assertEqual(1, len(cs))
        self.assertEqual(CreateSequence, type(cs[0]))

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

    def test_alter_sequence(self):
        # sequence exists in both schemas and they differ, alter it
        from pypgdiff.objects import Database, Schema
        from pypgdiff.changes import AlterSequence

        s1 = Schema(database=Database(conn=self.db1), name=self.schema1)
        s2 = Schema(database=Database(conn=self.db2), name=self.schema2)

        self.db1.cursor().execute("CREATE SEQUENCE %s.foo\n" % self.schema1 +
                                  "    START WITH 10\n" +
                                  "    INCREMENT BY 10\n"
                                  "    MINVALUE 10\n" +
                                  "    MAXVALUE 100\n" +
                                  "    CACHE 2\n" +
                                  "    NO CYCLE\n;")
        self.db2.cursor().execute("CREATE SEQUENCE %s.foo\n" % self.schema2 +
                                  "    START WITH 1\n" +
                                  "    INCREMENT BY 1\n"
                                  "    NO MINVALUE\n" +
                                  "    NO MAXVALUE\n" +
                                  "    CACHE 1\n" +
                                  "    CYCLE\n;")

        cs = s1 | s2

        self.assertEqual(1, len(cs))
        self.assertEqual(AlterSequence, type(cs[0]))

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

    def test_same_sequence(self):
        # sequence exists in both schemas and they match, no changes
        from pypgdiff.objects import Database, Schema
        from pypgdiff.changes import CreateSequence

        self.db1.cursor().execute("CREATE SEQUENCE %s.foo" % self.schema1)
        self.db2.cursor().execute("CREATE SEQUENCE %s.foo" % self.schema2)

        s1 = Schema(database=Database(conn=self.db1), name=self.schema1)
        s2 = Schema(database=Database(conn=self.db2), name=self.schema2)

        cs = s1 | s2

        # should be no changes
        self.assertEqual(0, len(cs))

class SchemaConstraintTestCase(PgDiffTestCase):
    def test_create_primary_key_constraint(self):
        # constraint exists in schema 1 but not schema 2, create it
        from pypgdiff.objects import Database, Schema
        from pypgdiff.changes import CreateConstraint

        self.db1.cursor().execute("CREATE TABLE %s.foo (bar int NOT NULL PRIMARY KEY)" % self.schema1)
        self.db2.cursor().execute("CREATE TABLE %s.foo (bar int NOT NULL)" % self.schema2)

        s1 = Schema(database=Database(conn=self.db1), name=self.schema1)
        s2 = Schema(database=Database(conn=self.db2), name=self.schema2)

        cs = s1 | s2

        # should have one change, and it's a CreateConstraint
        self.assertEqual(1, len(cs))
        self.assertEqual(CreateConstraint, type(cs[0]))
        self.assertEqual("PRIMARY KEY", cs[0].this.props["constraint_type"])

    def test_drop_primary_key_constraint(self):
        # constraint exists in schema 2 but not schema 1, drop it
        from pypgdiff.objects import Database, Schema
        from pypgdiff.changes import DropConstraint, DropIndex

        self.db1.cursor().execute("CREATE TABLE %s.foo (bar int NOT NULL)" % self.schema1)
        self.db2.cursor().execute("CREATE TABLE %s.foo (bar int NOT NULL PRIMARY KEY)" % self.schema2)

        s1 = Schema(database=Database(conn=self.db1), name=self.schema1)
        s2 = Schema(database=Database(conn=self.db2), name=self.schema2)

        cs = s1 | s2

        # should have one change, and it's a DropConstraint
        self.assertEqual(1, len(cs))
        self.assertEqual(DropConstraint, type(cs[0]))
        self.assertEqual("PRIMARY KEY", cs[0].that.props["constraint_type"])

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

    def test_create_foreign_key_constraint(self):
        # constraint exists in schema 1 but not schema 2, create it
        from pypgdiff.objects import Database, Schema
        from pypgdiff.changes import CreateConstraint

        c1 = self.db1.cursor()
        c2 = self.db2.cursor()

        c1.execute("CREATE TABLE %s.target (pk int NOT NULL PRIMARY KEY)" % self.schema1)
        c1.execute("CREATE TABLE %s.source (pk int NOT NULL PRIMARY KEY, fk int NOT NULL, " % self.schema1 +
                   "CONSTRAINT fk_foo FOREIGN KEY (fk) REFERENCES %s.target (pk))" % self.schema1)
        c2.execute("CREATE TABLE %s.target (pk int NOT NULL PRIMARY KEY)" % self.schema2)
        c2.execute("CREATE TABLE %s.source (pk int NOT NULL PRIMARY KEY, fk int NOT NULL)" % self.schema2)

        s1 = Schema(database=Database(conn=self.db1), name=self.schema1)
        s2 = Schema(database=Database(conn=self.db2), name=self.schema2)

        cs = s1 | s2

        # should have one change, and it's a CreateConstraint
        self.assertEqual(1, len(cs))
        self.assertEqual(CreateConstraint, type(cs[0]))
        self.assertEqual("FOREIGN KEY", cs[0].this.props["constraint_type"])

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

    def test_alter_constraint(self):
        # constraint with the same name in 2 schemas but differnt thingies
        from pypgdiff.objects import Database, Schema
        from pypgdiff.changes import DropConstraint, CreateConstraint

        self.db1.cursor().execute("CREATE TABLE %s.foo (bar int, CONSTRAINT c CHECK ((bar < 100)))" % self.schema1)
        self.db2.cursor().execute("CREATE TABLE %s.foo (bar int, CONSTRAINT c CHECK ((bar >= 0)))" % self.schema2)

        s1 = Schema(database=Database(conn=self.db1), name=self.schema1)
        s2 = Schema(database=Database(conn=self.db2), name=self.schema2)

        cs = s1 | s2

        # should have one change, and it's a DropConstraint
        self.assertEqual(2, len(cs))
        self.assertEqual(DropConstraint, type(cs[0]))
        self.assertEqual("CHECK", cs[0].that.props["constraint_type"])
        self.assertEqual(CreateConstraint, type(cs[1]))
        self.assertEqual("CHECK", cs[1].that.props["constraint_type"])

    def test_same_constraint(self):
        from pypgdiff.objects import Database, Schema
        from pypgdiff.changes import CreateConstraint

        self.db1.cursor().execute("CREATE TABLE %s.foo (bar int NOT NULL PRIMARY KEY)" % self.schema1)
        self.db2.cursor().execute("CREATE TABLE %s.foo (bar int NOT NULL PRIMARY KEY)" % self.schema2)

        s1 = Schema(database=Database(conn=self.db1), name=self.schema1)
        s2 = Schema(database=Database(conn=self.db2), name=self.schema2)

        cs = s1 | s2

        # should have no changes
        self.assertEqual(0, len(cs))

    def test_normalized_constraints_primary_key(self):
        # PRIMARY KEY constraint with different name in 2 schemas but same thingies
        from pypgdiff import Config
        from pypgdiff.objects import Database, Schema

        c1 = self.db1.cursor()
        c2 = self.db2.cursor()

        c1.execute("CREATE TABLE %s.foo (bar int NOT NULL PRIMARY KEY)" % self.schema1)
        c2.execute("CREATE TABLE %s.foo (bar int NOT NULL PRIMARY KEY)" % self.schema2)
        c2.execute("ALTER INDEX %s.foo_pkey RENAME TO wubba_lubba_dub_dub_pkey" % self.schema2)

        s1 = Schema(database=Database(conn=self.db1), name=self.schema1)
        s2 = Schema(database=Database(conn=self.db2), name=self.schema2)
        cs = s1 | s2
        self.assertEqual(2, len(cs))

        with Config(normalize_constraints=True):
            s1 = Schema(database=Database(conn=self.db1), name=self.schema1)
            s2 = Schema(database=Database(conn=self.db2), name=self.schema2)
            cs = s1 | s2
            self.assertEqual(0, len(cs))

    def test_normalized_constraints_unique(self):
        # UNIQUE constraint with different name in 2 schemas but same thingies
        from pypgdiff import Config
        from pypgdiff.objects import Database, Schema

        self.db1.cursor().execute("CREATE TABLE %s.foo (bar int, CONSTRAINT bar_uniq_one UNIQUE (bar))" % self.schema1)
        self.db2.cursor().execute("CREATE TABLE %s.foo (bar int, CONSTRAINT bar_uniq_two UNIQUE (bar))" % self.schema2)

        s1 = Schema(database=Database(conn=self.db1), name=self.schema1)
        s2 = Schema(database=Database(conn=self.db2), name=self.schema2)
        cs = s1 | s2
        self.assertEqual(2, len(cs))

        with Config(normalize_constraints=True):
            s1 = Schema(database=Database(conn=self.db1), name=self.schema1)
            s2 = Schema(database=Database(conn=self.db2), name=self.schema2)
            cs = s1 | s2
            self.assertEqual(0, len(cs))

    def test_normalized_constraints_foreign_key(self):
        # PRIMARY KEY constraint with different name in 2 schemas but same thingies
        from pypgdiff import Config
        from pypgdiff.objects import Database, Schema

        c1 = self.db1.cursor()
        c2 = self.db2.cursor()

        c1.execute("CREATE TABLE %s.target (pk int NOT NULL PRIMARY KEY)" % self.schema1)
        c1.execute("CREATE TABLE %s.source (pk int NOT NULL PRIMARY KEY, fk int NOT NULL, " % self.schema1 +
                   "CONSTRAINT fk_one FOREIGN KEY (fk) REFERENCES %s.target (pk))" % self.schema1)
        c2.execute("CREATE TABLE %s.target (pk int NOT NULL PRIMARY KEY)" % self.schema2)
        c2.execute("CREATE TABLE %s.source (pk int NOT NULL PRIMARY KEY, fk int NOT NULL, " % self.schema2 +
                   "CONSTRAINT fk_two FOREIGN KEY (fk) REFERENCES %s.target (pk))" % self.schema2)

        s1 = Schema(database=Database(conn=self.db1), name=self.schema1)
        s2 = Schema(database=Database(conn=self.db2), name=self.schema2)
        cs = s1 | s2
        self.assertEqual(2, len(cs))

        with Config(normalize_constraints=True):
            s1 = Schema(database=Database(conn=self.db1), name=self.schema1)
            s2 = Schema(database=Database(conn=self.db2), name=self.schema2)
            cs = s1 | s2
            self.assertEqual(0, len(cs))

    def test_normalized_constraints_check(self):
        # CHECK constraint with different name in 2 schemas but same thingies
        from pypgdiff import Config
        from pypgdiff.objects import Database, Schema

        self.db1.cursor().execute("CREATE TABLE %s.foo (bar int, CONSTRAINT c_one CHECK ((bar >= 0)))" % self.schema1)
        self.db2.cursor().execute("CREATE TABLE %s.foo (bar int, CONSTRAINT c_two CHECK ((bar >= 0)))" % self.schema2)

        s1 = Schema(database=Database(conn=self.db1), name=self.schema1)
        s2 = Schema(database=Database(conn=self.db2), name=self.schema2)
        cs = s1 | s2
        self.assertEqual(2, len(cs))

        with Config(normalize_constraints=True):
            s1 = Schema(database=Database(conn=self.db1), name=self.schema1)
            s2 = Schema(database=Database(conn=self.db2), name=self.schema2)
            cs = s1 | s2
            self.assertEqual(0, len(cs))

class SchemaIndexTestCase(PgDiffTestCase):
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

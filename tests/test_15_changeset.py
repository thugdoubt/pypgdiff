from tests.common import PgDiffTestCase

class ChangesetSortingTestCase(PgDiffTestCase):
    def test_sorting_changeset(self):
        from random import shuffle
        from pypgdiff.objects import Database, Schema
        from pypgdiff import Changeset, changes

        # expected order of operations:
        # 00 DropIndex
        # 10 DropConstraint
        # 20 DropTable
        # 30 DropSequence
        # 40 CreateSequence
        # 50 AlterSequence
        # 60 CreateTable
        # -- AlterTable
        # 71   DropColumn
        # 72   AddColumn
        # 73   AlterColumn
        # 80 CreateConstraint (80 - 85)
        # 90 CreateIndex

        s1 = Schema(database=Database(conn=self.db1), name=self.schema1)

        expected = Changeset()
        for cls in ("DropIndex",
                    "DropConstraint",
                    "DropTable",
                    "DropSequence",
                    "CreateSequence",
                    "AlterSequence",
                    "CreateTable",
                    "DropColumn",
                    "AddColumn",
                    "AlterColumn",
                    "CreateConstraint",
                    "CreateIndex"):
            expected += getattr(changes, cls)(s1, None)

        shuffled = expected[:]
        shuffle(shuffled)
        shuffled = Changeset(*shuffled)
        self.assertEqual(
            [x for x in expected],
            [x for x in sorted(shuffled)]
        )

    def test_sorting_live_changeset(self):
        from pypgdiff.objects import Database, Schema
        from pypgdiff import Changeset, changes

        s1 = Schema(database=Database(conn=self.db1), name=self.schema1)
        s2 = Schema(database=Database(conn=self.db2), name=self.schema2)

        expected = Changeset()
        for cls in ("DropIndex",
                    "DropConstraint",
                    "DropTable",
                    "DropSequence",
                    "CreateSequence",
                    "AlterSequence",
                    "CreateTable",
                    "AlterTable",
                    "CreateConstraint",
                    "CreateIndex"):
            expected += getattr(changes, cls)(s1, None)

        c1 = self.db1.cursor()
        c2 = self.db2.cursor()

        # CreateTable
        # CreateConstraint
        # CreateIndex
        c1.execute("CREATE TABLE %s.createme_table (" % self.schema1 +
                   "     bar int NOT NULL PRIMARY KEY" +
                   ")")

        # CreateSequence
        c1.execute("CREATE SEQUENCE %s.createme_seq" % self.schema1)

        # AlterTable
        c1.execute("CREATE TABLE %s.alterme_table (" % self.schema1 +
                   "    createme_col int," +
                   "    alterme_col int" +
                   ")")
        c2.execute("CREATE TABLE %s.alterme_table (" % self.schema2 +
                   "    dropme_col int," +
                   "    alterme_col varchar" +
                   ")")

        # CreateIndex
        # DropIndex
        c1.execute("CREATE INDEX alterme_idx ON %s.alterme_table USING btree (createme_col)" % self.schema1)
        c2.execute("CREATE INDEX alterme_idx ON %s.alterme_table USING btree (alterme_col)" % self.schema2)

        # AlterSequence
        c1.execute("CREATE SEQUENCE %s.alterme_seq INCREMENT BY 5" % self.schema1)
        c2.execute("CREATE SEQUENCE %s.alterme_seq INCREMENT BY 8" % self.schema2)

        # DropConstraint
        # DropTable
        # DropIndex
        c2.execute("CREATE TABLE %s.dropme_table (" % self.schema2 +
                   "     bar int NOT NULL PRIMARY KEY" +
                   ")")

        # DropSequence
        c2.execute("CREATE SEQUENCE %s.dropme_seq" % self.schema2)

        cs = s1 | s2

        self.assertEqual(
            [x.__class__ for x in expected],
            [x.__class__ for x in cs]
        )

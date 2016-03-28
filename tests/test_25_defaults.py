from tests.common import PgDiffTestCase
import mock

class DefaultTestCase(PgDiffTestCase):
    def test_collect_default_int(self):
        from pypgdiff.objects import Database, Schema
        from pypgdiff import Config

        c1 = self.db1.cursor()
        c2 = self.db2.cursor()

        c1.execute("CREATE TABLE %s.foo (bar int NOT NULL)" % self.schema1)
        c2.execute("CREATE TABLE %s.foo (bar int)" % self.schema2)

        with Config(prompt_for_defaults=True):
            s1 = Schema(database=Database(conn=self.db1), name=self.schema1)
            s2 = Schema(database=Database(conn=self.db2), name=self.schema2)
            with mock.patch("__builtin__.raw_input", return_value="7"):
                cs = s1 | s2

        self.assertEqual(1, len(cs))
        self.assertEqual(
            "ALTER TABLE %s.foo\n" % self.schema2 +
            "    ALTER COLUMN bar SET DEFAULT 7,\n" +
            "    ALTER COLUMN bar SET NOT NULL\n" +
            ";",
            cs[0].sql
        )

    def test_collect_default_str(self):
        from pypgdiff.objects import Database, Schema
        from pypgdiff import Config

        c1 = self.db1.cursor()
        c2 = self.db2.cursor()

        c1.execute("CREATE TABLE %s.foo (bar varchar(10) NOT NULL)" % self.schema1)
        c2.execute("CREATE TABLE %s.foo (bar varchar(10))" % self.schema2)

        with Config(prompt_for_defaults=True):
            s1 = Schema(database=Database(conn=self.db1), name=self.schema1)
            s2 = Schema(database=Database(conn=self.db2), name=self.schema2)
            with mock.patch("__builtin__.raw_input", return_value="'foo'"):
                cs = s1 | s2

        self.assertEqual(1, len(cs))
        self.assertEqual(
            "ALTER TABLE %s.foo\n" % self.schema2 +
            "    ALTER COLUMN bar SET DEFAULT 'foo',\n" +
            "    ALTER COLUMN bar SET NOT NULL\n" +
            ";",
            cs[0].sql
        )

    def test_collect_default_now(self):
        from pypgdiff.objects import Database, Schema
        from pypgdiff import Config

        c1 = self.db1.cursor()
        c2 = self.db2.cursor()

        c1.execute("CREATE TABLE %s.foo (bar timestamp NOT NULL)" % self.schema1)
        c2.execute("CREATE TABLE %s.foo (bar timestamp)" % self.schema2)

        with Config(prompt_for_defaults=True):
            s1 = Schema(database=Database(conn=self.db1), name=self.schema1)
            s2 = Schema(database=Database(conn=self.db2), name=self.schema2)
            with mock.patch("__builtin__.raw_input", return_value="NOW"):
                cs = s1 | s2

        self.assertEqual(1, len(cs))
        self.assertEqual(
            "ALTER TABLE %s.foo\n" % self.schema2 +
            "    ALTER COLUMN bar SET DEFAULT NOW(),\n" +
            "    ALTER COLUMN bar SET NOT NULL\n" +
            ";",
            cs[0].sql
        )

    def test_collect_default_datetime(self):
        from pypgdiff.objects import Database, Schema
        from pypgdiff import Config
        import datetime

        c1 = self.db1.cursor()
        c2 = self.db2.cursor()

        c1.execute("CREATE TABLE %s.foo (bar timestamp NOT NULL)" % self.schema1)
        c2.execute("CREATE TABLE %s.foo (bar timestamp)" % self.schema2)

        now = datetime.datetime.now()
        with Config(prompt_for_defaults=True):
            s1 = Schema(database=Database(conn=self.db1), name=self.schema1)
            s2 = Schema(database=Database(conn=self.db2), name=self.schema2)
            with mock.patch("__builtin__.raw_input", return_value="datetime.datetime.now()"):
                cs = s1 | s2

        self.assertEqual(1, len(cs))
        self.assertIn(
            "ALTER TABLE %s.foo\n" % self.schema2 +
            "    ALTER COLUMN bar SET DEFAULT '" + now.strftime("%Y-%m-%dT%H:%M:"),
            cs[0].sql
        )

from unittest import TestCase

class ConfigTestCase(TestCase):
    def test_config(self):
        # Config should only populate when invoked as a context manager
        from pypgdiff import Config

        c = Config(foo="bar")
        self.assertEqual({}, c.conf)

        with Config(hi="there") as c:
            self.assertEqual({ "hi": "there" }, c.conf)

            z = Config(foo="bar")
            self.assertEqual({ "hi": "there" }, z.conf)

        c = Config(bar="baz")
        self.assertEqual({}, c.conf)

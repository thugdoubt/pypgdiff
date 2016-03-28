class Undefined(object):
    def __new__(cls):
        if not hasattr(cls, '_instance'):
            cls._instance = super(Undefined, cls).__new__(cls)
        return cls._instance

    def __bool__(self):
        return False
    __nonzero__=__bool__

    def __eq__(self, other):
        raise TypeError("Can't compare Undefined")
    __ne__=__eq__
__builtins__['Undefined'] = Undefined()

class NOW(object):
    def __repr__(self):
        return "NOW()"

class Changeset(object):
    def __init__(self, *changes):
        self.changes = list(changes)

    def __len__(self):
        return len(self.changes)

    def __bool__(self):
        return bool(len(self))
    __nonzero__=__bool__

    def __getitem__(self, index):
        return self.changes[index]

    def __iadd__(self, *args):
        for other in args:
            if type(other) is self.__class__:
                # extend
                self.changes += other.changes
            else:
                # append
                self.changes += other
        return self
    add=__iadd__
    extend=__iadd__

class Config(object):
    _instance = None
    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(Config, cls).__new__(cls)
            cls._instance.args = ()
            cls._instance.conf = {}
        return cls._instance

    def __init__(self, *args, **conf):
        self._args = args
        self._conf = conf

    def __enter__(self):
        self.args = self._args
        self.conf = self._conf
        return self

    def __exit__(self, typ, val, tb):
        self.args = ()
        self.conf = {}

    def __getattr__(self, name):
        if name in self.__dict__:
            return self.__dict__[name]
        return self.conf.get(name)

def main():
    import argparse
    import psycopg2
    from pypgdiff.objects import Database, Schema

    p = argparse.ArgumentParser()
    p.add_argument("--host", type=str, help="Host for BOTH schemas")
    p.add_argument("--db", type=str, help="Database for BOTH schemas")
    p.add_argument("--user", type=str, help="Username for BOTH schemas")
    p.add_argument("--pass", type=str, help="Password for BOTH schemas")
    p.add_argument("--host1", type=str, help="Host for source schema")
    p.add_argument("--db1", type=str, help="Database for source schema")
    p.add_argument("--user1", type=str, help="Username for source schema")
    p.add_argument("--pass1", type=str, help="Password for source schema")
    p.add_argument("--host2", type=str, help="Host for target schema")
    p.add_argument("--db2", type=str, help="Database for target schema")
    p.add_argument("--user2", type=str, help="Username for target schema")
    p.add_argument("--pass2", type=str, help="Password for target schema")
    p.add_argument("--normalize-constraints", action="store_true", help="Use normalized names when comparing constraints")
    p.add_argument("--prompt", action="store_true", help="Prompt for default values")
    p.add_argument("schemas", type=str, nargs=2, help="Schemas to compare")

    args = p.parse_args()

    db1 = psycopg2.connect(
        database = args.db1 or args.db,
        user = args.user1 or args.user,
        password = args.pass1 or getattr(args, 'pass'),
        host = args.host1 or args.host)
    s1 = Schema(database=Database(conn=db1), name=args.schemas[0])

    db2 = psycopg2.connect(
        database = args.db2 or args.db,
        user = args.user2 or args.user,
        password = args.pass2 or getattr(args, 'pass'),
        host = args.host2 or args.host)
    s2 = Schema(database=Database(conn=db2), name=args.schemas[1])

    with Config(normalize_constraints=args.normalize_constraints, prompt_for_defaults=args.prompt):
        cs = s1 | s2

    for c in cs:
        print("%s\n" % c.sql)

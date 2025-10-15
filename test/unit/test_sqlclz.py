import datetime
import re
import sqlite3
import unittest
from pathlib import Path
from typing import NamedTuple, Annotated, Optional, Union

import sqlclz
from sqlclz import named_tuple_table_class, PRIMARY, Connection, check, foreign
from sqlclz.stat import SqlStat


@named_tuple_table_class
class Person(NamedTuple):
    name: Annotated[str, PRIMARY]
    age: int

    @check('age')
    def _age(self):
        return self.age > 10


@named_tuple_table_class
class Bank(NamedTuple):
    name: Annotated[str, PRIMARY]


@named_tuple_table_class
class Account(NamedTuple):
    bank: Annotated[str, PRIMARY]
    person: Annotated[str, PRIMARY]
    money: int

    @foreign(Person)
    def _person(self):
        return self.person

    @foreign(Bank)
    def _bank(self):
        return self.bank


class SqlpTableTest(unittest.TestCase):
    conn: Connection

    @classmethod
    def setUpClass(cls):
        cls.conn = Connection(debug=True)
        with cls.conn:
            sqlclz.create_table(Person).submit()
            sqlclz.create_table(Bank).submit()
            sqlclz.create_table(Account).submit()

            sqlclz.insert_into(Person).submit([
                Person('Alice', 18),
                Person('Bob', 20),
            ])

            sqlclz.insert_into(Bank).submit([
                Bank('V'),
                Bank('M'),
            ])

            sqlclz.insert_into(Account).submit([
                Account('V', 'Alice', 1000),
                Account('V', 'Bob', 2000),
                Account('M', 'Alice', 200),
                Account('M', 'Bob', 100),
            ])

    def setUp(self):
        self.conn.__enter__()

    def tearDown(self):
        self.conn.__exit__(None, None, None)

    def assertSqlExeEqual(self, raw_sql: str, stat: SqlStat, parameter=()):
        r1 = self.conn.execute(raw_sql, parameter).fetchall()
        r2 = self.conn.execute(stat, parameter).fetchall()
        print(stat.build())
        self.assertListEqual(r1, r2)

    def test_insert_fail_on_check(self):
        with self.assertRaises(sqlite3.IntegrityError):
            sqlclz.insert_into(Person).submit([Person('baby', 1)])

    def test_select_person(self):
        results = sqlclz.select_from(Person).order_by(Person.name).fetchall()

        self.assertListEqual([
            Person('Alice', 18),
            Person('Bob', 20),
        ], results)

    def test_select_with_literal(self):
        results = sqlclz.select_from(Person.name, Person.age, 1).order_by(Person.name).fetchall()

        self.assertListEqual([
            ('Alice', 18, 1),
            ('Bob', 20, 1),
        ], results)

    def test_where(self):
        results = sqlclz.select_from(Account).where(
            Account.bank == 'V'
        ).fetchall()

        self.assertListEqual([
            Account('V', 'Alice', 1000),
            Account('V', 'Bob', 2000),
        ], results)

    def test_update(self):
        sqlclz.insert_into(Account, policy='REPLACE').submit([Account('K', 'Eve', 0)])
        self.assertEqual(Account('K', 'Eve', 0), sqlclz.select_from(Account).where(Account.bank == 'K').fetchone())
        sqlclz.update(Account, Account.money == 100).where(
            Account.bank == 'K'
        ).submit()
        self.assertEqual(Account('K', 'Eve', 100), sqlclz.select_from(Account).where(Account.bank == 'K').fetchone())

    def test_delete(self):
        sqlclz.insert_into(Account, policy='REPLACE').submit([Account('K', 'Eve', 0)])
        self.assertEqual(Account('K', 'Eve', 0), sqlclz.select_from(Account).where(Account.bank == 'K').fetchone())
        sqlclz.delete_from(Account).where(Account.bank == 'K').submit()
        self.assertIsNone(sqlclz.select_from(Account).where(Account.bank == 'K').fetchone())

    def test_foreign_field(self):
        from sqlclz.table import table_foreign_field, ForeignConstraint
        person = table_foreign_field(Account, Person)
        self.assertIsInstance(person, ForeignConstraint)
        self.assertEqual(person.name, '_person')
        self.assertEqual(person.foreign_table, Person)
        self.assertListEqual(person.foreign_fields, ['name'])
        self.assertEqual(person.table, Account)
        self.assertListEqual(person.fields, ['person'])

    def test_foreign_field_callable(self):
        from sqlclz.table import table_foreign_field, ForeignConstraint
        person = table_foreign_field(Account._person)
        self.assertIsInstance(person, ForeignConstraint)
        self.assertEqual(person.name, '_person')
        self.assertEqual(person.foreign_table, Person)
        self.assertListEqual(person.foreign_fields, ['name'])
        self.assertEqual(person.table, Account)
        self.assertListEqual(person.fields, ['person'])

    def test_map_foreign(self):
        from sqlclz.util import map_foreign
        results = map_foreign(Account('V', 'Alice', 1000), Person).fetchall()
        self.assertListEqual([
            Person('Alice', 18),
        ], results)

    def test_pull_foreign(self):
        from sqlclz.util import pull_foreign
        results = pull_foreign(Account, Person('Alice', 18)).fetchall()
        self.assertListEqual([
            Account('V', 'Alice', 1000),
            Account('M', 'Alice', 200),
        ], results)


class SqlCreateTableTest(unittest.TestCase):
    conn: Connection

    def setUp(self):
        self.conn = Connection(debug=True)
        self.conn.__enter__()

    def tearDown(self):
        self.conn.__exit__(None, None, None)

    def test_create_table(self):
        @named_tuple_table_class
        class Test(NamedTuple):
            a: str
            b: int

        sqlclz.create_table(Test)

    def test_create_table_with_primary_key(self):
        @named_tuple_table_class
        class Test(NamedTuple):
            a: Annotated[str, sqlclz.PRIMARY]
            b: int

        sqlclz.create_table(Test)

    def test_create_table_with_primary_keys(self):
        @named_tuple_table_class
        class Test(NamedTuple):
            a: Annotated[str, sqlclz.PRIMARY]
            b: Annotated[int, sqlclz.PRIMARY]

        sqlclz.create_table(Test)

    def test_create_table_with_unique_key(self):
        @named_tuple_table_class
        class Test(NamedTuple):
            a: Annotated[str, sqlclz.UNIQUE]
            b: int

        sqlclz.create_table(Test)

    def test_create_table_with_unique_keys(self):
        @named_tuple_table_class
        class Test(NamedTuple):
            a: Annotated[str, sqlclz.UNIQUE]
            b: Annotated[str, sqlclz.UNIQUE]

        sqlclz.create_table(Test)

    def test_create_table_with_unique_keys_on_table(self):
        @named_tuple_table_class
        class Test(NamedTuple):
            a: str
            b: str

            @sqlclz.unique()
            def _unique_ab_pair(self):
                return self.a, self.b

        sqlclz.create_table(Test)

    def test_create_table_with_null(self):
        @named_tuple_table_class
        class Test(NamedTuple):
            a: Optional[str]
            b: Union[int, None]
            c: Union[None, float]

        sqlclz.create_table(Test)

    def test_create_table_with_auto_increment(self):
        @named_tuple_table_class
        class Test(NamedTuple):
            a: Annotated[int, sqlclz.PRIMARY(auto_increment=True)]

        sqlclz.create_table(Test)

    def test_create_table_with_default_date_time(self):
        @named_tuple_table_class
        class Test(NamedTuple):
            a: Annotated[datetime.date, sqlclz.CURRENT_DATE]
            b: Annotated[datetime.time, sqlclz.CURRENT_TIME]
            c: Annotated[datetime.datetime, sqlclz.CURRENT_TIMESTAMP]

        sqlclz.create_table(Test)

    def test_create_table_foreign(self):
        @named_tuple_table_class
        class Ref(NamedTuple):
            name: Annotated[str, sqlclz.PRIMARY]
            other: str

        @named_tuple_table_class
        class Test(NamedTuple):
            name: Annotated[str, sqlclz.PRIMARY]
            ref: str

            @foreign(Ref.name, update='NO ACTION', delete='NO ACTION')
            def _ref(self):
                return self.ref

        sqlclz.create_table(Ref)
        sqlclz.create_table(Test)

    def test_create_table_foreign_primary(self):
        @named_tuple_table_class
        class Ref(NamedTuple):
            name: Annotated[str, sqlclz.PRIMARY]
            other: str

        @named_tuple_table_class
        class Test(NamedTuple):
            name: Annotated[str, sqlclz.PRIMARY]
            ref: str

            @foreign(Ref, update='NO ACTION', delete='NO ACTION')
            def _ref(self):
                return self.ref

        sqlclz.create_table(Ref)
        sqlclz.create_table(Test)

    def test_create_table_foreign_self(self):
        @named_tuple_table_class
        class Test(NamedTuple):
            name: Annotated[str, sqlclz.PRIMARY]
            ref: str

            @foreign('name', update='NO ACTION', delete='NO ACTION')
            def _ref(self):
                return self.ref

        sqlclz.create_table(Test)

    def test_create_table_check_field(self):
        @named_tuple_table_class
        class Test(NamedTuple):
            name: Annotated[str, sqlclz.PRIMARY]
            age: int

            @check('age')
            def _age(self) -> bool:
                return self.age > 10

        sqlclz.create_table(Test)

    def test_create_table_check(self):
        @named_tuple_table_class
        class Test(NamedTuple):
            name: Annotated[str, sqlclz.PRIMARY]
            age: int

            @check()
            def _check_all(self) -> bool:
                return (self.age > 10) & (self.name != '')

        sqlclz.create_table(Test)

    def test_fail_on_stat_constructing(self):
        @named_tuple_table_class
        class Test(NamedTuple):
            a: str
            b: str

        sqlclz.create_table(Test)
        sqlclz.insert_into(Test).submit([Test('a', '1'), Test('b', '2')])

        with self.assertRaises(TypeError) as capture:
            sqlclz.delete_from(Test).where(Test.a == object()).build()

        print(capture.exception)

        self.assertListEqual([Test('a', '1'), Test('b', '2')],
                             sqlclz.select_from(Test).fetchall())


class SqlUpsertTest(unittest.TestCase):
    conn: Connection

    def setUp(self):
        self.conn = Connection(debug=True)
        with self.conn:
            sqlclz.create_table(Person).submit()
            sqlclz.create_table(Bank).submit()
            sqlclz.create_table(Account).submit()

            sqlclz.insert_into(Person).submit([
                Person('Alice', 18),
                Person('Bob', 20),
            ])

            sqlclz.insert_into(Bank).submit([
                Bank('V'),
                Bank('M'),
            ])

            sqlclz.insert_into(Account).submit([
                Account('V', 'Alice', 1000),
                Account('V', 'Bob', 2000),
                Account('M', 'Alice', 200),
                Account('M', 'Bob', 100),
            ])

    def test_update_conflict_with_name(self):
        with self.conn:
            sqlclz.insert_into(Person).on_conflict(Person.name).do_update(
                Person.age == sqlclz.excluded(Person).age
            ).submit(
                [Person('Alice', 28)]
            )

            result = sqlclz.select_from(Person).where(Person.name == 'Alice').fetchone()
            self.assertEqual(result, Person('Alice', 28))

    def test_update_conflict(self):
        with self.conn:
            sqlclz.insert_into(Person).on_conflict().do_nothing().submit(
                [Person('Alice', 28)]
            )

            result = sqlclz.select_from(Person).where(Person.name == 'Alice').fetchone()
            self.assertEqual(result, Person('Alice', 18))


class SqlTableOtherTest(unittest.TestCase):
    def assert_sql_state_equal(self, a: str, b: str):
        a = re.split(' +', a.replace('\n', ' ').strip())
        b = re.split(' +', b.replace('\n', ' ').strip())
        self.assertListEqual(a, b)

    def test_create_table_not_null(self):
        @named_tuple_table_class
        class A(NamedTuple):
            a: int
            b: Optional[int]

        with Connection(debug=True) as conn:
            sqlclz.create_table(A)
            self.assert_sql_state_equal("""
            CREATE TABLE A (
                [a] INTEGER NOT NULL ,
                [b] INTEGER
            )
            """, conn.table_schema(A))

    def test_create_table_default(self):
        @named_tuple_table_class
        class A(NamedTuple):
            a: int
            b: int = 0
            c: str = ''
            d: bool = True
            e: bool = False
            f: str = None

        with Connection(debug=True) as conn:
            sqlclz.create_table(A)
            self.assert_sql_state_equal("""
            CREATE TABLE A (
                [a] INTEGER NOT NULL ,
                [b] INTEGER NOT NULL DEFAULT 0 ,
                [c] TEXT NOT NULL DEFAULT '' ,
                [d] BOOLEAN NOT NULL DEFAULT True ,
                [e] BOOLEAN NOT NULL DEFAULT False ,
                [f] TEXT DEFAULT NULL
            )
            """, conn.table_schema(A))
            sqlclz.insert_into(A).submit([A(1)])

            result = sqlclz.select_from(A).fetchone()
            self.assertEqual(result, A(1, 0, '', True, False, None))

    def test_table_field_bool_casting(self):
        @named_tuple_table_class
        class A(NamedTuple):
            a: int
            b: bool

        with Connection(debug=True):
            sqlclz.create_table(A)
            sqlclz.insert_into(A).submit([A(0, False), A(1, True)])
            self.assertEqual(A(0, False), sqlclz.select_from(A).where(A.a == 0).fetchone())
            self.assertEqual(A(1, True), sqlclz.select_from(A).where(A.a == 1).fetchone())
            self.assertEqual((False,), sqlclz.select_from(A.b).where(A.a == 0).fetchone())
            self.assertEqual((True,), sqlclz.select_from(A.b).where(A.a == 1).fetchone())

    def test_table_field_path_casting(self):
        @named_tuple_table_class
        class A(NamedTuple):
            a: Annotated[Path, PRIMARY]

        with Connection(debug=True) as conn:
            sqlclz.create_table(A)
            self.assert_sql_state_equal("""
            CREATE TABLE A (
                [a] TEXT NOT NULL PRIMARY KEY
            )
            """, conn.table_schema(A))

            sqlclz.insert_into(A).submit([A(Path('test.txt'))])

            result = sqlclz.select_from(A).fetchone()
            self.assertEqual(result, A(Path('test.txt')))

    def test_table_property(self):
        @named_tuple_table_class
        class A(NamedTuple):
            a: int
            b: int

            @property
            def c(self) -> int:
                return self.a + self.b

        with Connection(debug=True) as conn:
            sqlclz.create_table(A)

            self.assertEqual(6, A(2, 4).c)

            sqlclz.insert_into(A).submit([
                A(0, 1),
                A(1, 2),
                A(2, 3),
            ])

            results = sqlclz.select_from(A.a, A.b, A.c).fetchall()
            self.assertListEqual([
                (0, 1, 1), (1, 2, 3), (2, 3, 5)
            ], results)

    def test_table_property_sqlclz_call(self):
        @named_tuple_table_class
        class A(NamedTuple):
            a: str
            b: str

            @property
            def c(self) -> str:
                return sqlclz.concat(self.a, self.b)

        with Connection(debug=True) as conn:
            sqlclz.create_table(A)

            value = A('2', '4').c
            self.assertIsInstance(value, str)
            self.assertEqual('24', value)

            sqlclz.insert_into(A).submit([
                A('0', '1'),
                A('1', '2'),
                A('2', '3'),
            ])

            results = sqlclz.select_from(A.a, A.b, A.c).fetchall()
            self.assertListEqual([
                ('0', '1', '01'), ('1', '2', '12'), ('2', '3', '23')
            ], results)



if __name__ == '__main__':
    unittest.main()

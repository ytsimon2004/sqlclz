import unittest
from typing import NamedTuple, Annotated

import sqlclz
from sqlclz import Connection


class SqlpFuncTest(unittest.TestCase):

    # https://www.sqlite.org/windowfunctions.html#introduction_to_window_functions

    def test_row_number(self):

        @sqlclz.named_tuple_table_class
        class T(NamedTuple):
            x: Annotated[int, sqlclz.PRIMARY]
            y: str

        with Connection(debug=True):
            sqlclz.create_table(T)
            sqlclz.insert_into(T).submit([
                T(1, 'aaa'),
                T(2, 'ccc'),
                T(3, 'bbb'),
            ])

            results = sqlclz.select_from(
                T.x,
                T.y,
                sqlclz.row_number().over(order_by=[T.y]) @ 'row_number'
            ).order_by(T.x).fetchall()

        self.assertListEqual(results, [
            (1, 'aaa', 1),
            (2, 'ccc', 3),
            (3, 'bbb', 2),
        ])

    def test_window_define(self):
        @sqlclz.named_tuple_table_class
        class T(NamedTuple):
            x: Annotated[int, sqlclz.PRIMARY]
            y: str

        with Connection(debug=True):
            sqlclz.create_table(T)
            sqlclz.insert_into(T).submit([
                T(1, 'aaa'),
                T(2, 'ccc'),
                T(3, 'bbb'),
            ])

            w1 = sqlclz.window_def('win1', order_by=[T.y])

            with w1.frame('RANGE') as f:
                f.between(f.unbounded_preceding(), f.current_row())

            w2 = sqlclz.window_def('win2', partition_by=[T.y], order_by=[T.x])

            results = sqlclz.select_from(
                T.x,
                T.y,
                sqlclz.row_number().over(w1),
                sqlclz.rank().over(w2)
            ).windows(w1, w2).order_by(T.x).fetchall()

        print(results)
        self.assertListEqual(results, [
            (1, 'aaa', 1, 1),
            (2, 'ccc', 3, 1),
            (3, 'bbb', 2, 1),
        ])


if __name__ == '__main__':
    unittest.main()

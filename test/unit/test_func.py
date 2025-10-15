import unittest
from typing import NamedTuple, Any, Optional

import sqlclz
from ._test import SqlTestCase
from ._tracks import *


@sqlclz.named_tuple_table_class
class AvgTest(NamedTuple):
    val: Optional[Any]


class FuncAvgTest(SqlTestCase):
    """https://www.sqlitetutorial.net/sqlite-avg/"""

    @classmethod
    def setUpClass(cls):
        cls.source_database = None
        super().setUpClass()

    def setUp(self):
        sqlclz.create_table(AvgTest).submit()

        self.connection.execute("""\
        INSERT INTO AvgTest (val)
        VALUES
         (1),
         (2),
         (10.1),
         (20.5),
         ('8'),
         ('B'),
         (NULL),
         (x'0010'),
         (x'0011');
        """)

    def test_setup(self):
        ret = self.assertSqlExeEqual("""SELECT rowid, val FROM AvgTest;""",
                                     sqlclz.select_from(sqlclz.ROWID, AvgTest.val))
        print(ret)

    def test_avg(self):
        self.assertSqlExeEqual("""\
        SELECT
            avg(val)
        FROM
            AvgTest
        WHERE
            rowid < 5;
        """, sqlclz.select_from(sqlclz.avg(AvgTest.val)).where(sqlclz.ROWID < 5))

    def test_avg_on_null(self):
        self.assertSqlExeEqual("""\
        SELECT
            avg(val)
        FROM
            AvgTest;
        """, sqlclz.select_from(sqlclz.avg(AvgTest.val)))

    def test_avg_distinct(self):
        self.assertSqlExeEqual("""\
        SELECT
            avg(DISTINCT val)
        FROM
            AvgTest;
        """, sqlclz.select_from(sqlclz.avg(AvgTest.val).distinct()))


class FuncAvgExampleTest(SqlTestCase):
    """https://www.sqlitetutorial.net/sqlite-avg/"""

    def test_avg_all(self):
        self.assertSqlExeEqual("""\
        SELECT
            avg(milliseconds)
        FROM
            tracks;
        """, sqlclz.select_from(sqlclz.avg(Tracks.Milliseconds)))

    def test_avg_group_by(self):
        self.assertSqlExeEqual("""\
        SELECT
            albumid,
            avg(milliseconds)
        FROM
            tracks
        GROUP BY
            albumid;
        """, sqlclz.select_from(Tracks.AlbumId, sqlclz.avg(Tracks.Milliseconds)).group_by(Tracks.AlbumId))

    def test_avg_join(self):
        self.assertSqlExeEqual("""\
        SELECT
            tracks.AlbumId,
            Title,
            round(avg(Milliseconds), 2) avg_length
        FROM
            tracks
        INNER JOIN albums ON albums.AlbumId = tracks.albumid
        GROUP BY
            tracks.albumid;
        """, sqlclz.select_from(
            Tracks.AlbumId,
            Albums.Title,
            sqlclz.round(sqlclz.avg(Tracks.Milliseconds), 2) @ 'avg_length',
            from_table=Tracks
        ).join(Albums.AlbumId == Tracks.AlbumId, by='inner').group_by(Tracks.AlbumId))

    def test_avg_join_having(self):
        self.assertSqlExeEqual("""\
        SELECT
            tracks.albumid,
            title,
            round(avg(milliseconds),2)  avg_length
        FROM
            tracks
        INNER JOIN albums ON albums.AlbumId = tracks.albumid
        GROUP BY
            tracks.albumid
        HAVING
            avg_length BETWEEN 100000 AND 200000;
        """, sqlclz.select_from(
            Tracks.AlbumId,
            Albums.Title,
            (avg_length := sqlclz.round(sqlclz.avg(Tracks.Milliseconds), 2) @ 'avg_length'),
            from_table=Tracks
        ).join(
            Albums.AlbumId == Tracks.AlbumId, by='inner'
        ).group_by(
            Tracks.AlbumId
        ).having(
            sqlclz.between(avg_length, 100000, 200000)
        ))


@sqlclz.named_tuple_table_class
class CountTest(NamedTuple):
    c: Optional[int]


class FuncCountTest(SqlTestCase):
    """https://www.sqlitetutorial.net/sqlite-count-function/"""

    @classmethod
    def setUpClass(cls):
        cls.source_database = None
        super().setUpClass()

    def setUp(self):
        sqlclz.create_table(CountTest).submit()

        self.connection.execute("""\
           INSERT INTO CountTest (c)
           VALUES(1),(2),(3),(null),(3);
           """)

    def test_setup(self):
        self.assertSqlExeEqual("""SELECT * FROM CountTest;""",
                               sqlclz.select_from(CountTest))

    def test_count_all(self):
        self.assertSqlExeEqual("""SELECT COUNT(*) FROM CountTest;""",
                               sqlclz.select_from(sqlclz.count(), from_table=CountTest))

    def test_count_non_null(self):
        self.assertSqlExeEqual("""SELECT COUNT(c) FROM CountTest;""",
                               sqlclz.select_from(sqlclz.count(CountTest.c)))

    def test_count_distinct(self):
        self.assertSqlExeEqual("""SELECT COUNT(DISTINCT c) FROM CountTest;""",
                               sqlclz.select_from(sqlclz.count(CountTest.c).distinct()))


class FuncCountExampleTest(SqlTestCase):
    """https://www.sqlitetutorial.net/sqlite-count-function/"""

    def test_count_all(self):
        self.assertSqlExeEqual("""\
        SELECT count(*)
        FROM tracks;
        """, sqlclz.select_from(sqlclz.count(), from_table=Tracks))

    def test_count_where(self):
        self.assertSqlExeEqual("""\
        SELECT count(*)
        FROM tracks
        WHERE albumid = 10;
        """, sqlclz.select_from(sqlclz.count(), from_table=Tracks).where(Tracks.AlbumId == 10))

    def test_count_group_by(self):
        self.assertSqlExeEqual("""\
        SELECT count(*)
        FROM tracks
        GROUP BY
            albumid;
        """, sqlclz.select_from(sqlclz.count(), from_table=Tracks).group_by(Tracks.AlbumId))

    def test_count_having(self):
        self.assertSqlExeEqual("""\
        SELECT 
            albumid,
            count(*)
        FROM tracks
        GROUP BY
            albumid
        HAVING COUNT(*) > 25
        """, sqlclz.select_from(
            Tracks.AlbumId, sqlclz.count()
        ).group_by(Tracks.AlbumId).having(
            sqlclz.count() > 25
        ))

    def test_count_join(self):
        self.assertSqlExeEqual("""\
        SELECT
            tracks.albumid, 
            title, 
            COUNT(*)
        FROM
            tracks
        INNER JOIN albums ON
            albums.albumid = tracks.albumid
        GROUP BY
            tracks.albumid
        HAVING
            COUNT(*) > 25
        ORDER BY
            COUNT(*) DESC;
        """, sqlclz.select_from(
            Tracks.AlbumId,
            Albums.Title,
            sqlclz.count()
        ).join(
            Tracks.AlbumId == Albums.AlbumId, by='inner'
        ).group_by(
            Tracks.AlbumId
        ).having(
            sqlclz.count() > 25
        ).order_by(
            sqlclz.desc(sqlclz.count())
        ))

    def test_count_distinct(self):
        self.assertSqlExeEqual("""\
        SELECT COUNT(title)
        FROM employees;
        """, sqlclz.select_from(sqlclz.count(Employees.Title)))

        self.assertSqlExeEqual("""\
        SELECT COUNT(DISTINCT title)
        FROM employees;
        """, sqlclz.select_from(sqlclz.count(Employees.Title).distinct()))


class FuncMinMaxTest(SqlTestCase):
    """
    https://www.sqlitetutorial.net/sqlite-max/
    https://www.sqlitetutorial.net/sqlite-min/
    """

    def test_max(self):
        self.assertSqlExeEqual("""\
        SELECT MAX(bytes) FROM tracks;
        """, sqlclz.select_from(sqlclz.max(Tracks.Bytes)))

    def test_max_subquery(self):
        self.assertSqlExeEqual("""\
        SELECT
            TrackId,
            Name,
            Bytes
        FROM
            tracks
        WHERE
            Bytes = (SELECT MAX(Bytes) FROM tracks);
        """, sqlclz.select_from(
            Tracks.TrackId,
            Tracks.Name,
            Tracks.Bytes
        ).where(
            Tracks.Bytes == sqlclz.select_from(sqlclz.max(Tracks.Bytes))
        ))

    def test_max_group_by(self):
        self.assertSqlExeEqual("""\
        SELECT
            AlbumId,
            MAX(bytes)
        FROM
            tracks
        GROUP BY
            AlbumId;
        """, sqlclz.select_from(Tracks.AlbumId, sqlclz.max(Tracks.Bytes)).group_by(Tracks.AlbumId))

    def test_max_having(self):
        self.assertSqlExeEqual("""\
        SELECT
            AlbumId,
            MAX(bytes)
        FROM
            tracks
        GROUP BY
            AlbumId
        HAVING MAX(bytes) > 6000000;
        """, sqlclz.select_from(
            Tracks.AlbumId, sqlclz.max(Tracks.Bytes)
        ).group_by(Tracks.AlbumId).having(
            sqlclz.max(Tracks.Bytes) > 6000000
        ))


class FuncSumTest(SqlTestCase):
    """
    https://www.sqlitetutorial.net/sqlite-sum/
    """

    def test_sum(self):
        self.assertSqlExeEqual("""\
        SELECT
           SUM(milliseconds)
        FROM
           tracks;
        """, sqlclz.select_from(sqlclz.sum(Tracks.Milliseconds)))

    def test_sum_group_by(self):
        self.assertSqlExeEqual("""\
        SELECT
            AlbumId,
            SUM(milliseconds)
        FROM
            tracks
        GROUP BY
            AlbumId;
        """, sqlclz.select_from(Tracks.AlbumId, sqlclz.sum(Tracks.Milliseconds)).group_by(Tracks.AlbumId))

    def test_sum_join(self):
        self.assertSqlExeEqual("""\
        SELECT
           tracks.albumid,
           title, 
           SUM(milliseconds)
        FROM
           tracks
        INNER JOIN albums ON albums.albumid = tracks.albumid
        GROUP BY
           tracks.albumid, 
           title;
        """, sqlclz.select_from(
            Tracks.AlbumId, Albums.Title, sqlclz.sum(Tracks.Milliseconds)
        ).join(Tracks._albums, by='inner').group_by(
            Tracks.AlbumId, Albums.Title
        ))

    def test_sum_join_having(self):
        self.assertSqlExeEqual("""\
        SELECT
           tracks.albumid,
           title, 
           SUM(milliseconds)
        FROM
           tracks
        INNER JOIN albums ON albums.albumid = tracks.albumid
        GROUP BY
           tracks.albumid, 
           title
        HAVING
           SUM(milliseconds) > 1000000;
        """, sqlclz.select_from(
            Tracks.AlbumId, Albums.Title, sqlclz.sum(Tracks.Milliseconds)
        ).join(Tracks._albums, by='inner').group_by(
            Tracks.AlbumId, Albums.Title
        ).having(
            sqlclz.sum(Tracks.Milliseconds) > 1000000
        ))


# https://www.sqlitetutorial.net/sqlite-group_concat/

if __name__ == '__main__':
    unittest.main()

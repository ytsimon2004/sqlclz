import unittest
from typing import NamedTuple

import sqlclz
from ._test import SqlTestCase
from ._tracks import *


class JoinTest(SqlTestCase):
    """
    https://www.sqlitetutorial.net/sqlite-join/
    """

    def test_inner_join(self):
        self.assertSqlExeEqual("""\
        SELECT
            Title,
            Name
        FROM
            albums
        INNER JOIN artists
            ON artists.ArtistId = albums.ArtistId;
        """, sqlclz.select_from(
            Albums.Title, Artists.Name
        ).join(
            Artists.ArtistId == Albums.ArtistId, by='inner'
        ))

    def test_inner_join_on_foreign_constraint(self):
        self.assertSqlExeEqual("""\
        SELECT
            Title,
            Name
        FROM
            albums
        INNER JOIN artists USING (ArtistId);
        """, sqlclz.select_from(Albums.Title, Artists.Name).join(Albums._artists, by='inner'))

    def test_join_alias(self):
        l = sqlclz.alias(Albums, 'l')
        r = sqlclz.alias(Artists, 'r')

        self.assertSqlExeEqual("""\
        SELECT
            l.Title,
            r.Name
        FROM
            albums l
        INNER JOIN artists r ON
            r.ArtistId = l.ArtistId;
        """, sqlclz.select_from(l.Title, r.Name).join(r.ArtistId == l.ArtistId, by='inner'))

    def test_join_using(self):
        self.assertSqlExeEqual("""\
        SELECT
           Title,
           Name
        FROM
           albums
        INNER JOIN artists USING(ArtistId);
        """, sqlclz.select_from(Albums.Title, Artists.Name)
                               .join(Artists.ArtistId, by='inner'))

    def test_left_join(self):
        self.assertSqlExeEqual("""\
        SELECT
            Name,
            Title
        FROM
            artists
        LEFT JOIN albums ON
            artists.ArtistId = albums.ArtistId
        ORDER BY Name;
        """, sqlclz.select_from(Artists.Name, Albums.Title)
                               .join(Artists.ArtistId == Albums.ArtistId, by='left')
                               .order_by(Artists.Name))

    def test_left_join_using(self):
        self.assertSqlExeEqual("""\
        SELECT
            Name,
            Title
        FROM
            artists
        LEFT JOIN albums USING (ArtistId)
        ORDER BY Name;
        """, sqlclz.select_from(Artists.Name, Albums.Title)
                               .join(Albums.ArtistId, by='left')
                               .order_by(Artists.Name))

    def test_left_join_with_where(self):
        self.assertSqlExeEqual("""\
        SELECT
            Name,
            Title
        FROM
            artists
        LEFT JOIN albums ON
            artists.ArtistId = albums.ArtistId
        WHERE Title IS NULL
        ORDER BY Name;
        """, sqlclz.select_from(Artists.Name, Albums.Title)
                               .join(Artists.ArtistId == Albums.ArtistId, by='left')
                               .where(sqlclz.is_null(Albums.Title))
                               .order_by(Artists.Name))


class InnerJoinTest(SqlTestCase):
    """
    https://www.sqlitetutorial.net/sqlite-inner-join/
    """

    def test_inner_join(self):
        self.assertSqlExeEqual("""\
        SELECT
            trackid,
            name,
            title
        FROM
            tracks
        INNER JOIN albums ON albums.albumid = tracks.albumid;
        """, sqlclz.select_from(Tracks.TrackId, Tracks.Name, Albums.Title)
                               .join(Albums.AlbumId == Tracks.AlbumId, by='inner'))

    def test_inner_join_show_case(self):
        self.assertSqlExeEqual("""\
        SELECT
            trackid,
            name,
            tracks.albumid AS album_id_tracks,
            albums.albumid AS album_id_albums,
            title
        FROM
            tracks
        INNER JOIN albums ON albums.albumid = tracks.albumid;
        """, sqlclz.select_from(
            Tracks.TrackId,
            Tracks.Name,
            Tracks.AlbumId @ 'album_id_tracks',
            Albums.AlbumId @ 'album_id_albums',
            Albums.Title
        ).join(Albums.AlbumId == Tracks.AlbumId, by='inner'))

    def test_join_multiple(self):
        self.assertSqlExeEqual("""\
        SELECT
            trackid,
            tracks.name AS track,
            albums.title AS album,
            artists.name AS artist
        FROM
            tracks
            INNER JOIN albums ON albums.albumid = tracks.albumid
            INNER JOIN artists ON artists.artistid = albums.artistid;
        """, sqlclz.select_from(
            Tracks.TrackId,
            Tracks.Name @ 'track',
            Albums.Title @ 'album',
            Artists.Name @ 'artist'
        ).join(
            Albums.AlbumId == Tracks.AlbumId, by='inner'
        ).join(
            Artists.ArtistId == Albums.ArtistId, by='inner'
        ))

    def test_join_multiple_where(self):
        self.assertSqlExeEqual("""\
        SELECT
            trackid,
            tracks.name AS Track,
            albums.title AS Album,
            artists.name AS Artist
        FROM
            tracks
            INNER JOIN albums ON albums.albumid = tracks.albumid
            INNER JOIN artists ON artists.artistid = albums.artistid
        WHERE
            artists.artistid = 10;
        """, sqlclz.select_from(
            Tracks.TrackId,
            Tracks.Name @ 'Track',
            Albums.Title @ 'Album',
            Artists.Name @ 'Artist'
        ).join(
            Albums.AlbumId == Tracks.AlbumId, by='inner'
        ).join(
            Artists.ArtistId == Albums.ArtistId, by='inner'
        ).where(Artists.ArtistId == 10))


class LeftJoinTest(SqlTestCase):
    """
    https://www.sqlitetutorial.net/sqlite-left-join/
    """

    def test_left_join(self):
        self.assertSqlExeEqual("""\
        SELECT
           artists.ArtistId,
           AlbumId
        FROM
           artists
        LEFT JOIN albums ON
           albums.ArtistId = artists.ArtistId
        ORDER BY
           AlbumId;
        """, sqlclz.select_from(Artists.ArtistId, Albums.AlbumId)
                               .join(Albums.ArtistId == Artists.ArtistId, by='left')
                               .order_by(Albums.AlbumId))

    def test_left_join_where(self):
        self.assertSqlExeEqual("""\
        SELECT
           artists.ArtistId,
           AlbumId
        FROM
           artists
        LEFT JOIN albums ON
           albums.ArtistId = artists.ArtistId
        WHERE
            AlbumId IS NULL;
        """, sqlclz.select_from(Artists.ArtistId, Albums.AlbumId)
                               .join(Albums.ArtistId == Artists.ArtistId, by='left')
                               .where(sqlclz.is_null(Albums.AlbumId)))


@sqlclz.named_tuple_table_class
class Ranks(NamedTuple):
    rank: str


@sqlclz.named_tuple_table_class
class Suits(NamedTuple):
    suit: str


@sqlclz.named_tuple_table_class
class Dogs(NamedTuple):
    type: str
    color: str


@sqlclz.named_tuple_table_class
class Cats(NamedTuple):
    type: str
    color: str


class CrossJoinTest(SqlTestCase):
    """
    https://www.sqlitetutorial.net/sqlite-cross-join/
    """

    @classmethod
    def setUpClass(cls):
        cls.connection = sqlclz.Connection()

        with cls.connection:
            sqlclz.create_table(Ranks)
            sqlclz.create_table(Suits)
            sqlclz.insert_into(Ranks).submit([
                Ranks('A'), Ranks('2'), Ranks('3'), Ranks('4'), Ranks('5'),
            ])
            sqlclz.insert_into(Suits).submit([
                Suits('Clubs'), Suits('Diamonds'), Suits('Hearts'), Suits('Spades')
            ])

    def test_cross_join(self):
        self.assertSqlExeEqual("""\
        SELECT rank,
               suit
        FROM ranks
        CROSS JOIN suits
        ORDER BY suit;
        """, sqlclz.select_from(Ranks.rank, Suits.suit).join(Suits, by='cross').order_by(Suits.suit))


class OuterJoinTest(SqlTestCase):
    """
    https://www.sqlitetutorial.net/sqlite-full-outer-join/
    """

    @classmethod
    def setUpClass(cls):
        cls.connection = sqlclz.Connection()

        with cls.connection:
            sqlclz.create_table(Dogs)
            sqlclz.create_table(Cats)
            sqlclz.insert_into(Dogs).submit([
                Dogs('Hunting', 'Black'), Dogs('Guard', 'Brown'),
            ])
            sqlclz.insert_into(Cats).submit([
                Cats('Indoor', 'White'), Cats('Outdoor', 'Black')
            ])

    @unittest.skip('FULL OUTER JOIN')
    def test_outer_join(self):
        self.assertSqlExeEqual("""\
        SELECT *
        FROM dogs
        FULL OUTER JOIN cats
            ON dogs.color = cats.color;
        """, sqlclz.select_from(Dogs).join(Dogs.color == Cats.color, by='full outer'))


class SelfJoinTest(SqlTestCase):
    """
    https://www.sqlitetutorial.net/sqlite-self-join/
    """

    def test_self_join(self):
        e = sqlclz.alias(Employees, 'e')
        m = sqlclz.alias(Employees, 'm')
        self.assertSqlExeEqual("""\
        SELECT m.firstname || ' ' || m.lastname AS 'Manager',
               e.firstname || ' ' || e.lastname AS 'Direct report'
        FROM employees e
        INNER JOIN employees m ON m.employeeid = e.reportsto
        ORDER BY manager;
        """, sqlclz.select_from(
            (manager := sqlclz.concat(m.FirstName, ' ', m.LastName) @ 'Manager'),
            sqlclz.concat(e.FirstName, ' ', e.LastName) @ 'Direct report',
            from_table=e
        ).join(m, m.EmployeeId == e.ReportsTo, by='inner').order_by(manager))

    def test_self_join_by_foreign_constraint(self):
        e = sqlclz.alias(Employees, 'e')
        m = sqlclz.alias(Employees, 'm')
        self.assertSqlExeEqual("""\
        SELECT m.firstname || ' ' || m.lastname AS 'Manager',
               e.firstname || ' ' || e.lastname AS 'Direct report'
        FROM employees e
        INNER JOIN employees m ON m.employeeid = e.reportsto
        ORDER BY manager;
        """, sqlclz.select_from(
            (manager := sqlclz.concat(m.FirstName, ' ', m.LastName) @ 'Manager'),
            sqlclz.concat(e.FirstName, ' ', e.LastName) @ 'Direct report',
            from_table=e
        ).join(m, Employees._report_to, by='inner').order_by(manager))

    def test_self_join_example2(self):
        e1 = sqlclz.alias(Employees, 'e1')
        e2 = sqlclz.alias(Employees, 'e2')
        self.assertSqlExeEqual("""\
        SELECT DISTINCT
            e1.city,
            e1.firstName || ' ' || e1.lastname AS fullname
        FROM
            employees e1
        INNER JOIN employees e2 ON e2.city = e1.city
           AND (e1.firstname <> e2.firstname AND e1.lastname <> e2.lastname)
        ORDER BY
            e1.city;
        """, sqlclz.select_from(
            e1.City,
            sqlclz.concat(e1.FirstName, ' ', e1.LastName) @ 'fullname',
            distinct=True
        ).join(
            e2,
            e2.City == e1.City,
            (e1.FirstName != e2.FirstName) & (e1.LastName != e2.LastName),
            by='inner'
        ).order_by(e1.City))


class GroupByTest(SqlTestCase):
    """
    https://www.sqlitetutorial.net/sqlite-group-by/
    """

    def test_group_by(self):
        self.assertSqlExeEqual("""\
        SELECT
            albumid,
            COUNT(trackid)
        FROM
            tracks
        GROUP BY
            albumid;
        """, sqlclz.select_from(Tracks.AlbumId, sqlclz.count(Tracks.AlbumId))
                               .group_by(Tracks.AlbumId))

    def test_group_by_then_order_by(self):
        self.assertSqlExeEqual("""\
        SELECT
            albumid,
            COUNT(trackid)
        FROM
            tracks
        GROUP BY
            albumid
        ORDER BY COUNT(trackid) DESC;
        """, sqlclz.select_from(Tracks.AlbumId, sqlclz.count(Tracks.TrackId))
                               .group_by(Tracks.AlbumId)
                               .order_by(sqlclz.desc(sqlclz.count(Tracks.TrackId))))

    def test_group_by_inner_join(self):
        self.assertSqlExeEqual("""\
        SELECT
            tracks.albumid,
            title,
            COUNT(trackid)
        FROM
            tracks
        INNER JOIN albums ON albums.albumid = tracks.albumid
        GROUP BY
            tracks.albumid;
        """, sqlclz.select_from(
            Tracks.AlbumId,
            Albums.Title,
            sqlclz.count(Tracks.TrackId)
        ).join(
            Albums.AlbumId == Tracks.AlbumId, by='inner'
        ).group_by(Tracks.AlbumId))

    def test_group_by_having(self):
        self.assertSqlExeEqual("""\
        SELECT
            tracks.albumid,
            title,
            COUNT(trackid)
        FROM
            tracks
        INNER JOIN albums ON albums.albumid = tracks.albumid
        GROUP BY
            tracks.albumid
        HAVING COUNT(trackid) > 15;
        """, sqlclz.select_from(
            Tracks.AlbumId, Albums.Title, sqlclz.count(Tracks.TrackId)
        ).join(
            Albums.AlbumId == Tracks.AlbumId, by='inner'
        ).group_by(Tracks.AlbumId).having(
            sqlclz.count(Tracks.TrackId) > 15
        ))

    def test_group_by_sum(self):
        self.assertSqlExeEqual("""\
        SELECT
            albumid,
            SUM(milliseconds) length,
            SUM(bytes) size
        FROM
            tracks
        GROUP BY
            albumid;
        """, sqlclz.select_from(
            Tracks.AlbumId,
            sqlclz.sum(Tracks.Milliseconds) @ 'length',
            sqlclz.sum(Tracks.Bytes) @ 'size'
        ).group_by(Tracks.AlbumId))

    def test_group_by_with_avg_funcs(self):
        self.assertSqlExeEqual("""\
        SELECT
            tracks.albumid,
            title,
            min(milliseconds),
            max(milliseconds),
            round(avg(milliseconds),2)
        FROM
            tracks
        INNER JOIN albums ON albums.albumid = tracks.albumid
        GROUP BY
            tracks.albumid;
        """, sqlclz.select_from(
            Tracks.AlbumId,
            Albums.Title,
            sqlclz.min(Tracks.Milliseconds),
            sqlclz.max(Tracks.Milliseconds),
            sqlclz.round(sqlclz.avg(Tracks.Milliseconds), 2)
        ).join(
            Albums.AlbumId == Tracks.AlbumId, by='inner'
        ).group_by(Tracks.AlbumId))

    def test_group_by_multiple_columns(self):
        self.assertSqlExeEqual("""\
        SELECT
           MediaTypeId,
           GenreId,
           COUNT(TrackId)
        FROM
           tracks
        GROUP BY
           MediaTypeId,
           GenreId;
        """, sqlclz.select_from(
            Tracks.MediaTypeId,
            Tracks.GenreId,
            sqlclz.count(Tracks.TrackId)
        ).group_by(Tracks.MediaTypeId, Tracks.GenreId))

    def test_group_by_date(self):
        self.assertSqlExeEqual("""\
        SELECT
           STRFTIME('%Y', InvoiceDate) InvoiceYear,
           COUNT(InvoiceId) InvoiceCount
        FROM
           invoices
        GROUP BY
           STRFTIME('%Y', InvoiceDate)
        ORDER BY
           InvoiceYear;
        """, sqlclz.select_from(
            (year := (sqlclz.strftime('%Y', Invoices.InvoiceDate) @ 'InvoiceYear')),
            sqlclz.count(Invoices.InvoiceId) @ 'InvoiceCount',
            from_table=Invoices,
        ).group_by(sqlclz.strftime('%Y', Invoices.InvoiceDate)).order_by(year))


class HavingTest(SqlTestCase):
    """
    https://www.sqlitetutorial.net/sqlite-having/
    """

    def test_having(self):
        self.assertSqlExeEqual("""\
        SELECT
            albumid,
            COUNT(trackid)
        FROM
            tracks
        GROUP BY
            albumid
        HAVING albumid = 1;
        """, sqlclz.select_from(
            Tracks.AlbumId, sqlclz.count(Tracks.TrackId)
        ).group_by(Tracks.AlbumId).having(
            Tracks.AlbumId == 1
        ))

    def test_having_between(self):
        self.assertSqlExeEqual("""\
        SELECT
           albumid,
           COUNT(trackid)
        FROM
           tracks
        GROUP BY
           albumid
        HAVING
           COUNT(albumid) BETWEEN 18 AND 20
        ORDER BY albumid;
        """, sqlclz.select_from(
            Tracks.AlbumId, sqlclz.count(Tracks.TrackId)
        ).group_by(Tracks.AlbumId).having(
            sqlclz.count(Tracks.AlbumId).between(18, 20)
        ).order_by(Tracks.AlbumId))

    def test_having_join(self):
        self.assertSqlExeEqual("""\
        SELECT
            tracks.AlbumId,
            title,
            SUM(Milliseconds) AS length
        FROM
            tracks
        INNER JOIN albums ON albums.AlbumId = tracks.AlbumId
        GROUP BY
            tracks.AlbumId
        HAVING
            length > 60000000;
        """, sqlclz.select_from(
            Tracks.AlbumId,
            Albums.Title,
            (length := sqlclz.sum(Tracks.Milliseconds) @ 'length')
        ).join(
            Albums.AlbumId == Tracks.AlbumId, by='inner'
        ).group_by(Tracks.AlbumId).having(
            length > 60000000
        ))


@sqlclz.named_tuple_table_class
class T1(NamedTuple):
    v1: int


@sqlclz.named_tuple_table_class
class T2(NamedTuple):
    v2: int


def create_t1t2():
    connection = sqlclz.Connection()

    with connection:
        sqlclz.create_table(T1).submit()
        sqlclz.create_table(T2).submit()
        sqlclz.insert_into(T1).submit([T1(1), T1(2), T1(3)])
        sqlclz.insert_into(T2).submit([T2(2), T2(3), T2(4)])

    return connection


class UnionTest(SqlTestCase):
    """
    https://www.sqlitetutorial.net/sqlite-union/
    """

    def test_union(self):
        self.connection = create_t1t2()

        self.assertSqlExeEqual("""\
        SELECT v1
          FROM t1
        UNION
        SELECT v2
          FROM t2;
        """, sqlclz.select_from(T1.v1) | sqlclz.select_from(T2.v2))

    def test_union_all(self):
        self.connection = create_t1t2()

        self.assertSqlExeEqual("""\
       SELECT v1
         FROM t1
       UNION ALL
       SELECT v2
         FROM t2;
       """, sqlclz.select_from(T1.v1).union(sqlclz.select_from(T2.v2), all=True))

    def test_union_example(self):
        a = sqlclz.select_from(Employees.FirstName, Employees.LastName, sqlclz.alias('Employee', 'Type'))
        b = sqlclz.select_from(Customers.FirstName, Customers.LastName, 'Customer')
        self.assertSqlExeEqual("""\
        SELECT FirstName, LastName, 'Employee' AS Type
        FROM employees
        UNION
        SELECT FirstName, LastName, 'Customer'
        FROM customers;
        """, a | b)

    def test_union_order_by(self):
        a = sqlclz.select_from(Employees.FirstName, Employees.LastName, sqlclz.alias('Employee', 'Type'))
        b = sqlclz.select_from(Customers.FirstName, Customers.LastName, 'Customer')
        self.assertSqlExeEqual("""\
        SELECT FirstName, LastName, 'Employee' AS Type
        FROM employees
        UNION
        SELECT FirstName, LastName, 'Customer'
        FROM customers
        ORDER BY FirstName, LastName;
        """, (a | b).order_by(Employees.FirstName, Employees.LastName))


class ExceptTest(SqlTestCase):
    """
    https://www.sqlitetutorial.net/sqlite-except/
    """

    def test_except_oper(self):
        self.connection = create_t1t2()

        self.assertSqlExeEqual("""\
        SELECT v1
          FROM t1
        EXCEPT
        SELECT v2
          FROM t2;
        """, sqlclz.select_from(T1.v1) - sqlclz.select_from(T2.v2))

    def test_except(self):
        self.assertSqlExeEqual("""\
        SELECT ArtistId
        FROM artists
        EXCEPT
        SELECT ArtistId
        FROM albums;
        """, sqlclz.select_from(Artists.ArtistId).except_(sqlclz.select_from(Albums.ArtistId)))


class IntersectTest(SqlTestCase):
    """
    https://www.sqlitetutorial.net/sqlite-intersect/
    """

    def test_intersect_oper(self):
        self.connection = create_t1t2()

        self.assertSqlExeEqual("""\
        SELECT v1
          FROM t1
        INTERSECT
        SELECT v2
          FROM t2;
        """, sqlclz.select_from(T1.v1) & sqlclz.select_from(T2.v2))

    def test_intersect(self):
        self.assertSqlExeEqual("""\
        SELECT CustomerId
        FROM customers
        INTERSECT
        SELECT CustomerId
        FROM invoices
        ORDER BY CustomerId;
        """, sqlclz.select_from(Customers.CustomerId).intersect(sqlclz.select_from(Invoices.CustomerId)).order_by(Customers.CustomerId))


class SubQueryTest(SqlTestCase):
    """
    https://www.sqlitetutorial.net/sqlite-subquery/
    """

    def test_sub_query(self):
        self.assertSqlExeEqual("""\
        SELECT trackid,
               name,
               albumid
        FROM tracks
        WHERE albumid = (
           SELECT albumid
           FROM albums
           WHERE title = 'Let There Be Rock'
        );
        """, sqlclz.select_from(
            Tracks.TrackId,
            Tracks.Name,
            Tracks.AlbumId
        ).where(
            Tracks.AlbumId == sqlclz.select_from(
                Albums.AlbumId
            ).where(
                Albums.Title == 'Let There Be Rock'
            )
        ))

    def test_sub_query_in(self):
        self.assertSqlExeEqual("""\
        SELECT customerid,
               firstname,
               lastname
          FROM customers
        WHERE supportrepid IN (
               SELECT employeeid
                 FROM employees
                WHERE country = 'Canada'
        );
        """, sqlclz.select_from(
            Customers.CustomerId,
            Customers.FirstName,
            Customers.LastName
        ).where(
            sqlclz.contains(Customers.SupportRepId, sqlclz.select_from(
                Employees.EmployeeId
            ).where(Employees.Country == 'Canada'))
        ))

    def test_sub_query_from(self):
        from_table = sqlclz.select_from(
            sqlclz.sum(Tracks.Bytes) @ 'size',
            from_table=Tracks
        ).group_by(Tracks.AlbumId) @ 'album'

        self.assertSqlExeEqual("""\
        SELECT
            AVG(album.size)
        FROM
            (
                SELECT
                    SUM(bytes) SIZE
                FROM
                    tracks
                GROUP BY
                    albumid
            ) AS album;
        """, sqlclz.select_from(sqlclz.avg(from_table.size), from_table=from_table))

    def test_sub_query_correlated(self):
        self.assertSqlExeEqual("""\
        SELECT albumid,
               title
          FROM albums
         WHERE 10000000 > (
                              SELECT sum(bytes)
                                FROM tracks
                               WHERE tracks.AlbumId = albums.AlbumId
                          )
         ORDER BY title;
        """, sqlclz.select_from(
            Albums.AlbumId,
            Albums.Title
        ).where(sqlclz.wrap(10000000) > sqlclz.select_from(sqlclz.sum(Tracks.Bytes)).where(
            Tracks.AlbumId == Albums.AlbumId
        )).order_by(Albums.Title))

    def test_sub_query_select(self):
        self.assertSqlExeEqual("""\
        SELECT albumid,
               title,
               (
                   SELECT count(trackid)
                     FROM tracks
                    WHERE tracks.AlbumId = albums.AlbumId
               )
               tracks_count
          FROM albums
         ORDER BY tracks_count DESC;
        """, sqlclz.select_from(
            Albums.AlbumId,
            Albums.Title,
            (tracks_count := sqlclz.select_from(
                sqlclz.count(Tracks.TrackId),
                from_table=Tracks
            ).where(
                Tracks.AlbumId == Albums.AlbumId
            ) @ 'tracks_count')
        ).order_by(sqlclz.desc(tracks_count)))


class ExistTest(SqlTestCase):
    """
    https://www.sqlitetutorial.net/sqlite-exists/
    """

    def test_exists(self):
        self.assertSqlExeEqual("""\
        SELECT
            CustomerId,
            FirstName,
            LastName,
            Company
        FROM
            Customers c
        WHERE
            EXISTS (
                SELECT 
                    1 
                FROM 
                    Invoices
                WHERE 
                    CustomerId = c.CustomerId
            )
        ORDER BY
            FirstName,
            LastName; 
        """, sqlclz.select_from(Customers.CustomerId, Customers.FirstName, Customers.LastName, Customers.Company).where(
            sqlclz.exists(sqlclz.select_from(1, from_table=Invoices).where(Invoices.CustomerId == Customers.CustomerId))
        ).order_by(Customers.FirstName, Customers.LastName))

    def test_exists_table(self):
        self.assertSqlExeEqual("""\
        SELECT
            CustomerId,
            FirstName,
            LastName,
            Company
        FROM
            Customers c
        WHERE
            EXISTS (
                SELECT 
                    1 
                FROM 
                    Invoices
                WHERE 
                    CustomerId = c.CustomerId
            )
        ORDER BY
            FirstName,
            LastName; 
        """, sqlclz.select_from(Customers.CustomerId, Customers.FirstName, Customers.LastName, Customers.Company).where(
            sqlclz.exists(Invoices, Invoices.CustomerId == Customers.CustomerId)
        ).order_by(Customers.FirstName, Customers.LastName))

    def test_use_in_replace_exists(self):
        self.assertSqlExeEqual("""\
        SELECT
           CustomerId, 
           FirstName, 
           LastName, 
           Company
        FROM
           Customers c
        WHERE
           CustomerId IN (
              SELECT
                 CustomerId
              FROM
                 Invoices
           )
        ORDER BY
           FirstName, 
           LastName;
        """, sqlclz.select_from(Customers.CustomerId, Customers.FirstName, Customers.LastName, Customers.Company).where(
            sqlclz.contains(Customers.CustomerId, sqlclz.select_from(Invoices.CustomerId))
        ).order_by(Customers.FirstName, Customers.LastName))

    def test_not_exists(self):
        self.assertSqlExeEqual("""\
        SELECT
           *
        FROM
           Artists a
        WHERE
           NOT EXISTS(
              SELECT
                 1
              FROM
                 Albums
              WHERE
                 ArtistId = a.ArtistId
           )
        ORDER BY Name;
        """, sqlclz.select_from(Artists).where(
            ~sqlclz.exists(sqlclz.select_from(1, from_table=Albums).where(Albums.ArtistId == Artists.ArtistId))
        ).order_by(Artists.Name))

    def test_not_exists_table(self):
        self.assertSqlExeEqual("""\
        SELECT
           *
        FROM
           Artists a
        WHERE
           NOT EXISTS(
              SELECT
                 1
              FROM
                 Albums
              WHERE
                 ArtistId = a.ArtistId
           )
        ORDER BY Name;
        """, sqlclz.select_from(Artists).where(
            ~sqlclz.exists(Albums, Albums.ArtistId == Artists.ArtistId)
        ).order_by(Artists.Name))


if __name__ == '__main__':
    unittest.main()

# sqlclz

A type-safe Python SQL query builder that uses NamedTuple classes to define database schemas and generate SQL
statements.

## Installation

```bash
pip install sqlclz
```

## Quick Start

### Define Your Schema

```python
from typing import NamedTuple, Annotated
from sqlclz import named_tuple_table_class, PRIMARY


@named_tuple_table_class
class Person(NamedTuple):
    name: Annotated[str, PRIMARY()]
    age: int
    email: str
```

### Create Tables and Insert Data

```python
import sqlclz
from sqlclz import Connection

# Create a connection (in-memory SQLite database)
with Connection() as conn:
    # Create table
    conn.execute(sqlclz.create_table(Person))

    # Insert data
    conn.execute(
        sqlclz.insert_into(Person),
        [("Alice", 30, "alice@example.com"),
         ("Bob", 25, "bob@example.com")]
    )

    # Query data
    result = conn.execute(
        sqlclz.select_from(Person).where(Person.age > 25)
    ).fetchall()

    print(result)
    # Output: [('Alice', 30, 'alice@example.com')]
```

### Type-Safe Queries

```python
# SELECT with specific fields
query = sqlclz.select_from(Person.name, Person.email)

# WHERE conditions with type safety
query = sqlclz.select_from(Person).where(
    Person.age >= 18,
    Person.age <= 65
)

# ORDER BY
query = sqlclz.select_from(Person).order_by(sqlclz.desc(Person.age))

# LIMIT and OFFSET
query = sqlclz.select_from(Person).limit(10, offset=5)
```

### Advanced Features

#### Foreign Keys

```python
from sqlclz import foreign


@named_tuple_table_class
class Department(NamedTuple):
    id: Annotated[int, PRIMARY(auto_increment=True)]
    name: str


@named_tuple_table_class
class Employee(NamedTuple):
    id: Annotated[int, PRIMARY(auto_increment=True)]
    name: str
    dept_id: int

    @foreign(Department)
    def _department(self):
        return self.dept_id
```

#### Joins

```python
# Join tables
query = sqlclz.select_from(
    Employee.name,
    Department.name
).join(
    Employee.dept_id == Department.id,
    by='inner'
)
```

#### Aggregations

```python
# Count, sum, avg, etc.
query = sqlclz.select_from(
    sqlclz.count(Person.name),
    sqlclz.avg(Person.age)
).group_by(Person.email)
```

## Features

- **Type-Safe**: Leverage Python's type system to catch errors at development time
- **Pythonic**: Define schemas using familiar NamedTuple syntax
- **SQL Generation**: Automatically generate correct SQL statements
- **Foreign Keys**: Support for foreign key relationships
- **Comprehensive**: Supports SELECT, INSERT, UPDATE, DELETE, JOIN, and more
- **Window Functions**: Support for advanced SQL window functions
- **CTE Support**: Common Table Expressions (WITH clause)

## Running Tests

```bash
# Install test dependencies
cd test/unit
curl -L -o chinook.zip https://www.sqlitetutorial.net/wp-content/uploads/2018/03/chinook.zip
unzip chinook.zip

# Run tests
python -m unittest discover -s test/unit -p "test*.py"
```

## Documentation

For more examples and detailed documentation, see the [test directory](test/unit/).

## License

[BSD 3-Clause](LICENSE)
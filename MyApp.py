import argparse
import enum
import os
import sqlite3
import sys
import time

from typing import Dict, Any
from datetime import datetime, date
from faker import Faker


class DBType(enum.Enum):
    """
    Enum, for storing possible types of table fields
    """
    NULL = "NULL"  # Null value
    INTEGER = "INTEGER PRIMARY KEY AUTOINCREMENT"  # Integer
    REAL = "REAL"  # Floating point number
    TEXT = "TEXT"  # Text
    DATE = "DATE"  # Date
    BLOB = "BLOB"  # Binary representation of large objects stored exactly as it was entered


class DataBase:
    def __init__(self, path: str, filename: str):
        """
        This method initializes the main class to operate (and manage) the database
        :param path: The path where the database is located (it is necessary to locate the database)
        :param filename: The name of the database (along with the extension). For example: 'some.bd'
        """
        self.__path = DataBase.__is_path_valid(path=path)
        self.__filename = DataBase.__is_filename_valid(filename=filename)
        self.__connector = sqlite3.connect(os.path.join(self.__path, self.__filename), check_same_thread=False)
        self.__cursor = self.__connector.cursor()

    def create_table(self, tablename: str, columns: Dict[str, DBType]) -> None:
        """
        This method creates a table in the database
        :param tablename: Table name
        :param columns: A dictionary where the key is the column name and the value is the data type
        """
        self.__cursor.execute(f"CREATE TABLE IF NOT EXISTS {tablename} ({self.__prepare_labels(columns)})")

    def delete_table(self, tablename: str) -> None:
        """
        This method deletes a table in the database
        :param tablename: Table name
        """
        self.__cursor.execute(f"DROP TABLE IF EXISTS {tablename}")

    @staticmethod
    def __prepare_labels(labels: Dict[str, DBType]) -> str:
        """
        This method converts a custom dictionary with columns and their types into a SQL-friendly part of the command
        :param labels:A dictionary where the key is the column name and the values are the data type from the DBType set
        """
        return ",".join([f"{label} {labels[label].value}" for label in labels])

    @staticmethod
    def __is_path_valid(path: str) -> str:
        """
        This method checks if this string is the path to the folder/file
        :param path: The intended path to the file
        """
        if os.path.exists(path):
            return path
        else:
            raise Exception(f"The path \'{path}\' is not valid!")

    @staticmethod
    def __is_filename_valid(filename: str) -> str:
        """
        This method checks if this string is the path to the folder/file
        :param filename: The intended path to the file
        """
        if filename.endswith(".db") or filename.endswith(".sqlite3"):
            return filename
        else:
            raise Exception(f"The filename \'{filename}\' is not valid!")

    def insert(self, tablename: str, row: Any) -> None:
        """
        This method adds a new row to the table
        :param tablename: Name of the table
        :param row: List of row values
        """
        self.__cursor.execute(f"INSERT OR IGNORE INTO {tablename} VALUES (NULL,?,?,?)", row)
        self.__connector.commit()

    def insert_many(self, tablename: str, rows: Any) -> None:
        """
        This method adds a new row to the table
        :param tablename: Name of the table
        :param rows: List of rows values
        """
        self.__cursor.executemany(f"INSERT OR IGNORE INTO {tablename} VALUES(NULL,?,?,?)", rows)
        self.__connector.commit()

    def read_all(self, tablename: str, column_names: Any, order_by: str) -> Any:
        """
        This method returns the values from the table located in the columns "column_names"
        :param tablename: Name of the table
        :param column_names: The name of the columns from which the value should be returned
        """
        self.__cursor.execute(f"SELECT {', '.join(column_names)} FROM {tablename} ORDER BY {order_by}")
        rows = self.__cursor.fetchall()

        result = [row + (my_sqlite3_database.__how_old_a_you(row),) for row in rows]
        return result

    def read(self, tablename: str, column: str, data: Any, column_names: Any) -> Any:
        """
        This method find the values from the table
        :param tablename: Name of the table
        :param column: Column where to find data
        :param data: Value to be found
        :param column_names: The name of the columns from which the value should be returned
        """
        self.__cursor.execute(f"SELECT {', '.join(column_names)} FROM {tablename} WHERE {column} Like '{data}'")
        rows = self.__cursor.fetchall()

        result = [row + (my_sqlite3_database.__how_old_a_you(row),) for row in rows]
        return result

    @staticmethod
    def __how_old_a_you(row: Any) -> int:
        """
        This method return the age of the user
        """
        birth_date = datetime.strptime(row[1], '%Y-%m-%d')
        today = date.today()
        age = today.year - birth_date.year - ((today.month, today.day) < (birth_date.month, birth_date.day))
        return age

    def find_name_f(self, tablename: str) -> Any:
        """
        This method find the values from the table WHERE name LIKE 'F%' AND gender = 'M'
        :param tablename: Name of the table
        """
        self.__cursor.execute(f"SELECT * FROM {tablename} WHERE name LIKE 'F%' AND gender = 'M'")
        rows = self.__cursor.fetchall()
        return rows

    def create_index(self, tablename: str, indexname: str) -> None:
        """
        This method add the index at the database ((name, gender) WHERE name LIKE 'F%' AND gender = 'M')
        :param tablename: Name of the table
        :param indexname: Name of the index
        """
        self.__cursor.execute(f"CREATE INDEX IF NOT EXISTS {indexname} ON {tablename} (name, gender) WHERE name LIKE 'F%' AND gender = 'M'")

    def delete_index(self, indexname: str) -> None:
        """
        This method delete the index at the database
        :param indexname: Name of the index
        """
        self.__cursor.execute(f"DROP INDEX IF EXISTS {indexname}")


def parse_args(args):
    """
    This method parse command line parameters
    :param args: Command line
    """
    my_parser = argparse.ArgumentParser(description='Select the option')

    my_parser.add_argument('case', choices=['1', '2', '3', '4', '5', '6', '7'], help='List of case commands')
    my_parser.add_argument('name', nargs='*', help='String of user (name, birthdate, gender)')

    args = my_parser.parse_args()

    if args.case == '2' and args.name == []:
        my_parser.error("Cannot use case 2 with NULL String of user (name, birthdate, gender)")

    return args


def generate_profile():
    """
    This method generates an object to write to the database
    """
    profile = fake.simple_profile()
    person = (profile['name'], str(profile['birthdate']), profile['sex'])
    return person


def generate_profile_f():
    """
    This method generates an object with name starts on F to write to the database
    """
    profile = fake.simple_profile()
    person = ('F' + profile['name'], str(profile['birthdate']), profile['sex'])
    return person


if __name__ == '__main__':
    my_parser = parse_args(sys.argv[1:])

    my_sqlite3_database = DataBase(path=os.getcwd(), filename="my_BD.db")

    if my_parser.case == '1':
        my_sqlite3_database.create_table(tablename="users",
                                         columns={"user_id": DBType.INTEGER,
                                                  "name": DBType.TEXT,
                                                  "date": DBType.DATE,
                                                  "gender": DBType.TEXT})
        print("Table created")

    if my_parser.case == '2':
        my_sqlite3_database.insert(tablename="users", row=my_parser.name)
        print("Insert completed")

    if my_parser.case == '3':
        for item in my_sqlite3_database.read_all(tablename="users", column_names=("name", "date", "gender"), order_by="name"):
            print(item)

    if my_parser.case == '4':
        people = []
        people_f = []
        fake = Faker()

        for i in range(0, 1_000_000):
            person = generate_profile()
            people.append(person)
        my_sqlite3_database.insert_many(tablename="users", rows=people)

        for i in range(0, 100):
            person = generate_profile_f()
            people_f.append(person)
        my_sqlite3_database.insert_many(tablename="users", rows=people_f)

        print("Generate completed")

    if my_parser.case == '5':
        start_time = time.time()
        result = my_sqlite3_database.find_name_f(tablename="users")
        end_time = time.time()

        for item in result:
            print(item)

        print("--- %s seconds ---" % (end_time - start_time))

    if my_parser.case == '6':
        my_sqlite3_database.create_index(tablename="users", indexname="my_index")
        print("Index created")

        start_time = time.time()
        result = my_sqlite3_database.find_name_f(tablename="users")
        end_time = time.time()

        for item in result:
            print(item)

        print("--- %s seconds ---" % (end_time - start_time))

    if my_parser.case == '7':
        print("Test case for debugging and custom commands")

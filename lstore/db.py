import os

from lstore.disk import Disk
from lstore.table import Table

class Database():

    def __init__(self):
        self.tables:dict[str,Table] = dict()
        self.db_path = None

    def __load_database(self):
        for table_path in os.listdir(self.db_path):
            table_path = os.path.join(self.db_path, table_path)
            metadata = Disk.read_from_path_metadata(table_path)
            self.tables[table_path] = Table(
                metadata["table_path"],
                metadata["num_columns"],
                metadata["key_index"],
                metadata["num_records"]
            )

    def open(self, path:str)->None:
        self.db_path = path
        try:
            Disk.create_path_directory(path)
        except FileExistsError:
            print(f"Database at path {path} already exists.")
            self.__load_database()
        else:
            print(f"Database at path {path} created.")

    def close(self):
        for table_path in self.tables.keys():
            del self.tables[table_path]
        self.db_path = None

    def create_table(self, name:str, num_columns:int, key_index:int):
        """
        Create new table.

        :param name: string         #Table name
        :param num_columns: int     #Number of Columns: all columns are integer
        :param key: int             #Index of table key in columns
        """
        # create table directory
        table_path = os.path.join(self.db_path, name)
        if os.path.exists(table_path): raise FileExistsError
        os.mkdir(table_path)
        
        # create table object and its metadata
        metadata = {
            "table_path": table_path,
            "num_columns": num_columns,
            "key_index": key_index,
            "num_records": 0,
        }
        Disk.write_to_path_metadata(table_path, metadata)
        table = Table(table_path, num_columns, key_index, 0)
        return table

    def drop_table(self, name:str):
        """
        Delete specified table.
        """
        del self.tables[name]

    def get_table(self, name:str):
        """
        Return table with passed name.
        """
        return self.tables[name]

import os

from lstore.disk import Disk
from lstore.record_info import Record, RID
from lstore.page_info import Page_Range
from lstore.index import Index

class Table:

    def __init__(self, table_path:str, num_columns:int, key_index:int, num_records:int)->None:
        self.table_path:str                   = table_path
        self.num_columns:int                  = num_columns
        self.key_index:int                    = key_index
        self.num_records:int                  = num_records

        self.index:Index                      = Index(self.table_path, self.num_columns, self.key_index)

        self.page_ranges:dict[int,Page_Range] = dict()
        self.__load_page_ranges()

    def __del__(self)->None:
        del self.page_ranges
        self.page_ranges = None

    def __increment_num_records(self)->None:
        # increment number of records in object
        self.num_records += 1

        # increment number of records in data
        metadata = Disk.read_from_path_metadata(self.table_path)
        metadata["num_records"] = self.num_records
        Disk.write_to_path_metadata(self.table_path, metadata)

    def __get_page_ranges(self)->tuple[list[str],int]:
        page_range_dirs = Disk.list_directories_in_path(self.table_path)
        return (page_range_dirs, len(page_range_dirs))

    def __load_page_ranges(self):
        """
        Load page ranges from disk.
        """
        page_range_paths, num_page_ranges = self.__get_page_ranges()
        if not num_page_ranges: return
        for page_range_path in page_range_paths:
            page_range_index = int(os.path.basename(page_range_path).removeprefix("PR"))
            metadata = Disk.read_from_path_metadata(page_range_path)
            self.page_ranges[page_range_index] = Page_Range(
                metadata["page_range_path"],
                metadata["page_range_index"],
                metadata["latest_tid"],
                metadata["tps_index"],
            )

    def __merge(self):
        print("merge is happening")
        # TODO

    def __create_page_range(self, page_range_index:int):
        page_range_path = os.path.join(self.table_path, f"PR{page_range_index}")
        if os.path.exists(page_range_path): raise FileExistsError
        Disk.create_path_directory(page_range_path)
        metadata = {
            "page_range_path": page_range_path,
            "page_range_index": page_range_index,
            "latest_tid": 0,
            "tps_index": 0,
        }
        Disk.write_to_path_metadata(page_range_path, metadata)
        self.page_ranges[page_range_index] = Page_Range(
            metadata["page_range_path"],
            metadata["page_range_index"],
            metadata["latest_tid"],
            metadata["tps_index"],
        )

    def __access_page_range(self, page_range_index:int)->None:
        if not page_range_index in self.page_ranges:
            self.__create_page_range(page_range_index)

    def insert_record(self, columns:tuple)->None:
        """
        Insert record to table.
        """
        # increment num_records first (base RID starts at 1)
        self.__increment_num_records()

        # create record
        record = Record(self.num_records, self.key_index, columns)

        # insert to index
        self.index.insert(record.get_columns(), record.get_rid())

        # insert to physical disk
        self.__access_page_range(record.get_page_range_index())
        self.page_ranges[record.get_page_range_index()].insert_record(record)

    def select_record(self, search_key, search_key_index:int, selected_columns:list=None, rollback_version:int=0)->list[Record]:
        rlist = list()
        # get specific RIDs from index
        try:
            rids = self.index.locate(search_key, search_key_index)
        # if no index available, conduct full table scan
        except KeyError:
            rids = [RID(i) for i in range(1, self.num_records + 1)]
        for rid in rids:
            self.__access_page_range(rid.get_page_range_index())
            columns = self.page_ranges[rid.get_page_range_index()].get_record_columns(rid, rollback_version)
            
            # conditional that avoids creating records for non-searched info (only really useful for full table scans)
            if columns[search_key_index] != search_key: continue
            
            # construct record and add to records list
            if selected_columns != None:
                assert len(columns) == len(selected_columns)
                columns = tuple([_ for i, _ in enumerate(columns) if selected_columns[i] == 1])
            rlist.append(Record(rid, self.key_index, columns))
        return rlist

    def sum_records(self, start_range, end_range, aggregate_column_index:int, rollback_version:int=0)->int:
        rsum = 0
        rids = self.index.locate_range(start_range, end_range, self.key_index)
        for rid in rids:
            self.__access_page_range(rid.get_page_range_index())
            columns = self.page_ranges[rid.get_page_range_index()].get_record_columns(rid, rollback_version)
            rsum += columns[aggregate_column_index]
        return rsum

    def update_record(self, primary_key, new_columns:tuple)->None:
        # identify RID
        rids = self.index.locate(primary_key, self.key_index)
        if len(rids) == 0: return
        assert len(rids) == 1
        rid = rids.pop()

        # update entry values associated to RID in index
        old_columns = self.select_record(primary_key, self.key_index)[0].get_columns()
        assert len(old_columns) == len(new_columns)
        self.index.update(old_columns, new_columns, rid)

        # update record in disk
        self.__access_page_range(rid.get_page_range_index())
        self.page_ranges[rid.get_page_range_index()].update_record(rid, old_columns, new_columns)

    def delete_record(self, primary_key)->None:
        rids = self.index.locate(primary_key, self.key_index)
        assert len(rids) == 1
        rid = rids.pop()

        # delete record from index
        columns = self.select_record(primary_key, self.key_index)[0].get_columns()
        self.index.delete(columns, rid)

        # delete record from disk
        self.__access_page_range(rid.get_page_range_index())
        self.page_ranges[rid.get_page_range_index()].delete_record(rid)

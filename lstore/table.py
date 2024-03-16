import os

from lstore.disk import Disk
from lstore.record_info import Record, RID
from lstore.page_info import Page_Range
from lstore.index import Index
from lstore.lock_info import LM

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
        # increment number of records in memory
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

    def __abort_read_lock(self, rid:RID)->bool:
        LM.release_read(rid.get_page_range_index())
        return False

    def __succeed_read_lock(self, rid:RID)->bool:
        LM.release_read(rid.get_page_range_index())
        return True
    
    def __abort_write_lock(self, rid:RID)->bool:
        LM.release_write(rid.get_page_range_index())
        return False
    
    def __succeed_write_lock(self, rid:RID)->bool:
        LM.release_write(rid.get_page_range_index())
        return True

    def insert_record(self, columns:tuple)->None:
        """
        Insert record to table.
        """

        # lock RID
        while not LM.acquire_write(RID(self.num_records + 1).get_page_range_index()): pass

        # create RID from num_records (base RID starts at 1)
        rid = RID(self.num_records + 1)

        # perform checks that may cause operation to be aborted
        try:
            # number of columns in inserted column is wrong
            if len(columns) != self.num_columns: raise Exception
            # key already exists in table
            if len(self.index.locate(columns[self.key_index], self.key_index)): raise Exception
        except Exception:
            return self.__abort_write_lock(rid)            
        else:
            # create record
            record = Record(rid, self.key_index, columns)

            # insert to index
            self.index.insert(record.get_columns(), rid)

            # insert to physical disk
            self.__access_page_range(record.get_page_range_index())
            self.page_ranges[record.get_page_range_index()].insert_record(record)
            
            # apply new num_records to table's metadata
            self.__increment_num_records()

            return self.__succeed_write_lock(rid)

    def select_record(self, search_key, search_key_index:int, selected_columns:list=None, rollback_version:int=0)->list[Record]:
        rlist = list()

        # get specific RIDs from index
        try:
            rids = self.index.locate(search_key, search_key_index)
        # if no index available, conduct full table scan
        except KeyError:
            rids = {RID(i) for i in range(1, self.num_records + 1)}

        # construct a list of records
        for rid in rids:
            # lock RID
            print(f"KEY {search_key} TRYING TO ACCESS READ")
            while not LM.acquire_read(rid.get_page_range_index(), search_key): pass
            print(f"KEY {search_key} MADE IT PAST READ. ASSOCIATED RID: {rid}")

            try:
                # access column values from disk
                self.__access_page_range(rid.get_page_range_index())
                columns = self.page_ranges[rid.get_page_range_index()].get_record_columns(rid, rollback_version)
                print(f"{search_key} FOR RID {rid} MADE IT PAST COLUMN ACCESS FROM DISK")

                # conditional that avoids creating records for non-searched info (only really useful for full table scans)
                if columns[search_key_index] != search_key: continue

                # construct record and add to records list
                if selected_columns != None:
                    if len(columns) != len(selected_columns): raise Exception
                    columns = tuple([_ for i, _ in enumerate(columns) if selected_columns[i] == 1])
                rlist.append(Record(rid, self.key_index, columns))
                print(f"{search_key} HAS APPENDED RID {rid} TO THE LIST")
            except Exception:
                return self.__abort_read_lock(rid)
            else:
                self.__succeed_write_lock(rid)

        return rlist

    def sum_records(self, start_range, end_range, aggregate_column_index:int, rollback_version:int=0)->int:
        rsum = 0
        
        # get RIDs
        try:
            rids = self.index.locate_range(start_range, end_range, self.key_index)
        except KeyError:
            rids = {RID(i) for i in range(1, self.num_records + 1)}

        for rid in rids:
            # lock RID
            while not LM.acquire_read(rid.get_page_range_index()): pass

            try:
                # access column from disk
                self.__access_page_range(rid.get_page_range_index())
                columns = self.page_ranges[rid.get_page_range_index()].get_record_columns(rid, rollback_version)
                rsum += columns[aggregate_column_index]
            except Exception:
                return self.__abort_read_lock(rid)
            else:
                self.__succeed_read_lock(rid)

        return rsum

    def update_record(self, primary_key, new_columns:tuple)->bool:
        # identify RID
        rids = self.index.locate(primary_key, self.key_index)
        if len(rids) == 0: return
        assert len(rids) == 1
        rid = rids.pop()

        # lock RID
        while not LM.acquire_write(rid.get_page_range_index()): pass
        print(f"ACCESSING UPDATE FOR RID {rid}")

        # perform checks that may abort the operation
        try:
            # get old columns associated to RID
            old_columns = self.select_record(primary_key, self.key_index)[0].get_columns()
            if len(old_columns) != len(new_columns): raise Exception
        except Exception:
            print(f"RETURNING ABORTED UDPATE FOR {rid}")
            return self.__abort_write_lock(rid)
        else:
            # update entry values associated to RID in index
            self.index.update(old_columns, new_columns, rid)

            # update record in disk
            self.__access_page_range(rid.get_page_range_index())
            self.page_ranges[rid.get_page_range_index()].update_record(rid, old_columns, new_columns)

            print(f"RETURNING SUCCESSFUL UPDATE FOR {rid}")
            return self.__succeed_write_lock(rid)

    def delete_record(self, primary_key)->bool:
        rids = self.index.locate(primary_key, self.key_index)
        assert len(rids) == 1
        rid = rids.pop()

        # lock RID
        while not LM.acquire_write(rid.get_page_range_index()): pass

        try:
            # delete record from index
            columns = self.select_record(primary_key, self.key_index)[0].get_columns()
        except Exception:
            return self.__abort_write_lock(rid)
        else:
            # delete info associated to RID in index
            self.index.delete(columns, rid)

            # delete record from disk
            self.__access_page_range(rid.get_page_range_index())
            self.page_ranges[rid.get_page_range_index()].delete_record(rid)

            return self.__succeed_write_lock(rid)


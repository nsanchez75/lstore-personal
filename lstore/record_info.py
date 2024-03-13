import lstore.config as Config

class RID:

    def __init__(self, rid:int)->None:
        self.rid:int = int(rid)

    def __repr__(self)->str:
        return str(self.rid)

    def __str__(self)->str:
        return str(self.rid)

    def __int__(self)->int:
        return self.rid

    def get_page_range_index(self)->int:
        return (abs(self.rid) - 1) // (Config.NUM_RECORDS_PER_PAGE * Config.NUM_BASE_PAGES_PER_PAGE_RANGE)

    def get_base_page_index(self)->int:
        return ((abs(self.rid)- 1) // Config.NUM_RECORDS_PER_PAGE) % Config.NUM_BASE_PAGES_PER_PAGE_RANGE


class TID(RID):

    def __init__(self, rid:int)->None:
        super().__init__(rid)

    def __int__(self)->int:
        return self.rid

    def get_tail_page_index(self)->int:
        return (abs(self.rid) - 1) // Config.NUM_RECORDS_PER_PAGE


class Record:

    def __init__(self, rid:int, key_index:int, columns:tuple)->None:
        self.rid:RID       = RID(rid)
        self.key_index:int = key_index
        self.columns:tuple = columns

    def __str__(self)->str:
        return f"RID {self.rid}: {self.columns}"

    def get_rid(self)->RID:
        return self.rid

    def set_rid(self, new_rid:RID)->None:
        self.rid = new_rid

    def get_columns(self)->tuple:
        return self.columns

    def get_page_range_index(self)->int:
        return self.rid.get_page_range_index()

    def get_base_page_index(self)->int:
        return self.rid.get_base_page_index()

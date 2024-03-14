from threading import Lock
from collections import defaultdict

from lstore.record_info import RID


class Lock_Manager:

    def __init__(self)->None:
        self.locks:defaultdict[RID,RWL] = defaultdict()

    def get_locks(self)->defaultdict:
        return self.locks

    def acquire_read(self, rid:RID)->bool:
        self.locks[rid].acquire_read()

    def acquire_write(self, rid:RID)->bool:
        self.locks[rid].acquire_write()
    
    def release_read(self, rid:RID)->None:
        self.locks[rid].release_read()

    def release_write(self, rid:RID)->None:
        self.locks[rid].release_write()


class RWL:

    def __init__(self)->None:
        self.num_readers:int = 0
        self.is_writer:bool  = False
        self.lock:Lock       = Lock()

    def acquire_read(self)->bool:
        self.lock.acquire()
        if self.is_writer:
            self.lock.release()
            return False
        self.num_readers += 1
        self.lock.release()
        return True

    def acquire_write(self)->bool:
        self.lock.acquire()
        if self.is_writer or self.num_readers:
            self.lock.release()
            return False
        self.is_writer = True
        self.lock.release()
        return True

    def release_read(self)->None:
        self.lock.acquire()
        self.num_readers -= 1
        self.lock.release()

    def release_write(self)->None:
        self.lock.acquire()
        self.is_writer = False
        self.lock.release()

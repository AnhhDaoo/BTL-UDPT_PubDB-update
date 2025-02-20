"""
    Core module containing entrypoint functions for PupDB.
"""

import logging
import os
import json
import traceback
# thêm thư viện quản lý luồng
import threading

from filelock import FileLock

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(process)d | %(levelname)s | %(message)s'
)


# pylint: disable=useless-object-inheritance
class PupDB(object):
    """ This class represents the core of the PupDB database. """
        
    def __init__(self, db_file_path):
        """ Initializes the PupDB database instance. """
        
        self.db_file_path = db_file_path
        self.process_lock_path = '{}.lock'.format(db_file_path)
        self.process_lock = FileLock(self.process_lock_path, timeout=-1)
        self._thread_lock = threading.Lock()  # Khóa cấp luồng
        self.init_db()    

    def __repr__(self):
        """ String representation of this class instance. """

        return str(self._get_database())

    def __len__(self):
        """ Function to return the size of iterable. """

        return len(self._get_database())

    def init_db(self):
        """ Initializes the database file. """

        with self.process_lock:
            if not os.path.exists(self.db_file_path):
                with open(self.db_file_path, 'w') as db_file:
                    db_file.write(json.dumps({}))
        return True

    def _get_database(self):
        """ Returns the database json object. """

        with self.process_lock:
            with open(self.db_file_path, 'r') as db_file:
                database = json.loads(db_file.read())
                return database

    def _flush_database(self, database):
        """ Flushes/Writes the database changes to disk. """

        with self.process_lock:
            with open(self.db_file_path, 'w') as db_file:
                db_file.write(json.dumps(database))
                return True


    # def set(self, key, val):
    # # Giữ khóa file cho toàn bộ quá trình read-modify-write.
    #     with self.process_lock:
    #         try:
    #             # Đọc database (không cần _get_database() vì đã có khóa)
    #             with open(self.db_file_path, 'r') as db_file:
    #                 database = json.load(db_file)
    #             # Cập nhật dữ liệu
    #             database[key] = val
    #             # Ghi lại database
    #             with open(self.db_file_path, 'w') as db_file:
    #                 json.dump(database, db_file)
    #         except Exception:
    #             logging.error('Error while writing to DB: %s', traceback.format_exc())
    #             return False
    #         return True


    def set(self, key, val):
        with self._thread_lock:
            with self.process_lock:
                try:
                    with open(self.db_file_path, 'r') as db_file:
                        database = json.load(db_file)
                    database[key] = val
                    with open(self.db_file_path, 'w') as db_file:
                        json.dump(database, db_file)
                except Exception:
                    logging.error('Error while writing to DB: %s', traceback.format_exc())
                    return False
        return True


    # def get(self, key):
    #     """
    #         Gets the value of a key from the database.
    #         Returns None if the key is not found in the database.
    #     """

    #     key = str(key)
    #     database = self._get_database()
    #     return database.get(key, None)

    def get(self, key):
        key = str(key)
        with self._thread_lock:
            database = self._get_database()
        return database.get(key, None)

        
    # def remove(self, key):
    #     """
    #     Removes a key from the database.
    #     """
    #     key = str(key)
    #     with self.process_lock:
    #         # Đọc database
    #         with open(self.db_file_path, 'r') as db_file:
    #             database = json.load(db_file)
    #         # Nếu key không tồn tại, raise KeyError ngay
    #         if key not in database:
    #             raise KeyError('Non-existent Key {} in database'.format(key))
    #         # Xoá key
    #         del database[key]
    #         # Ghi lại database
    #         with open(self.db_file_path, 'w') as db_file:
    #             json.dump(database, db_file)
    #     return True

    def remove(self, key):
        key = str(key)
        with self._thread_lock:
            with self.process_lock:
                with open(self.db_file_path, 'r') as db_file:
                    database = json.load(db_file)
                if key not in database:
                    raise KeyError('Non-existent Key {} in database'.format(key))
                del database[key]
                with open(self.db_file_path, 'w') as db_file:
                    json.dump(database, db_file)
        return True

        
    # def keys(self):
    #     """
    #         Returns a list (py27) or iterator (py3) of all the keys
    #         in the database.
    #     """

    #     return self._get_database().keys()

    def keys(self):
        with self._thread_lock:
            return self._get_database().keys()


    # def values(self):
    #     """
    #         Returns a list (py27) or iterator (py3) of all the values
    #         in the database.
    #     """

    #     return self._get_database().values()
    
    def values(self):
        with self._thread_lock:
            return self._get_database().values()


    # def items(self):
    #     """
    #         Returns a list (py27) or iterator (py3) of all the items i.e.
    #         (key, val) pairs in the database.
    #     """

    #     return self._get_database().items()
    
    def items(self):
        with self._thread_lock:
            return self._get_database().items()


    # def dumps(self):
    #     """ Returns a string dump of the entire database sorted by key. """

    #     return json.dumps(self._get_database(), sort_keys=True)
    
    def dumps(self):
        with self._thread_lock:
            return json.dumps(self._get_database(), sort_keys=True)


    # def truncate_db(self):
    #     """ Truncates the entire database (makes it empty). """

    #     self._flush_database({})
    #     return True
    
    def truncate_db(self):
        with self._thread_lock:
            self._flush_database({})
        return True


from constans import DATA_RECORDS_PER_PAGE, DATA_RECORD_SIZE
from record import Record


class DataPage:
    def __init__(self, records_per_page):
        self.max_size = records_per_page * DATA_RECORD_SIZE
        self.current_size = 0
        self.records = []
        self.page_number = 1

    def add_record(self, record):
        self.records.append(Record(record[0], record[1:]))
        self.current_size += DATA_RECORD_SIZE

    def is_full(self):
        return self.current_size == self.max_size

    def clear_page(self):
        self.current_size = 0
        self.records = []
        self.page_number += 1

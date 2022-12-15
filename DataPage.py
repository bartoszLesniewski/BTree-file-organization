from constans import DATA_RECORDS_PER_PAGE, DATA_RECORD_SIZE, INT_SIZE, BYTE_ORDER, MAX_INT, DATA_RECORD_LENGTH
from record import Record


class DataPage:
    next_page = 1

    def __init__(self, records_per_page, page_number=None):
        self.records_per_page = records_per_page
        self.max_size = records_per_page * DATA_RECORD_SIZE
        self.current_size = 0
        self.records = []

        if page_number is None:
            self.page_number = DataPage.next_page
            DataPage.next_page += 1
        else:
            self.page_number = page_number

    def add_record(self, record):
        self.records.append(Record(record[0], record[1:]))
        self.current_size += DATA_RECORD_SIZE

    def is_full(self):
        return len(self.records) == self.records_per_page

    def clear_page(self):
        self.current_size = 0
        self.records = []
        self.page_number += 1

    def serialize(self):
        bytes_entries = []

        for index, record in enumerate(self.records):
            bytes_entries.append(record.key.to_bytes(INT_SIZE, BYTE_ORDER))

            for number in record.numbers:
                bytes_entries.append(number.to_bytes(INT_SIZE, BYTE_ORDER))

            if len(record.numbers) < DATA_RECORD_LENGTH:
                for _ in range(DATA_RECORD_LENGTH - len(record.numbers)):
                    bytes_entries.append(MAX_INT.to_bytes(INT_SIZE, BYTE_ORDER))

        if len(self.records) < self.records_per_page:
            for _ in range(self.records_per_page - len(self.records)):
                bytes_entries.append(MAX_INT.to_bytes(INT_SIZE, BYTE_ORDER))  # key
                for _ in range(DATA_RECORD_LENGTH):
                    bytes_entries.append(MAX_INT.to_bytes(INT_SIZE, BYTE_ORDER))

        return bytes_entries

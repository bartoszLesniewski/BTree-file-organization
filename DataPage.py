from constans import DATA_RECORD_SIZE, INT_SIZE, BYTE_ORDER, MAX_INT, DATA_RECORD_LENGTH


class DataRecord:
    def __init__(self, key=None, numbers=None):
        self.key = key
        if numbers is None:
            self.numbers = []
        else:
            self.numbers = numbers

    def print(self):
        print(self.key, end=": ")
        print(self.numbers)


class DataPage:
    next_page = 1
    max_size = 0

    def __init__(self, records_per_page, page_number=None):
        self.records_per_page = records_per_page
        DataPage.max_size = records_per_page * DATA_RECORD_SIZE
        self.records = []
        self.dirty_bit = False

        if page_number is None:
            self.page_number = DataPage.next_page
            DataPage.next_page += 1
        else:
            self.page_number = page_number

    def add_record(self, record):
        self.records.append(DataRecord(record[0], record[1:]))
        self.dirty_bit = True

    def is_full(self):
        return len(self.records) == self.records_per_page

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

    def print_record(self, key):
        for record in self.records:
            if record.key == key:
                record.print()
                break

    def remove_record(self, key):
        record_to_remove = None
        for record in self.records:
            if record.key == key:
                record_to_remove = record
                break

        self.records.remove(record_to_remove)
        self.dirty_bit = True

    def is_dirty(self):
        return self.dirty_bit

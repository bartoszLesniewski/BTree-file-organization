from constans import INDEX_ENTRIES_PER_PAGE, INDEX_ENTRY_SIZE, INT_SIZE, BYTE_ORDER, MAX_INT, INDEX_RECORD_SIZE


class IndexPage:
    next_page = 1

    def __init__(self, records_per_page, page_number=None):
        self.records_per_page = records_per_page
        self.max_size = records_per_page * (3 * INT_SIZE) + INT_SIZE
        self.current_size = 0

        if page_number is None:
            self.page_number = IndexPage.next_page
            IndexPage.next_page += 1
        else:
            self.page_number = page_number

        self.records = []
        self.pointers = []

    def add_entry(self, key, page_number):
        if not self.records:
            self.pointers.append(None)  # left son
            self.current_size += INT_SIZE

        self.records.append([key, page_number])
        self.pointers.append(None)  # right son
        self.current_size += 3 * INT_SIZE

    def is_full(self):
        return self.current_size == self.max_size

    def clear_page(self):
        self.current_size = 0
        self.records = []
        self.pointers = []
        self.page_number += 1

    def serialize(self):
        bytes_entries = []

        if self.pointers:
            bytes_entries.append(self.pointers[0].to_bytes(INT_SIZE, BYTE_ORDER))
        else:
            bytes_entries.append(MAX_INT.to_bytes(INT_SIZE, BYTE_ORDER))

        for index, record in enumerate(self.records):
            bytes_entries.append(record[0].to_bytes(INT_SIZE, BYTE_ORDER))
            bytes_entries.append(record[1].to_bytes(INT_SIZE, BYTE_ORDER))

            if self.pointers:
                bytes_entries.append(self.pointers[index + 1].to_bytes(INT_SIZE, BYTE_ORDER))
            else:
                bytes_entries.append(MAX_INT.to_bytes(INT_SIZE, BYTE_ORDER))

        if len(self.records) < self.records_per_page:
            for _ in range(self.records_per_page - len(self.records)):
                for _ in range(3):
                    bytes_entries.append(MAX_INT.to_bytes(INT_SIZE, BYTE_ORDER))

        return bytes_entries

    def is_leaf(self):
        for pointer in self.pointers:
            if pointer is not None:
                return False

        return True


from constans import INT_SIZE, BYTE_ORDER, MAX_INT


class IndexRecord:
    def __init__(self, key, data_page_number):
        self.key = key
        self.data_page_number = data_page_number


class IndexPage:
    next_page = 1
    max_size = 0

    def __init__(self, records_per_page, page_number=None):
        self.records_per_page = records_per_page
        IndexPage.max_size = records_per_page * (3 * INT_SIZE) + INT_SIZE + INT_SIZE
        self.current_size = 0

        if page_number is None:
            self.page_number = IndexPage.next_page
            IndexPage.next_page += 1
        else:
            self.page_number = page_number

        self.records = []
        self.pointers = []
        self.parent_page = None
        self.dirty_bit = False

    def add_record(self, position, record):
        self.records.insert(position, record)
        self.dirty_bit = True

    def add_pointer(self, position, page_pointer):
        self.pointers.insert(position, page_pointer)
        self.dirty_bit = True

    def get_key(self, record_number):
        return self.records[record_number].key

    def get_data_page_number(self, record_number):
        return self.records[record_number].data_page_number

    def get_record(self, record_number):
        return self.records[record_number]

    def get_records(self, index_from=None, index_to=None):
        if not index_from and not index_to:
            return self.records
        if index_from and index_to:
            return self.records[index_from:index_to]
        if index_from and not index_to:
            return self.records[index_from:]

        return self.records[:index_to]

    def get_pointer(self, pointer_number):
        return self.pointers[pointer_number]

    def get_pointers(self, index_from=None, index_to=None):
        if not index_from and not index_to:
            return self.pointers
        if index_from and index_to:
            return self.pointers[index_from:index_to]
        if index_from and not index_to:
            return self.pointers[index_from:]

        return self.pointers[:index_to]

    def get_parent(self):
        return self.parent_page

    def set_record(self, record_number, new_record):
        self.records[record_number] = new_record
        self.dirty_bit = True

    def set_records(self, new_records):
        self.records = new_records
        self.dirty_bit = True

    def set_pointers(self, new_pointers):
        self.pointers = new_pointers
        self.dirty_bit = True

    def set_parent(self, new_parent_page):
        self.parent_page = new_parent_page
        self.dirty_bit = True

    def remove_record(self, record):
        self.records.remove(record)
        self.dirty_bit = True

    def remove_pointer(self, pointer):
        self.pointers.remove(pointer)
        self.dirty_bit = True

    def serialize(self):
        bytes_entries = []

        if self.pointers:
            bytes_entries.append(self.pointers[0].to_bytes(INT_SIZE, BYTE_ORDER))
        else:
            bytes_entries.append(MAX_INT.to_bytes(INT_SIZE, BYTE_ORDER))

        for index, record in enumerate(self.records):
            bytes_entries.append(record.key.to_bytes(INT_SIZE, BYTE_ORDER))
            bytes_entries.append(record.data_page_number.to_bytes(INT_SIZE, BYTE_ORDER))

            if self.pointers:  # and len(self.pointers) > index + 1:
                bytes_entries.append(self.pointers[index + 1].to_bytes(INT_SIZE, BYTE_ORDER))
            else:
                bytes_entries.append(MAX_INT.to_bytes(INT_SIZE, BYTE_ORDER))

        if len(self.records) < self.records_per_page:
            for _ in range(self.records_per_page - len(self.records)):
                for _ in range(3):
                    bytes_entries.append(MAX_INT.to_bytes(INT_SIZE, BYTE_ORDER))

        if self.parent_page:
            bytes_entries.append(self.parent_page.to_bytes(INT_SIZE, BYTE_ORDER))
        else:
            bytes_entries.append(MAX_INT.to_bytes(INT_SIZE, BYTE_ORDER))

        return bytes_entries

    def is_leaf(self):
        for pointer in self.pointers:
            if pointer is not None:
                return False

        return True

    def is_dirty(self):
        return self.dirty_bit

    def is_empty(self):
        if len(self.records) == 0 and len(self.pointers) == 0 and self.parent_page is None:
            return True

        return False

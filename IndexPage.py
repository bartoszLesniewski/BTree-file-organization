from constans import INDEX_ENTRIES_PER_PAGE, INDEX_ENTRY_SIZE, INT_SIZE, BYTE_ORDER


class IndexPage:
    def __init__(self):
        self.max_size = INDEX_ENTRIES_PER_PAGE * (2 * INT_SIZE)
        self.current_size = 0
        self.entries = {}
        self.page_number = 0

    def add_entry(self, key, page_number):
        self.current_size += INDEX_ENTRY_SIZE
        self.entries[key] = page_number

    def is_full(self):
        return self.current_size == self.max_size

    def clear_page(self):
        self.current_size = 0
        self.entries = {}
        self.page_number += 1

    def serialize(self):
        bytes_entries = []
        for key, value in self.entries.items():
            bytes_entries.append(key.to_bytes(INT_SIZE, BYTE_ORDER))
            bytes_entries.append(value.to_bytes(INT_SIZE, BYTE_ORDER))

        return bytes_entries



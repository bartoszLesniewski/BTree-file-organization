import os.path

from DataPage import DataPage
from IndexPage import IndexPage
from constans import BYTE_ORDER, INT_SIZE


class FilesHandler:
    def __init__(self, index_filename="index.txt", data_filename="data.txt"):
        self.index_page = IndexPage()
        self.data_file_page = DataPage()
        self.index_filename = index_filename
        self.data_filename = data_filename

    def add_entries(self, record):
        if self.index_page.is_full():
            self.save_index_page()
        if self.data_file_page.is_full():
            self.save_data_file_page()

        self.data_file_page.add_record(record)
        self.index_page.add_entry(record[0], self.data_file_page.page_number)

    def save_index_page(self):
        with open(self.index_filename, "ab") as file:
            serialized_entries = self.index_page.serialize()
            for entry in serialized_entries:
                file.write(entry)

    def save_data_file_page(self):
        pass

    def load_index_page(self):
        with open(self.index_filename, "rb") as file:
            while file.tell() < os.path.getsize(self.index_filename):
                key = int.from_bytes(file.read(INT_SIZE), BYTE_ORDER)
                page = int.from_bytes(file.read(INT_SIZE), BYTE_ORDER)

                print(key)
                print(page)
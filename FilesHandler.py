import os.path

from DataPage import DataPage
from IndexPage import IndexPage
from constans import BYTE_ORDER, INT_SIZE, MAX_INT


class FilesHandler:
    def __init__(self, records_per_page, index_filename="index.txt", data_filename="data.txt"):
        self.data_file_page = DataPage(records_per_page)
        self.index_filename = index_filename
        self.data_filename = data_filename
        self.records_per_page = records_per_page
        open(self.index_filename, "w").close()

    def add_entries(self, record):
        if self.index_page.is_full():
            self.save_index_page()
        if self.data_file_page.is_full():
            self.save_data_file_page()

        self.data_file_page.add_record(record)
        self.index_page.add_entry(record[0], self.data_file_page.page_number)

    def save_index_page(self, index_page):
        with open(self.index_filename, "rb+") as file:
            file.seek((index_page.page_number - 1) * index_page.max_size)
            serialized_entries = index_page.serialize()
            for entry in serialized_entries:
                file.write(entry)

    def save_data_file_page(self):
        pass

    def load_index_page(self, page_number=1):  # == load BTreeNode
        index_page = IndexPage(self.records_per_page, page_number)
        with open(self.index_filename, "rb") as file:
            file.seek((page_number - 1) * index_page.max_size)

            read_bytes = 0
            read_counter = 0
            while read_bytes < index_page.max_size:
                number = int.from_bytes(file.read(INT_SIZE), BYTE_ORDER)
                if read_counter % 3 == 0:
                    if number != MAX_INT:
                        index_page.pointers.append(number)
                    read_bytes += INT_SIZE
                    read_counter += 1
                else:
                    key = number
                    page = int.from_bytes(file.read(INT_SIZE), BYTE_ORDER)
                    if key == MAX_INT or page == MAX_INT:
                        break
                    else:
                        index_page.records.append([key, page])
                        read_bytes += 2 * INT_SIZE
                        read_counter += 2

            # print(index_page.records)
            # print(index_page.pointers)
            return index_page

    def create_new_page(self):
        page = IndexPage(self.records_per_page)
        self.save_index_page(page)
        return page

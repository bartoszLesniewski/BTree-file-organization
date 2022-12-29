import os.path

from DataPage import DataPage, DataRecord
from IndexPage import IndexPage, IndexRecord
from constans import *


class FilesHandler:
    def __init__(self, records_per_page, index_filename="index.txt", data_filename="data.txt"):
        self.index_filename = index_filename
        self.data_filename = data_filename
        self.records_per_page = records_per_page
        self.last_data_page_number = None
        open(self.index_filename, "w").close()
        open(self.data_filename, "w").close()
        self.index_buffer = []
        self.data_buffer = []
        self.index_reads = 0
        self.index_writes = 0
        self.data_reads = 0
        self.data_writes = 0
        self.index_empty_pages = []
        self.data_non_full_pages = []

    def save_index_page(self, index_page):
        if index_page.is_dirty():
            if index_page.is_empty() and index_page.page_number not in self.index_empty_pages:
                self.index_empty_pages.append(index_page.page_number)

            with open(self.index_filename, "rb+") as file:
                file.seek((index_page.page_number - 1) * index_page.max_size)
                serialized_entries = index_page.serialize()
                for entry in serialized_entries:
                    file.write(entry)

            self.index_writes += 1

    def load_index_page(self, page_number=1):  # == load BTreeNode
        index_page = IndexPage(self.records_per_page, page_number)
        with open(self.index_filename, "rb") as file:
            file.seek((page_number - 1) * index_page.max_size)

            read_bytes = 0
            read_counter = 0
            while read_bytes < index_page.max_size - INT_SIZE:
                number = int.from_bytes(file.read(INT_SIZE), BYTE_ORDER)
                if read_counter % 3 == 0:
                    if number != MAX_INT:
                        index_page.pointers.append(number)
                    read_bytes += INT_SIZE
                    read_counter += 1
                else:
                    key = number
                    page = int.from_bytes(file.read(INT_SIZE), BYTE_ORDER)
                    if key != MAX_INT or page != MAX_INT:
                        index_page.records.append(IndexRecord(key, page))

                    read_bytes += 2 * INT_SIZE
                    read_counter += 2

            parent_page = int.from_bytes(file.read(INT_SIZE), BYTE_ORDER)
            if parent_page != MAX_INT:
                index_page.parent_page = parent_page

        self.add_index_page_to_buffer(index_page)
        self.index_reads += 1
        return index_page

    def add_index_page_to_buffer(self, index_page):
        if len(self.index_buffer) < INDEX_BUFFER_SIZE:
            self.index_buffer.insert(0, index_page)
        else:
            lru_page = self.index_buffer.pop()
            self.save_index_page(lru_page)
            del lru_page
            self.index_buffer.insert(0, index_page)

    def reduce_usage(self, index_page):
        self.index_buffer.remove(index_page)
        self.index_buffer.append(index_page)

    def get_index_page(self, page_number):
        for index_page in self.index_buffer:
            if index_page.page_number == page_number:
                self.move_to_the_beginning(self.index_buffer, index_page)
                return index_page

        return self.load_index_page(page_number)

    @staticmethod
    def move_to_the_beginning(buffer, index_page):
        buffer.remove(index_page)
        buffer.insert(0, index_page)

    def create_new_index_page(self):
        if self.index_empty_pages:
            page_number = self.index_empty_pages[0]
            page = self.get_index_page(page_number)
            self.index_empty_pages.remove(page_number)
        else:
            page = IndexPage(self.records_per_page)

        self.add_index_page_to_buffer(page)
        return page

    def flush_index_buffer(self):
        for index_page in self.index_buffer:
            self.save_index_page(index_page)

        self.index_buffer = []

    def save_data_page(self, data_page):
        if data_page.is_dirty():
            with open(self.data_filename, "rb+") as file:
                file.seek((data_page.page_number - 1) * data_page.max_size)
                serialized_entries = data_page.serialize()
                for entry in serialized_entries:
                    file.write(entry)
            self.data_writes += 1

    def load_data_page(self, page_number=1):
        data_page = DataPage(self.records_per_page, page_number)
        with open(self.data_filename, "rb") as file:
            file.seek((page_number - 1) * data_page.max_size)
            read_bytes = 0
            while read_bytes < data_page.max_size:
                key = int.from_bytes(file.read(INT_SIZE), BYTE_ORDER)

                if key == MAX_INT:
                    break
                else:
                    record = DataRecord()
                    record.key = key
                    for _ in range(DATA_RECORD_LENGTH):
                        number = int.from_bytes(file.read(INT_SIZE), BYTE_ORDER)
                        if number != MAX_INT:
                            record.numbers.append(number)

                    read_bytes += DATA_RECORD_SIZE
                    data_page.records.append(record)

        self.add_data_page_to_buffer(data_page)
        self.data_reads += 1
        return data_page

    def get_data_page(self, page_number):
        for data_page in self.data_buffer:
            if data_page.page_number == page_number:
                self.move_to_the_beginning(self.data_buffer, data_page)
                return data_page

        return self.load_data_page(page_number)

    def add_record_to_data_file(self, record):
        if self.last_data_page_number is None or self.get_data_page(self.last_data_page_number).is_full():
            last_data_page = self.create_new_data_page()
            self.last_data_page_number = last_data_page.page_number
        else:
            last_data_page = self.get_data_page(self.last_data_page_number)

        last_data_page.add_record(record)

        return self.last_data_page_number

    def remove_record_from_data_file(self, data_page_number, key):
        data_page = self.get_data_page(data_page_number)
        data_page.remove_record(key)

        if data_page_number != self.last_data_page_number and data_page_number not in self.data_non_full_pages:
            self.data_non_full_pages.append(data_page_number)

    def create_new_data_page(self):
        # If there is any page that is not full, then use it.
        if self.data_non_full_pages:
            page_number = self.data_non_full_pages[0]
            page = self.get_data_page(page_number)
            self.data_non_full_pages.remove(page_number)

        # Otherwise, create the new page.
        else:
            page = DataPage(self.records_per_page)

        self.add_data_page_to_buffer(page)
        return page

    def add_data_page_to_buffer(self, data_page):
        if len(self.data_buffer) < DATA_BUFFER_SIZE:
            self.data_buffer.insert(0, data_page)
        else:
            lru_page = self.data_buffer.pop()
            self.save_data_page(lru_page)
            del lru_page
            self.data_buffer.insert(0, data_page)

    def flush_data_buffer(self):
        for data_page in self.data_buffer:
            self.save_data_page(data_page)

        self.data_buffer = []

    def reset_io_counters(self):
        self.index_reads = 0
        self.index_writes = 0
        self.data_reads = 0
        self.data_writes = 0
        self.index_buffer = []
        self.data_buffer = []

    def print_reads_and_writes(self, btree_height):
        print(f"Index     reads: {self.index_reads}  writes: {self.index_writes}")
        print(f"Data      reads: {self.data_reads}  writes: {self.data_writes}")
        print(f"B-Tree height: {btree_height}")

        # Uncomment for tests.
        # print(f"Index empty pages: {self.index_empty_pages}  Data non full pages: {self.data_non_full_pages}")

    def print_file(self, file_type):
        print(f"Structure of the {file_type} page:")
        if file_type == "index":
            print("p0 key address p1 key address p2 ... parent_pointer")
            filename = self.index_filename
            max_size = IndexPage.max_size
        else:
            print("key [record - 15 numbers]")
            filename = self.data_filename
            max_size = DataPage.max_size

        read_bytes = 0
        page_number = 1
        with open(filename, "rb") as file:
            while read_bytes < os.path.getsize(filename):
                page_bytes = 0
                print(f"PAGE {page_number}: ", end=" ")
                while page_bytes < max_size:
                    num = int.from_bytes(file.read(INT_SIZE), BYTE_ORDER)
                    if num == MAX_INT:
                        print(".", end=" ")
                    else:
                        print(num, end=" ")
                    page_bytes += INT_SIZE
                    read_bytes += INT_SIZE
                print()
                page_number += 1

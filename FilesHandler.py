import os.path

from DataPage import DataPage, DataRecord
from IndexPage import IndexPage, IndexRecord
from constans import BYTE_ORDER, INT_SIZE, MAX_INT, DATA_RECORD_LENGTH, DATA_RECORD_SIZE, INDEX_BUFFER_SIZE


class FilesHandler:
    def __init__(self, records_per_page, index_filename="index.txt", data_filename="data.txt"):
        self.index_filename = index_filename
        self.data_filename = data_filename
        self.records_per_page = records_per_page
        self.last_data_page = None
        open(self.index_filename, "w").close()
        open(self.data_filename, "w").close()
        self.index_buffer = []
        self.data_buffer = []
        self.index_reads = 0
        self.index_writes = 0
        self.count_index = False

    def save_index_page(self, index_page):
        if index_page.is_dirty():
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

            # print(index_page.records)
            # print(index_page.pointers)
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
        # del index_page

    def get_index_page(self, page_number):
        for index_page in self.index_buffer:
            if index_page.page_number == page_number:
                self.move_to_the_beginning(index_page)
                return index_page

        return self.load_index_page(page_number)

    def move_to_the_beginning(self, index_page):
        self.index_buffer.remove(index_page)
        self.index_buffer.insert(0, index_page)

    def create_new_index_page(self):
        page = IndexPage(self.records_per_page)
        # self.save_index_page(page)
        self.add_index_page_to_buffer(page)
        return page

    def flush_index_buffer(self):
        for index_page in self.index_buffer:
            self.save_index_page(index_page)

        self.index_buffer = []

    def add_record_to_data_file(self, record):
        if self.last_data_page is None or self.last_data_page.is_full():
            self.last_data_page = DataPage(self.records_per_page)

        self.last_data_page.records.append(DataRecord(record[0], record[1:]))
        self.save_data_page(self.last_data_page)

        return self.last_data_page.page_number

    def save_data_page(self, data_page):
        with open(self.data_filename, "rb+") as file:
            file.seek((data_page.page_number - 1) * data_page.max_size)
            serialized_entries = data_page.serialize()
            for entry in serialized_entries:
                file.write(entry)

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

        return data_page

    def reset_index_counters(self):
        self.index_reads = 0
        self.index_writes = 0
        self.index_buffer = []

    def print_index_reads_and_writes(self, btree_height):
        print(f"Number of reads: {self.index_reads}")
        print(f"Number of writes: {self.index_writes}")
        print(f"B-Tree height: {btree_height}")

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
                    print(num, end=" ")
                    page_bytes += INT_SIZE
                    read_bytes += INT_SIZE
                print()
                page_number += 1


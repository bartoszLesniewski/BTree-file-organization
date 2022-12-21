from FilesHandler import FilesHandler


class BTree:
    def __init__(self, d=2):
        self.d = d
        self.root_page = None  # root page address
        self.filesHandler = FilesHandler(2 * d)
        self.h = 0

    def insert(self, record):
        self.filesHandler.reset_index_counters()
        data_page_number = self.filesHandler.add_record_to_data_file(record)
        record = [record[0], data_page_number]

        if self.root_page is None:
            root_node = self.create_root()
        else:
            root_node = self.filesHandler.get_index_page(self.root_page)

        record, child_pointer = self.insert_into_node(record, root_node)
        if record and child_pointer:
            self.create_root(record, child_pointer)

        print(f"Number of reads: {self.filesHandler.index_reads}")
        print(f"Number of writes: {self.filesHandler.index_writes}")
        print(f"B-Tree height: {self.h}")

    def create_root(self, record=None, new_child_pointer=None):
        self.h += 1
        previous_root = None
        if self.root_page is not None:
            previous_root = self.filesHandler.get_index_page(self.root_page)

        root_node = self.filesHandler.create_new_index_page()
        self.root_page = root_node.page_number

        if record and new_child_pointer and previous_root:
            root_node.add_record(0, record)
            root_node.add_pointer(0, previous_root.page_number)
            root_node.add_pointer(1, new_child_pointer)
            self.update_parent(root_node)
            self.filesHandler.save_index_page(root_node)  # change it

        return root_node

    def insert_into_node(self, record, node):
        if not node.is_leaf():
            i = self.find_position(node, record)
            if i is None:
                raise Exception("Record already exists!")

            record, new_child_pointer = self.insert_into_node(record,
                                                              self.filesHandler.get_index_page(node.get_pointer(i)))

            # if there was split when inserting record to the child, we have to add a new record to its parent
            # otherwise, we don't have to modify parent
            if new_child_pointer:
                # parent isn't full
                if len(node.records) < 2 * self.d:
                    node.add_record(i, record)
                    node.add_pointer(i + 1, new_child_pointer)
                    self.filesHandler.save_index_page(node)  # change it

                # parent is full
                else:
                    can_compensation = self.try_compensation4(node, record, new_child_pointer)
                    if not can_compensation:
                        record, new_child_pointer = self.split(node, i, record, new_child_pointer)
                        return record, new_child_pointer

        # leaf node
        else:
            i = self.find_position(node, record)
            if len(node.records) < 2 * self.d:
                node.add_record(i, record)
                self.filesHandler.save_index_page(node)  # change it
            else:
                can_compensation = self.try_compensation4(node, record)
                if not can_compensation:
                    record, new_child_pointer = self.split(node, i, record)
                    return record, new_child_pointer

        return None, None

    @staticmethod
    def find_position(node, record):
        i = len(node.records) - 1
        while i >= 0 and record[0] < node.get_key(i):
            i -= 1
        i += 1

        if i < len(node.records) and node.get_record(i) == record:
            return None  # such record exists
        else:
            return i

    def try_compensation4(self, node, record, pointer=None):
        can_compensate = False
        if node.get_parent():
            parent_node = self.filesHandler.get_index_page(node.get_parent())
            index = parent_node.pointers.index(node.page_number)
            if index - 1 >= 0:
                left_neighbour = self.filesHandler.get_index_page(parent_node.get_pointer(index - 1))
                if len(left_neighbour.records) < 2 * self.d:
                    self.compensation(left_neighbour, node, parent_node, index - 1, record, pointer)
                    can_compensate = True
            if not can_compensate and index + 1 < len(parent_node.pointers):
                right_neighbour = self.filesHandler.get_index_page(parent_node.get_pointer(index + 1))
                if len(right_neighbour.records) < 2 * self.d:
                    self.compensation(node, right_neighbour, parent_node, index, record, pointer)
                    can_compensate = True

        return can_compensate

    def compensation(self, left_child, right_child, parent, i, record, pointer=None):
        # pointer parameter is necessary only for non-leaf nodes
        records_distribution_list = left_child.get_records() + [parent.get_record(i)] + right_child.get_records()

        j = len(records_distribution_list) - 1
        while j >= 0 and record[0] < records_distribution_list[j][0]:
            j -= 1
        j += 1
        records_distribution_list.insert(j, record)
        middle = len(records_distribution_list) // 2

        left_child.set_records(records_distribution_list[0:middle])  # left_child.records = records_distribution_list[0:middle]
        right_child.set_records(records_distribution_list[middle + 1:])  # right_child.records = records_distribution_list[middle + 1:]
        parent.set_record(i, records_distribution_list[middle])  # parent.records[i] = records_distribution_list[middle]

        if not left_child.is_leaf():
            pointers_distribution_list = left_child.get_pointers() + right_child.get_pointers()
            pointers_distribution_list.insert(j + 1, pointer)
            left_child.set_pointers(pointers_distribution_list[0:middle + 1])
            right_child.set_pointers(pointers_distribution_list[middle + 1:])
            self.update_parent(left_child)
            self.update_parent(right_child)
            print("AAAAA NOT LEAF ;-;")

        self.filesHandler.save_index_page(left_child)  # change it
        self.filesHandler.save_index_page(right_child)  # change it
        self.filesHandler.save_index_page(parent)  # change it

    def split(self, node, index, record, pointer=None):
        new_node = self.filesHandler.create_new_index_page()
        middle = self.d - 1
        record_for_parent = node.get_record(middle)
        new_node.set_records(node.get_records(middle+1))
        new_node.set_parent(node.get_parent())  # set the same parent by default, it will be changed if it is necessary
        # parent_node.add_pointer(index + 1, new_node.page_number)
        # parent_node.add_record(index, son_node.records[middle])
        node.set_records(node.get_records(0, middle))

        if not node.is_leaf():
            new_node.set_pointers(node.get_pointers(middle + 1))
            node.set_pointers(node.get_pointers(0, middle + 1))  # or 0:middle

        self.insert_after_split(node, new_node, index, record, pointer)
        return record_for_parent, new_node.page_number

    def insert_after_split(self, old_node, new_node, index, record, pointer):
        if index > len(old_node.records) or (index < len(old_node.records) and record[0] > old_node.get_key(index)):
            i = self.find_position(new_node, record)
            new_node.add_record(i, record)
            if not new_node.is_leaf():
                new_node.add_pointer(i + 1, pointer)
        else:
            old_node.add_record(index, record)
            if not old_node.is_leaf():
                old_node.add_pointer(index + 1, pointer)

        self.filesHandler.save_index_page(old_node)  # change it
        self.filesHandler.save_index_page(new_node)  # change it
        self.update_parent(new_node)

    def update_parent(self, parent_node):
        for child_page in parent_node.get_pointers():
            child_node = self.filesHandler.get_index_page(child_page)
            child_node.set_parent(parent_node.page_number)
            self.filesHandler.save_index_page(child_node)

    def print(self, print_records=False):
        if self.root_page is not None:
            root_node = self.filesHandler.get_index_page(self.root_page)
            self.visit_node(root_node, print_records)
            print()

    def visit_node(self, node, print_records=False):
        if not print_records:
            print("( ", end="")

        for i in range(len(node.records)):
            if not node.is_leaf():
                self.visit_node(self.filesHandler.get_index_page(node.get_pointer(i)), print_records)

            if not print_records:
                print(node.get_key(i), end=" ")
            else:
                data_page = self.filesHandler.load_data_page(node.get_data_page_number(i))
                data_page.print_record(node.get_key(i))

        if not node.is_leaf():
            self.visit_node(self.filesHandler.get_index_page(node.get_pointer(len(node.records))), print_records)

        if not print_records:
            print(") ", end="")

    def search(self, key, print_message=False):
        self.filesHandler.reset_index_counters()
        self.search_by_key(key, self.root_page, print_message)

    def search_by_key(self, key, page, print_message=False):
        if page is None or self.root_page is None:
            if print_message:
                print("Key not found!")
            return False

        node = self.filesHandler.get_index_page(page)
        i = 0

        while i < len(node.records) and key > node.get_key(i):
            i += 1

        if i < len(node.records) and key == node.get_key(i):
            if print_message:
                print(f"Key found: {node.get_record(i)}")
            return True
        else:
            if node.is_leaf():
                if print_message:
                    print("Key not found!")
                return False

            return self.search_by_key(key, node.get_pointer(i), print_message)

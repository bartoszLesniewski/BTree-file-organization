from FilesHandler import FilesHandler
from IndexPage import IndexRecord


class BTree:
    def __init__(self, d=2):
        self.d = d
        self.root_page = None  # root page address
        self.h = 0
        self.filesHandler = FilesHandler(2 * d)

    def insert(self, record):
        self.filesHandler.reset_io_counters()
        data_page_number = self.filesHandler.add_record_to_data_file(record)
        record = IndexRecord(record[0], data_page_number)
        self.filesHandler.flush_data_buffer()

        if self.root_page is None:
            root_node = self.create_root()
        else:
            root_node = self.filesHandler.get_index_page(self.root_page)

        try:
            record, child_pointer = self.insert_into_node(record, root_node)
            if record and child_pointer:
                self.create_root(record, child_pointer)

            self.filesHandler.flush_index_buffer()
            self.filesHandler.print_reads_and_writes(self.h)
        except ValueError:
            print("Record already exists!")

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
            self.update_parent([previous_root.page_number, new_child_pointer], root_node.page_number)
            # self.filesHandler.save_index_page(root_node)  # change it

        return root_node

    def insert_into_node(self, record, node):
        i = self.find_position(node, record.key)
        if i < len(node.records) and node.get_key(i) == record.key:
            raise ValueError

        if not node.is_leaf():
            node_page_number = node.page_number  # because it may be removed from memory during further operations
            record, new_child_pointer = self.insert_into_node(record,
                                                              self.filesHandler.get_index_page(node.get_pointer(i)))

            # when returning from recursion, it may turn out that the node has been removed from the buffer,
            # so now we have to restore it
            node = self.filesHandler.get_index_page(node_page_number)

            # if there was a split when inserting record to the child, we have to add a new record to its parent
            # otherwise, we don't have to modify the parent
            if new_child_pointer:
                # parent isn't full
                if len(node.records) < 2 * self.d:
                    node.add_record(i, record)
                    node.add_pointer(i + 1, new_child_pointer)
                    # self.filesHandler.save_index_page(node)  # change it

                # parent is full
                else:
                    can_compensation = self.try_compensation4(node, record, new_child_pointer)
                    if not can_compensation:
                        record, new_child_pointer = self.split(node, i, record, new_child_pointer)
                        return record, new_child_pointer

        # leaf node
        else:
            if len(node.records) < 2 * self.d:
                node.add_record(i, record)
                # self.filesHandler.save_index_page(node)  # change it
            else:
                can_compensation = self.try_compensation4(node, record)
                if not can_compensation:
                    record, new_child_pointer = self.split(node, i, record)
                    return record, new_child_pointer

        return None, None

    @staticmethod
    def find_position(node, key):
        i = len(node.records) - 1
        while i >= 0 and key < node.get_key(i):
            i -= 1

        if 0 <= i < len(node.records) and node.get_key(i) == key:
            return i  # such record exists
        else:
            return i + 1

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
                else:
                    # if compensation with the left neighbour failed, it will no longer be used
                    # so we can remove it from the buffer, so it doesn't take up space
                    self.filesHandler.reduce_usage(left_neighbour)
            if not can_compensate and index + 1 < len(parent_node.pointers):
                right_neighbour = self.filesHandler.get_index_page(parent_node.get_pointer(index + 1))
                if len(right_neighbour.records) < 2 * self.d:
                    self.compensation(node, right_neighbour, parent_node, index, record, pointer)
                    can_compensate = True
                else:
                    # if compensation with the right neighbour failed, it will no longer be used
                    # so we can remove it from the buffer, so it doesn't take up space
                    self.filesHandler.reduce_usage(right_neighbour)

        return can_compensate

    def compensation(self, left_child, right_child, parent, i, record, pointer=None):
        # pointer parameter is necessary only for non-leaf nodes
        records_distribution_list = left_child.get_records() + [parent.get_record(i)] + right_child.get_records()

        j = len(records_distribution_list) - 1
        while j >= 0 and record.key < records_distribution_list[j].key:
            j -= 1
        j += 1
        records_distribution_list.insert(j, record)
        middle = len(records_distribution_list) // 2

        left_child.set_records(records_distribution_list[0:middle])
        right_child.set_records(records_distribution_list[middle + 1:])
        parent.set_record(i, records_distribution_list[middle])

        if not left_child.is_leaf():
            pointers_distribution_list = left_child.get_pointers() + right_child.get_pointers()
            pointers_distribution_list.insert(j + 1, pointer)
            left_child.set_pointers(pointers_distribution_list[0:middle + 1])
            right_child.set_pointers(pointers_distribution_list[middle + 1:])
            self.update_parent(pointers_distribution_list[0:middle + 1], left_child.page_number)
            self.update_parent(pointers_distribution_list[middle + 1:], right_child.page_number)

        # self.filesHandler.save_index_page(left_child)  # change it
        # self.filesHandler.save_index_page(right_child)  # change it
        # self.filesHandler.save_index_page(parent)  # change it

    def split(self, node, index, record, pointer=None):
        new_node = self.filesHandler.create_new_index_page()
        middle = self.d
        node.add_record(index, record)

        record_for_parent = node.get_record(middle)
        new_node.set_records(node.get_records(middle+1))
        new_node.set_parent(node.get_parent())  # set the same parent by default, it will be changed if necessary
        node.set_records(node.get_records(0, middle))

        if not node.is_leaf():
            node.add_pointer(index + 1, pointer)
            pointers = node.get_pointers(middle + 1)
            new_node.set_pointers(pointers)
            node.set_pointers(node.get_pointers(0, middle + 1))
            self.update_parent(pointers, new_node.page_number)

        # self.filesHandler.save_index_page(node)
        # self.filesHandler.save_index_page(new_node)
        return record_for_parent, new_node.page_number

    def update_parent(self, children_pointers, parent_page):
        for child_page in children_pointers:
            child_node = self.filesHandler.get_index_page(child_page)
            child_node.set_parent(parent_page)
            # self.filesHandler.save_index_page(child_node)

    def print(self, print_records=False):
        if self.root_page is not None:
            self.filesHandler.reset_io_counters()
            root_node = self.filesHandler.get_index_page(self.root_page)
            self.visit_node(root_node, print_records)
            print()
            self.filesHandler.flush_index_buffer()
            self.filesHandler.flush_data_buffer()
            self.filesHandler.print_reads_and_writes(self.h)

    def visit_node(self, node, print_records=False):
        if not print_records:
            print("( ", end="")

        for i in range(len(node.records)):
            if not node.is_leaf():
                self.visit_node(self.filesHandler.get_index_page(node.get_pointer(i)), print_records)

            if not print_records:
                print(node.get_key(i), end=" ")
            else:
                data_page = self.filesHandler.get_data_page(node.get_data_page_number(i))
                data_page.print_record(node.get_key(i))

        if not node.is_leaf():
            self.visit_node(self.filesHandler.get_index_page(node.get_pointer(len(node.records))), print_records)

        if not print_records:
            print(") ", end="")

    def search(self, key, print_message=False):
        self.filesHandler.reset_io_counters()
        self.search_by_key(key, self.root_page, print_message)
        self.filesHandler.flush_index_buffer()
        self.filesHandler.flush_data_buffer()
        self.filesHandler.print_reads_and_writes(self.h)

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

    def remove(self, key):
        self.filesHandler.reset_io_counters()

        if self.root_page is None:
            print("B-Tree is empty!")
        else:
            root_node = self.filesHandler.get_index_page(self.root_page)
            data_page_number = self.remove_from_node(key, root_node)
            if data_page_number:
                self.filesHandler.remove_record_from_data_file(data_page_number, key)
                self.filesHandler.flush_data_buffer()
            else:
                print(f"Record can't be deleted because there is no record with key {key}!")

        self.filesHandler.flush_index_buffer()
        self.filesHandler.print_reads_and_writes(self.h)

    def remove_from_node(self, key, node):
        i = self.find_position(node, key)

        if node.is_leaf() and i < len(node.records) and node.get_key(i) == key:
            data_page_number = node.get_data_page_number(i)
            self.remove_from_leaf(node, i)
            return data_page_number
        elif not node.is_leaf() and i < len(node.records) and node.get_key(i) == key:
            data_page_number = node.get_data_page_number(i)
            self.remove_from_internal_node(node, i)
            return data_page_number
        elif node.is_leaf() and (i > len(node.records) or (i < len(node.records) and node.get_key(i) != key)):
            return None  # record with such a key doesn't exist
        else:
            return self.remove_from_node(key, self.filesHandler.get_index_page(node.get_pointer(i)))

    def remove_from_leaf(self, node, i):
        node.remove_record(node.get_record(i))
        self.repair_node_after_removal(self.filesHandler.get_index_page(node.page_number))

    def repair_node_after_removal(self, node):
        if node.page_number != self.root_page and len(node.records) < self.d:
            can_compensate = self.try_compensation_for_remove(node)
            if not can_compensate:
                parent_node = self.filesHandler.get_index_page(node.get_parent())
                i = parent_node.pointers.index(node.page_number)
                if i + 1 < len(parent_node.pointers):
                    right_neighbour = self.filesHandler.get_index_page(parent_node.get_pointer(i + 1))
                    self.merge(node, right_neighbour, parent_node, i)
                elif i - 1 >= 0:
                    left_neighbour = self.filesHandler.get_index_page(parent_node.get_pointer(i - 1))
                    self.merge(left_neighbour, node, parent_node, i - 1)
                else:
                    raise ValueError("This exception should never occur!")
        elif node.page_number == self.root_page:
            if len(node.records) == 0:
                if not node.is_leaf():
                    self.root_page = node.get_pointer(0)
                    node.set_records([])
                    node.set_pointers([])
                    node.set_parent(None)
                    # self.update_parent(node.get_pointers(), None)  # there is only ony pointer to update its parent
                    self.update_parent([self.root_page], None)
                else:
                    self.root_page = None

                self.h -= 1

            # self.filesHandler.save_index_page(node)
        else:
            # self.filesHandler.save_index_page(node)
            pass

    def try_compensation_for_remove(self, node):
        can_compensate = False
        if node.get_parent():
            parent_node = self.filesHandler.get_index_page(node.get_parent())
            index = parent_node.pointers.index(node.page_number)
            if index - 1 >= 0:
                left_neighbour = self.filesHandler.get_index_page(parent_node.get_pointer(index - 1))
                if len(left_neighbour.records) > self.d:
                    self.compensate_with_left_neighbour(node, left_neighbour, parent_node, index - 1)
                    can_compensate = True
                else:
                    # if compensation with the left neighbour failed, it will no longer be used
                    # so we can remove it from the buffer, so it doesn't take up space
                    self.filesHandler.reduce_usage(left_neighbour)

            if not can_compensate and index + 1 < len(parent_node.pointers):
                right_neighbour = self.filesHandler.get_index_page(parent_node.get_pointer(index + 1))
                if len(right_neighbour.records) > self.d:
                    self.compensate_with_right_neighbour(node, right_neighbour, parent_node, index)
                    can_compensate = True
                else:
                    # if compensation with the left neighbour failed, it will no longer be used
                    # so we can remove it from the buffer, so it doesn't take up space
                    self.filesHandler.reduce_usage(right_neighbour)

        return can_compensate

    def compensate_with_left_neighbour(self, node, neighbour, parent, i):
        node.add_record(0, parent.get_record(i))
        record = neighbour.get_record(-1)
        parent.set_record(i, record)
        neighbour.remove_record(record)

        if not node.is_leaf():
            pointer = neighbour.get_pointer(-1)
            node.add_pointer(0, pointer)
            neighbour.remove_pointer(pointer)
            self.update_parent([pointer], node.page_number)

        # self.filesHandler.save_index_page(node)
        # self.filesHandler.save_index_page(neighbour)
        # self.filesHandler.save_index_page(parent)

    def compensate_with_right_neighbour(self, node, neighbour, parent, i):
        node.add_record(len(node.records), parent.get_record(i))
        record = neighbour.get_record(0)
        parent.set_record(i, record)
        neighbour.remove_record(record)

        if not node.is_leaf():
            pointer = neighbour.get_pointer(0)
            node.add_pointer(len(node.pointers), pointer)
            neighbour.remove_pointer(pointer)
            self.update_parent([pointer], node.page_number)

        # self.filesHandler.save_index_page(node)
        # self.filesHandler.save_index_page(neighbour)
        # self.filesHandler.save_index_page(parent)

    def remove_from_internal_node(self, node, i):
        node_page_number = node.page_number  # because it may be removed from memory during further operations
        left_child = self.filesHandler.get_index_page(node.get_pointer(i))
        if len(left_child.records) > self.d:
            leaf_node, predecessor = self.find_predecessor(left_child)
            node = self.filesHandler.get_index_page(node_page_number)
            node.set_record(i, predecessor)
            # self.filesHandler.save_index_page(node)
            self.remove_from_node(predecessor.key, leaf_node)
        else:
            self.filesHandler.reduce_usage(left_child)
            right_child = self.filesHandler.get_index_page(node.get_pointer(i + 1))
            if len(right_child.records) > self.d:
                leaf_node, successor = self.find_successor(right_child)
                node = self.filesHandler.get_index_page(node_page_number)
                node.set_record(i, successor)
                # self.filesHandler.save_index_page(node)
                self.remove_from_node(successor.key, leaf_node)
            else:
                self.filesHandler.reduce_usage(right_child)
                left_child = self.filesHandler.get_index_page(node.get_pointer(i))
                leaf_node, predecessor = self.find_predecessor(left_child)
                node = self.filesHandler.get_index_page(node_page_number)
                node.set_record(i, predecessor)
                # self.filesHandler.save_index_page(node)
                self.remove_from_node(predecessor.key, leaf_node)

    def find_predecessor(self, node):
        predecessor = node.get_record(-1)
        while not node.is_leaf():
            node = self.filesHandler.get_index_page(node.get_pointer(-1))
            predecessor = node.get_record(-1)

        return node, predecessor

    def find_successor(self, node):
        successor = node.get_record(0)
        while not node.is_leaf():
            node = self.filesHandler.get_index_page(node.get_pointer(0))
            successor = node.get_record(0)

        return node, successor

    def merge(self, node, neighbour, parent, i):
        # because they may be removed from memory during further operations
        neighbour_page_number = neighbour.page_number
        parent_page_number = parent.page_number

        record_from_parent = parent.get_record(i)
        node.set_records(node.get_records() + [record_from_parent] + neighbour.get_records())
        if not node.is_leaf():
            node.set_pointers(node.get_pointers() + neighbour.get_pointers())

        parent.remove_record(record_from_parent)
        parent.remove_pointer(neighbour.page_number)
        self.update_parent(neighbour.get_pointers(), node.page_number)

        neighbour = self.filesHandler.get_index_page(neighbour_page_number)
        neighbour.set_records([])
        neighbour.set_pointers([])
        neighbour.set_parent(None)

        # self.filesHandler.save_index_page(node)
        # self.filesHandler.save_index_page(neighbour)
        # self.filesHandler.save_index_page(parent)
        self.repair_node_after_removal(self.filesHandler.get_index_page(parent_page_number))

    def update(self, old_key, new_key, record):
        self.remove(old_key)
        self.insert([new_key] + record)

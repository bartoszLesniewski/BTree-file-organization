from FilesHandler import FilesHandler


class BTree:
    def __init__(self, d=2):
        self.d = d
        self.root_page = None  # root page address
        self.filesHandler = FilesHandler(2*d)

    def insert(self, record):
        # self.filesHandler.add_entries(record)
        if self.search(record[0], self.root_page):
            print(f"KEY {record[0]} ALREADY EXISTS!")
            return

        if self.root_page is None:
            root_node = self.filesHandler.create_new_page()
            self.root_page = root_node.page_number
        else:
            root_node = self.filesHandler.load_index_page(self.root_page)

        if len(root_node.records) == 2 * self.d:
            new_root_node = self.filesHandler.create_new_page()
            new_root_node.pointers.append(self.root_page)
            self.split_child(new_root_node, 0, root_node)
            self.root_page = new_root_node.page_number
            self.insert_non_full(new_root_node, record)
        else:
            self.insert_non_full(root_node, record)

    def split_child(self, parent_node, index, son_node):
        # son - overflown node
        # index - position of the son node in parent node

        new_node = self.filesHandler.create_new_page()
        middle = self.d - 1
        new_node.records = son_node.records[middle + 1:]
        parent_node.pointers.insert(index + 1, new_node.page_number)
        parent_node.records.insert(index, son_node.records[middle])
        son_node.records = son_node.records[0:middle]

        if not son_node.is_leaf():
            new_node.pointers = son_node.pointers[middle + 1:]
            son_node.pointers = son_node.pointers[0: middle]

        self.filesHandler.save_index_page(son_node)
        self.filesHandler.save_index_page(parent_node)
        self.filesHandler.save_index_page(new_node)

    def insert_non_full(self, node, record):
        i = len(node.records) - 1
        while i >= 0 and record[0] < node.records[i][0]:
            i -= 1
        i += 1

        if node.is_leaf():
            node.records.insert(i, record)
            self.filesHandler.save_index_page(node)
        else:
            child = self.filesHandler.load_index_page(node.pointers[i])
            if len(child.records) == 2 * self.d:
                # fix it - save information about number of keys on the page
                can_compensate = False
                if i - 1 >= 0:
                    left_neighbour = self.filesHandler.load_index_page(node.pointers[i - 1])
                    if len(left_neighbour.records) < 2 * self.d:
                        self.compensation(left_neighbour, child, node, i - 1)
                        can_compensate = True
                if not can_compensate and i + 1 < len(node.pointers) - 1:
                    right_neighbour = self.filesHandler.load_index_page(node.pointers[i + 1])
                    if len(right_neighbour.records) < 2 * self.d:
                        self.compensation(child, right_neighbour, node, i)
                        can_compensate = True
                if not can_compensate:
                    self.split_child(node, i, child)

                    if record[0] > node.records[i][0]:
                        child = self.filesHandler.load_index_page(node.pointers[i + 1])

            self.insert_non_full(child, record)

    def compensation(self, left_neighbour, right_neighbour, ancestor, i):
        records_distribution_list = left_neighbour.records + [ancestor.records[i]] + right_neighbour.records
        pointers_distribution_list = left_neighbour.pointers + right_neighbour.pointers
        partition = len(records_distribution_list) // 2

        left_neighbour.records = records_distribution_list[0:partition]
        left_neighbour.pointers = pointers_distribution_list[0:partition + 1]
        right_neighbour.records = records_distribution_list[partition + 1:]
        right_neighbour.pointers = pointers_distribution_list[partition + 1:]
        ancestor.records[i] = records_distribution_list[partition]

        self.filesHandler.save_index_page(left_neighbour)
        self.filesHandler.save_index_page(right_neighbour)
        self.filesHandler.save_index_page(ancestor)

    def print(self):
        if self.root_page is not None:
            root_node = self.filesHandler.load_index_page(self.root_page)
            self.visit_node(root_node)
            print()

    def visit_node(self, node):
        print("( ", end="")

        for i in range(len(node.records)):
            if not node.is_leaf():
                self.visit_node(self.filesHandler.load_index_page(node.pointers[i]))
            print(node.records[i][0], end=" ")

        if not node.is_leaf():
            self.visit_node(self.filesHandler.load_index_page(node.pointers[len(node.records)]))

        print(") ", end="")

    def search(self, key, page, print_message=False):
        if page is None or self.root_page is None:
            if print_message:
                print("KEY NOT FOUND")
            return False

        node = self.filesHandler.load_index_page(page)
        i = 0

        while i < len(node.records) and key > node.records[i][0]:
            i += 1

        if i < len(node.records) and key == node.records[i][0]:
            if print_message:
                print(f"KEY FOUND: {node.records[i]}")
            return True
        else:
            if node.is_leaf():
                if print_message:
                    print("KEY NOT FOUND")
                return False

            return self.search(key, node.pointers[i])

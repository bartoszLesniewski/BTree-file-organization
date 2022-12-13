from FilesHandler import FilesHandler
from IndexPage import IndexPage


class BTree:
    def __init__(self, d=2):
        self.d = d
        self.root_page = None  # root page address
        self.filesHandler = FilesHandler(2*d)

    def insert(self, record):
        self.filesHandler.add_entries(record)
        root_node = None

        if self.root_page is None:
            root_node = IndexPage(2 * self.d)
            self.root_page = root_node.page_number
        else:
            root_node = self.filesHandler.load_index_page(self.root_page)

        if len(self.root_page.reocrds) == 2 * self.d:
            new_root_node = IndexPage(2 * self.d)
            new_root_node.pointers.append(self.root_page)
            self.split_child(new_root_node, 0, root_node)
            self.insert_non_full(root_node, record)

    def split_child(self, parent_node, index, son):
        new_node = IndexPage(2 * self.d)
        middle = self.d - 1
        new_node.records = son.records[middle + 1:]
        son.records = son.record[0:middle]

        if not son.is_leaf():
            new_node.pointers = son.pointers[middle + 1:]
            son.pointers = son.pointers[0: middle]

        parent_node.pointers.insert(index + 1, new_node.page_number)
        parent_node.records.insert(index, son.records[middle])







from FilesHandler import FilesHandler


class BTree:
    def __init__(self, d=2):
        self.d = d
        self.root = None
        self.filesHandler = FilesHandler()

    def insert(self, record):
        self.filesHandler.add_entries(record)


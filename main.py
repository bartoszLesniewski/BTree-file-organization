from BTree import BTree
from manager import Manager


def main():
    manager = Manager()
    manager.run()


def io_test():
    btree = BTree()
    page1 = btree.filesHandler.create_new_page()
    page1.records.append([1, 1])
    page1.records.append([2, 2])
    page1.records.append([3, 3])

    page2 = btree.filesHandler.create_new_page()
    page2.records.append([4, 4])
    page2.records.append([5, 5])
    page2.records.append([6, 6])

    page3 = btree.filesHandler.create_new_page()
    page3.records.append([7, 7])
    page3.records.append([8, 8])
    page3.records.append([9, 9])

    btree.filesHandler.save_index_page(page1)
    btree.filesHandler.save_index_page(page2)
    btree.filesHandler.save_index_page(page3)

    page1_read = btree.filesHandler.load_index_page(1)
    page2_read = btree.filesHandler.load_index_page(2)
    page3_read = btree.filesHandler.load_index_page(3)

    page2_load = btree.filesHandler.load_index_page(2)
    page2_load.records[1] = [10, 10]
    btree.filesHandler.save_index_page(page2_load)

    page1_read = btree.filesHandler.load_index_page(1)
    page2_read = btree.filesHandler.load_index_page(2)
    page3_read = btree.filesHandler.load_index_page(3)


def test_data():
    btree = BTree()
    page1 = btree.filesHandler.load_data_page(1)
    page2 = btree.filesHandler.load_data_page(2)
    pass
    page3 = btree.filesHandler.load_data_page(123)
    pass


if __name__ == "__main__":
    main()
    # io_test()
    # test_data()

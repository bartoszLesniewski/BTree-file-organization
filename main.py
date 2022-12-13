from BTree import BTree


def main():
    btree = BTree()
    btree.filesHandler.load_index_page()
    while True:
        command = input("Please enter the command: ")
        command = command.split()
        records = command[1:]
        records = [int(record) for record in records]

        if command[0] == "INSERT":
            btree.insert(records)
        elif command[0] == "READ":
            btree.filesHandler.load_index_page()
        elif command[0] == "EXIT":
            break


if __name__ == "__main__":
    main()

from enum import Enum
from BTree import BTree
from utils import generate_random_record


class Mode(Enum):
    INTERACTIVE = 1
    FILE = 2
    EXIT = 3


class Command(Enum):
    INSERT = "INSERT"
    PRINT = "PRINT"
    PRINT_RECORDS = "PRINT-RECORDS"
    SEARCH = "SEARCH"
    REMOVE = "REMOVE"
    UPDATE = "UPDATE"
    PRINT_INDEX_FILE = "PRINT-INDEX-FILE"
    PRINT_DATA_FILE = "PRINT-DATA-FILE"
    EXIT = "EXIT"


class Manager:
    def __init__(self):
        self.program_mode = None
        self.btree = None

    def run(self):
        self.display_main_menu()
        self.choose_menu_option()

        if self.program_mode == Mode.EXIT:
            return

        self.initialize_btree()

        if self.program_mode == Mode.INTERACTIVE:
            self.handle_commands()
        else:
            self.read_commands_from_file()

    @staticmethod
    def display_main_menu():
        print("======= DATABASE STRUCTURES - PROJECT 2: B-TREE =======")
        print("================ Bartosz Lesniewski,  184783 ================")
        print("1. Interactive mode.")
        print("2. Read commands from file.")
        print("3. Exit.")

    def choose_menu_option(self):
        option = int(input("Please select the program mode by entering the option number: "))
        if option == Mode.INTERACTIVE.value:
            self.program_mode = Mode.INTERACTIVE
        elif option == Mode.FILE.value:
            self.program_mode = Mode.FILE
        elif option == Mode.EXIT.value:
            self.program_mode = Mode.EXIT
        else:
            print("Invalid option. Program will close.")
            self.program_mode = Mode.EXIT

    def initialize_btree(self):
        try:
            d = int(input("Please enter the order of B-Tree (d parameter): "))
            self.btree = BTree(d)
        except ValueError:
            print("Invalid value entered. Program will close.")
            exit(-1)

    def handle_commands(self):
        self.display_available_commands()
        run = True
        while run:
            command = input("Please enter the command: ")
            action, value = self.parse_command(command)
            run = self.run_command(action, value)

    def run_command(self, action, value):
        if action == Command.INSERT.value:
            if isinstance(value, list):
                self.btree.insert(value)
            else:
                self.btree.insert([value] + generate_random_record())
        elif action == Command.PRINT.value:
            self.btree.print()
        elif action == "PRINT-KEYS":
            self.btree.print()
        elif action == "PRINT-RECORDS":
            self.btree.print(print_records=True)
        elif action == Command.PRINT_INDEX_FILE.value:
            self.btree.filesHandler.print_file("index")
        elif action == Command.PRINT_DATA_FILE.value:
            self.btree.filesHandler.print_file("data")
        elif action == Command.SEARCH.value:
            self.btree.search(value, True)
        elif action == Command.REMOVE.value:
            self.btree.remove(value)
        elif action == Command.UPDATE.value:
            if isinstance(value, list) and len(value) > 1:
                if len(value) >= 3:
                    self.btree.update(value[0], value[1], value[2:])
                else:
                    self.btree.update(value[0], value[1], generate_random_record())
            else:
                print("Insufficient number of parameters for INSERT operation!")

        elif action == Command.EXIT.value:
            return False
        else:
            print("Invalid command.")

        return True

    @staticmethod
    def display_available_commands():
        print("Available commands:")
        print("INSERT key [numbers]")
        print("PRINT")
        print("PRINT-RECORDS")
        print("SEARCH key")
        print("REMOVE key")
        print("UPDATE old_key new_key [numbers]")
        print("PRINT-INDEX-FILE")
        print("PRINT-DATA-FILE")
        print("EXIT")

    @staticmethod
    def parse_command(command):
        if command:
            command = command.split()

            if len(command) > 0:
                action = command[0].upper()

                if len(command) > 1:
                    value = command[1:]
                    if len(value) == 1:
                        value = int(value[0])
                    else:
                        value = [int(v) for v in value]
                else:
                    value = None
            else:
                action, value = None, None

            return action, value

        return None, None

    def read_commands_from_file(self):
        test_file_name = input("Please enter a test file name: ")
        with open(test_file_name) as file:
            lines = file.readlines()
            for line in lines:
                if line.strip():
                    print(line.strip())
                    action, value = self.parse_command(line.strip())
                    self.run_command(action, value)

    def TEST_read_commands_from_file(self):
        with open("tests.txt", "r") as file:
            for line in file.readlines():
                if line == "PRINT\n":
                    print("PRINT")
                    self.btree.print()
                elif line == "PRINT-RECORDS\n":
                    self.btree.print(print_records=True)
                elif line == "PRINT-INDEX-FILE\n":
                    self.btree.filesHandler.print_file("index")
                elif line == "PRINT-DATA-FILE\n":
                    self.btree.filesHandler.print_file("data")
                else:
                    line = line.split()
                    val = line[1]
                    if line[0] == "INSERT":
                        val = [int(val), 1]
                        print(f"INSERT {val}")
                        self.btree.insert(val)
                    else:
                        if int(val) == 74:
                            pass
                        print(f"REMOVE {val}")
                        self.btree.remove(int(val))

from enum import Enum
from BTree import BTree


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
        while True:
            command = input("Please enter the command: ")
            action, value = self.parse_command(command)

            if action == Command.INSERT.value:
                if isinstance(value, list):
                    self.btree.insert(value)
                else:
                    print("Insufficient number of parameters for INSERT operation!")
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
            elif action == Command.EXIT.value:
                break
            else:
                print("Invalid command.")

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
        with open("tests.txt", "r") as file:
            for line in file.readlines():
                if line == "PRINT\n":
                    self.btree.print()
                elif line == "PRINT-RECORDS\n":
                    self.btree.print(print_records=True)
                else:
                    line = line.split()
                    val = line[1]
                    if line[0] == "INSERT":
                        val = [int(val), 1]
                        self.btree.insert(val)
                    else:
                        if int(val) == 67:
                            pass
                        self.btree.remove(int(val))

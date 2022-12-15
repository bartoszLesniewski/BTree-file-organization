
class Record:
    def __init__(self, key=None, numbers=None):
        self.key = key
        if numbers is None:
            self.numbers = []
        else:
            self.numbers = numbers

    def print(self):
        print(self.key, end=": ")
        print(self.numbers, end="")


import random
import matplotlib.pyplot as plt

from manager import Manager

test_numbers = [100, 500, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000]


def generate_test_files():
    for number in test_numbers:
        with open(f"tests/test_{number}.txt", "w") as file:
            keys_insert = [i for i in range(1, number + 1)]
            keys_remove = list(keys_insert)
            for _ in range(number):
                num = random.choice(keys_insert)
                keys_insert.remove(num)
                file.write(f"INSERT {num}\n")
            for _ in range(number):
                num = random.choice(keys_remove)
                keys_remove.remove(num)
                file.write(f"REMOVE {num}\n")


def generate_search_test_file():
    with open(f"tests/search_{5000}.txt", "w") as file:
        keys_insert = [i for i in range(1, 5000 + 1)]
        keys_search = list(keys_insert)
        for _ in range(5000):
            num = random.choice(keys_insert)
            keys_insert.remove(num)
            file.write(f"INSERT {num}\n")

        for i in range(1000):
            num = random.choice(keys_search)
            keys_search.remove(num)
            file.write(f"SEARCH {num}\n")


def run_experiment():
    manager = Manager()
    # manager.run_for_experiment(100)
    # manager.run_for_experiment(500)
    # manager.run_for_experiment(1000)
    # manager.run_for_experiment(2000)
    # manager.run_for_experiment(3000)
    # manager.run_for_experiment(4000)
    # manager.run_for_experiment(5000)
    # manager.run_for_experiment(6000)
    # manager.run_for_experiment(7000)
    # manager.run_for_experiment(8000)
    # manager.run_for_experiment(9000)
    # manager.run_for_experiment(10000)

    # search test
    # manager.run_for_experiment(5000)

    # index buffer test
    manager.run_for_experiment(2000)


def generate_plots():
    generate_insert_rw_plot()
    generate_remove_rw_plot()
    generate_files_size_plot()


def generate_insert_rw_plot():
    numbers_of_records = list(test_numbers)  # x
    insert_avg_reads = [4.64, 7.83, 9.08, 9.95, 10.64, 10.93, 11.18, 11.41, 11.55, 11.60, 12.25, 12.14]
    insert_avg_writes = [2.86, 3.72, 3.90, 3.89, 3.94, 3.92, 3.93, 3.97, 3.933, 3.91, 3.92, 4.03]
    plt.figure(figsize=(10, 5))
    plt.plot(numbers_of_records, insert_avg_reads, label="Odczyty")
    plt.plot(numbers_of_records, insert_avg_writes, label="Zapisy")

    plt.title("Średnia liczba odczytów i zapisów w zależności od liczby rekordów dla operacji dodawania",
              fontweight="bold")
    plt.xlabel("Liczba rekordów")
    plt.ylabel("Średnia liczba odczytów / zapisów")
    plt.legend()
    plt.grid()
    plt.savefig('plots/insert_rw.png', dpi=600)
    plt.show()


def generate_remove_rw_plot():
    numbers_of_records = list(test_numbers)  # x
    remove_avg_reads = [4.17, 5.74, 6.15, 7.01, 7.19, 7.24, 7.32, 7.31, 7.87, 7.43, 8.09, 8.16]
    remove_avg_writes = [2.38, 2.55, 2.60, 2.68, 2.68, 2.67, 2.66, 2.65, 2.67, 2.67, 2.68, 2.68]
    plt.figure(figsize=(10, 5))
    plt.plot(numbers_of_records, remove_avg_reads, label="Odczyty")
    plt.plot(numbers_of_records, remove_avg_writes, label="Zapisy")

    plt.title("Średnia liczba odczytów i zapisów w zależności od liczby rekordów dla operacji usuwania",
              fontweight="bold")
    plt.xlabel("Liczba rekordów")
    plt.ylabel("Średnia liczba odczytów / zapisów")
    plt.legend()
    plt.grid()
    plt.savefig('plots/remove_rw.png', dpi=600)
    plt.show()


def generate_files_size_plot():
    numbers_of_records = list(test_numbers)  # x
    index_file_size = [1.86, 8.42, 16.84, 33.74, 51.13, 67.59, 84.44, 101.01, 119.33, 136.17, 151.10, 168.05]
    data_file_size = [6.25, 31.25, 62.50, 125.00, 187.50, 259.00, 312.50, 375.00, 437.50, 500.00, 562.50, 625.00]
    plt.figure(figsize=(10, 5))
    plt.plot(numbers_of_records, index_file_size, label="Plik indeksowy")
    plt.plot(numbers_of_records, data_file_size, label="Plik z danymi")

    plt.title("Wielkości plików w zależności od liczby rekordów", fontweight="bold")
    plt.xlabel("Liczba rekordów")
    plt.ylabel("Wielkość pliku indeksowego / pliku z danymi [kB]")
    plt.legend()
    plt.grid()
    plt.savefig('plots/files_size.png', dpi=600)
    plt.show()


def main():
    # generate_test_files()
    # generate_plots()
    # generate_search_test_file()

    run_experiment()


main()

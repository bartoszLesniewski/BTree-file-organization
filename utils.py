import random
from constans import MAX_RECORD_LENGTH, MIN_NUMBER, MAX_NUMBER


def generate_random_record():
    record = []
    record_length = random.randint(1, MAX_RECORD_LENGTH)
    while record_length:
        record.append(random.randint(MIN_NUMBER, MAX_NUMBER))
        record_length -= 1

    return record

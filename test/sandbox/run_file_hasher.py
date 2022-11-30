import os

from rust_utils import rust_utils

from utils.datagen import FakeDataGen
from utils.file_handler import FileHandler
import timeit
import shutil

# file_handler = FileHandler()
# file_handler.scan_filesystem("/home/balazs/Downloads/test_data/archive")

filenames = []
for root, dirs, files in os.walk("/home/balazs/Downloads/test_data"):
    for filename in files:
        if filename.endswith(".csv"):
            filenames.append(root + "/" + filename)

# crc32 = rust_utils.crc32_hash([filenames[0]])
# adler32 = rust_utils.adler32_hash([filenames[0]])
# fletcher16 = rust_utils.fletcher16_hash([filenames[0]])

print("end")
duplication = []
# roughly 70gb
for _ in range(0, 40):
    duplication += filenames


def call_python_hash():
    result = FileHandler.get_python_file_hash(duplication)
    print(len(result))
    print(result[-1])

def call_crc32_single_no_batch():
    results = []
    for dup in duplication:
        result = rust_utils.crc32_hash([dup])[0]
        results.append(result)
    print(len(results))
    print(results[-1])
def call_crc32_single():
    result = rust_utils.crc32_hash(duplication)
    print(len(result))
    print(result[-1])


def call_crc32_multi_2():
    result = rust_utils.multithreaded_crc32_hash(duplication, 2)
    print(len(result))
    print(result[-1])


def call_crc32_multi_4():
    result = rust_utils.multithreaded_crc32_hash(duplication, 4)
    print(len(result))
    print(result[-1])


def call_crc32_multi_8():
    result = rust_utils.multithreaded_crc32_hash(duplication, 8)
    print(len(result))
    print(result[-1])


def call_crc32_multi_16():
    result = rust_utils.multithreaded_crc32_hash(duplication, 16)
    print(len(result))
    print(result[-1])


def call_crc32_multi_2_single_io():
    result = rust_utils.multithreaded_crc32_hash_with_single_io_thread(duplication, 2)
    print(len(result))
    print(result[-1])


def call_crc32_multi_4_single_io():
    result = rust_utils.multithreaded_crc32_hash_with_single_io_thread(duplication, 4)
    print(len(result))
    print(result[-1])


def call_crc32_multi_8_single_io():
    result = rust_utils.multithreaded_crc32_hash_with_single_io_thread(duplication, 8)
    print(len(result))
    print(result[-1])


def call_crc32_multi_16_single_io():
    result = rust_utils.multithreaded_crc32_hash_with_single_io_thread(duplication, 16)
    print(len(result))
    print(result[-1])


def call_crc32_multi_2_uring_io():
    result = rust_utils.multithreaded_crc32_hash_with_uring(duplication, 2)
    print(len(result))
    print(result[-1])


def call_crc32_multi_4_uring_io():
    result = rust_utils.multithreaded_crc32_hash_with_uring(duplication, 4)
    print(len(result))
    print(result[-1])


def call_crc32_multi_8_uring_io():
    result = rust_utils.multithreaded_crc32_hash_with_uring(duplication, 8)
    print(len(result))
    print(result[-1])


def call_crc32_multi_16_uring_io():
    result = rust_utils.multithreaded_crc32_hash_with_uring(duplication, 16)
    print(len(result))
    print(result[-1])


def call_python_crc32():
    result = FileHandler.get_python_file_hash(duplication)
    print(len(result))
    print(result[-1])


def call_adler32_single():
    result = rust_utils.adler32_hash(duplication)
    print(len(result))
    print(result[-1])


def call_adler32_multi():
    result = rust_utils.multithreaded_adler32_hash(duplication, 4)
    print(len(result))
    print(result[-1])


def call_fletcher16_single():
    result = rust_utils.fletcher16_hash(duplication)
    print(len(result))
    print(result[-1])




print("crc32_multi_2_batch_uring_io: " + str(timeit.Timer(call_crc32_multi_2_uring_io).timeit(number=1)))
print("-----------------------------")
print("crc32_multi_4_batch_uring_io: " + str(timeit.Timer(call_crc32_multi_4_uring_io).timeit(number=1)))
print("-----------------------------")
print("crc32_multi_8_batch_uring_io: " + str(timeit.Timer(call_crc32_multi_8_uring_io).timeit(number=1)))
print("-----------------------------")
print("crc32_multi_16_batch_uring_io: " + str(timeit.Timer(call_crc32_multi_16_uring_io).timeit(number=1)))
print("-----------------------------")
print("crc32_single_batch: " + str(timeit.Timer(call_crc32_single).timeit(number=1)))
print("-----------------------------")
print("call_crc32_multi_2_batch: " + str(timeit.Timer(call_crc32_multi_2).timeit(number=1)))
print("-----------------------------")
print("call_crc32_multi_4_batch: " + str(timeit.Timer(call_crc32_multi_4).timeit(number=1)))
print("-----------------------------")
print("call_crc32_multi_8_batch: " + str(timeit.Timer(call_crc32_multi_8).timeit(number=1)))
print("-----------------------------")
print("call_crc32_multi_16_batch: " + str(timeit.Timer(call_crc32_multi_16).timeit(number=1)))
print("-----------------------------")



# print("crc32_multi_2_batch_single_io: " + str(timeit.Timer(call_crc32_multi_2_single_io).timeit(number=1)))
# print("-----------------------------")
# print("crc32_multi_4_batch_single_io: " + str(timeit.Timer(call_crc32_multi_4_single_io).timeit(number=1)))
# print("-----------------------------")
# print("crc32_multi_8_batch_single_io: " + str(timeit.Timer(call_crc32_multi_8_single_io).timeit(number=1)))
# print("-----------------------------")
# print("crc32_multi_16_batch_single_io: " + str(timeit.Timer(call_crc32_multi_16_single_io).timeit(number=1)))
# print("-----------------------------")

# print("-----------------------------")
# print("crc32_single_no_batch: " + str(timeit.Timer(call_crc32_single_no_batch).timeit(number=1)))
# print("-----------------------------")
# print("crc32_single_batch: " + str(timeit.Timer(call_crc32_single).timeit(number=1)))
# print("-----------------------------")
# print("call_crc32_multi_2_batch: " + str(timeit.Timer(call_crc32_multi_2).timeit(number=1)))
# print("-----------------------------")
# print("call_crc32_multi_4_batch: " + str(timeit.Timer(call_crc32_multi_4).timeit(number=1)))
# print("-----------------------------")
# print("call_crc32_multi_8_batch: " + str(timeit.Timer(call_crc32_multi_8).timeit(number=1)))
# print("-----------------------------")
# print("call_crc32_multi_16_batch: " + str(timeit.Timer(call_crc32_multi_16).timeit(number=1)))
# print("-----------------------------")
# print("adler32_multi_batch: " + str(timeit.Timer(call_adler32_multi).timeit(number=1)))
# print("-----------------------------")
# print("adler32_single_batch: " + str(timeit.Timer(call_adler32_single).timeit(number=1)))
# print("-----------------------------")
# print("fletcher16_single_batch: " + str(timeit.Timer(call_fletcher16_single).timeit(number=1)))

# print("crc32_single_batch: " + str(timeit.Timer(call_crc32_single).timeit(number=1)))
# print("-----------------------------")
# print("crc32_multi_batch: " + str(timeit.Timer(call_crc32_multi).timeit(number=1)))
# print("-----------------------------")
# print("-----------------------------")

import os

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

duplication = []
# roughly 70gb
for _ in range(1, 20):
    duplication += filenames


def call_python_hash():
    result = FileHandler.get_python_file_hash(duplication)
    print(len(result))
    print(result[-1])

def call_rust_hash():
    result = FileHandler.get_rust_file_hash(duplication)
    print(len(result))
    print(result[-1])

print("rust: " + str(timeit.Timer(call_rust_hash).timeit(number=1)))
print("-----------------------------")
print("python: " + str(timeit.Timer(call_python_hash).timeit(number=1)))

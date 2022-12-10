import os
from rust_utils import rust_utils

filenames = []
for root, dirs, files in os.walk("/home/balazs/Downloads/test_data"):
    for filename in files:
        if filename.endswith(".csv"):
            filenames.append(root + "/" + filename)

rust_utils.create_daemon(filenames)


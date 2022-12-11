import os
from rust_utils import rust_utils

filenames = []
for root, dirs, files in os.walk("/home/balazs/Documents/random_data"):
    for filename in files:
        if filename.endswith(".data"):
            filenames.append(root + "/" + filename)

rust_utils.create_daemon(filenames)


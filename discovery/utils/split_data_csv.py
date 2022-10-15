import os
import pandas as pd
 
"""
Defines the number of the rows in the csv file
"""
class FileSettings(object):
    def __init__(self, file_name, row_size=10):
        self.file_name = file_name
        self.row_size = row_size


class FileSplitter(object):
 
    """
    Read the csv file and divide the data into chunks
    """
    def __init__(self, file_settings):
        self.file_settings = file_settings
 
        self.df = pd.read_csv(self.file_settings.file_name,
                              chunksize=self.file_settings.row_size)
 
    def run(self, directory="mock_filesystem"): 
        counter = 0
        while True:
            try:
                file_name = "{}_split_{}.csv".format(
                    self.file_settings.file_name.split(".")[0], counter
                )
                df = next(self.df).to_csv(file_name)
                counter = counter + 1
            except StopIteration:
                break
        return True
 

def main():
    helper =  FileSplitter(FileSettings(
        file_name='mock_filesystem/new_test_0.csv',
        row_size=10
    ))
    helper.run()
 
main()
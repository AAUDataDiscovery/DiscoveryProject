import pandas as pd
import dask.dataframe as dd
import time

class SplitData:
     def  read_csv(self, file_path, npartitions):
          
          ddf = dd.read_csv(
               file_path, 
               sep=',')
          
          shuffle=ddf.sample(frac=1)
          self.partition = shuffle.repartition(npartitions)

          """
          Read, shuffle and partition the CSV/multiple CSVs
          """
     
     def split_object_data(self, file_export):
          object = self.partition.select_dtypes(include=['object'])
          object.to_csv(
               file_export +"data_object_*.csv", 
               index=False, 
               mode='w', 
               header = True)

          """
          Exports only the string data type 
          """
     
     def split_float_data(self, file_export):
          float = self.partition.select_dtypes(include=['float64'])
          float.to_csv(
               file_export + "data_float_*.csv", 
               index=False, 
               mode='w', 
               header = True)

          """
          Exports only the float data type 
          """

if __name__ == "__main__":
    splitdatagen = SplitData()
    start_time = time.time()
    splitdatagen.read_csv(file_path="mock_filesystem/new_test_*.csv", npartitions=5)
    splitdatagen.split_object_data(file_export="mock_filesystem/")
    splitdatagen.split_float_data(file_export="mock_filesystem/")

    print(f"Completed data generation in {time.time() - start_time} seconds")
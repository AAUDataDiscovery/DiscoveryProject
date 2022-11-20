import pandas as pd
import dask.dataframe as dd
import time


class SplitData:
     def  read_csv(self, file_path, npartitions):
          
          ddf = dd.read_csv(
               file_path, 
               sep=',')
          
          shuffle=ddf.sample(frac=1)
          
          partition = shuffle.repartition(npartitions)
          
          object_data = partition.select_dtypes(include=['object'])

          self.object = object_data.dropna(thresh=1)
          
          float_data = partition.select_dtypes(include=['float64'])

          self.float = float_data.dropna(thresh=1)

          """
          Read, shuffle and partition the CSV/multiple CSVs
          """
     

     def export_csv(self, file_export):
          self.object.to_csv(
               file_export +"data_object_*.csv", 
               index=False, 
               mode='w', 
               header = True)

          self.float.to_csv(
               file_export + "data_float_*.csv", 
               index=False, 
               mode='w', 
               header = True)

          """
          Exports only the object & float data in separate CSVs
          """


if __name__ == "__main__":
    splitdatagen = SplitData()
    start_time = time.time()
    splitdatagen.read_csv(file_path="mock_filesystem/new_test_*.csv", npartitions=5)
    splitdatagen.export_csv(file_export="mock_filesystem/")

    print(f"Completed data generation in {time.time() - start_time} seconds")
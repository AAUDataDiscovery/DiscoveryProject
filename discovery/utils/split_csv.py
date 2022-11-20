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
          Read, shuffle and partition the CSV into multiple CSVs
          """
     

     def export_csv(self, file_export):
          self.partition.to_csv(
               file_export +"partitioned_data_*.csv", 
               index=False, 
               mode='w', 
               header = True)

          """
          Exports only the object & float data in separate CSVs
          """


if __name__ == "__main__":
    splitdatagen = SplitData()
    start_time = time.time()
    splitdatagen.read_csv(file_path="mock_filesystem/chip_data.csv", npartitions=10)
    splitdatagen.export_csv(file_export="mock_filesystem/")

    print(f"Completed data generation in {time.time() - start_time} seconds")
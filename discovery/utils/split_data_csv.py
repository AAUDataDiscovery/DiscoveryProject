import pandas as pd
import numpy as np
import random

#path of the file 
file_path = 'mock_filesystem/'

#read the csv file from the path
file_name = file_path + 'new_test_0.csv'

#get the number of lines from the csv file
number_lines = sum(1 for line in (open(file_name)))

#the number of rows for each splitted csv file
rowsize = 1000

counter = 0

#start looping through data
for i in range(0,number_lines,rowsize):

    df = pd.read_csv(
        file_name,
        nrows = rowsize,
        skiprows = i        
    )

    #reads only the continuous data
    df_continuous=df.select_dtypes(exclude=['object'])

    out_csv_continuous = file_path + 'continuous_data' + str(counter) + '.csv'

    df_continuous.to_csv(out_csv_continuous,
          index=False,
          header=True,     
          mode='a',
          chunksize=rowsize
          )

    #reads only the categoric data
    df_categoric = df.select_dtypes(include='object')

    out_csv_categoric = file_path + 'categoric_data' + str(counter) + '.csv'
    
    counter = counter + 1
    
    df_categoric.to_csv(out_csv_categoric,
          index=False,
          header=True,     
          mode='a',
          chunksize=rowsize
          )
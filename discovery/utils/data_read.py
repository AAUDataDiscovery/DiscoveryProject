import pandas as pd
import numpy as np


df = pd.read_csv('mock_filesystem/new_test_0.csv', sep=',')

"""
Reads only the continuous data
"""
df_continuous = df.select_dtypes(exclude='object')

"""
Reads only the categoric data
"""
df_categoric = df.select_dtypes(include='object')

print(f"\nAll data")
print(df.to_string())
print(f"\nOnly continuous data")
print(df_continuous)
print(f"\nOnly categoric data")
print(df_categoric)
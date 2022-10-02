import os

import pandas as pd

from dataframe_match_module.type_matchers.noop_type_matcher import NoOpTypeMatcher
from dataframe_match_module.type_matchers.temperature_type_matcher import TemperatureTypeMatcher
from dataframe_match_module.data_frame_matcher import DataFrameMatcher
from discovery.utils.metadata import Metadata
from discovery.visualizer import Visualizer

#matcher = TemperatureTypeMatcher()

#seriesGood = pd.Series(range(0,30))
#seriesBad = pd.Series(range(-1000,-600))

#singleGood = 30
#singleBad = -1000

#good1 = matcher.is_recognized_value(singleGood)
#good2 = matcher.is_recognized_series(seriesGood)

#bad1 = matcher.is_recognized_value(singleBad)
#bad2 = matcher.is_recognized_series(seriesBad)

tmatcher = TemperatureTypeMatcher()
nmatcher = NoOpTypeMatcher()

#test = DataFrameMatcher([tmatcher, nmatcher])
#cel = pd.read_csv("testdata/test_cel.csv")
#f = pd.read_csv("testdata/test_f.csv")
#res = test.match_all_columns(cel,f)
#testMD = Metadata("testdata/test_cel.csv", cel)
#testMD1 = Metadata("testdata/test_f.csv", f)

#visualizer = Visualizer([testMD, testMD1])
#visualizer.draw('test.dot')

visualizer = Visualizer("testdata")

for r, d, f in os.walk("testdata"):
    visualizer.change_working_graph(r)
    for file in f:
        if '.csv' in file:
            metadata = Metadata(file, pd.read_csv(r+"/"+file))
            visualizer.draw_metadata(metadata)

visualizer.draw('test.dot')
print("hello world")

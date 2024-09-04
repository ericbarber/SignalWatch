#exampels/temereature_regulation/preform_control_analysis.@property
# import sys
# import os
# sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../control')))
#

import pandas as pd
from pyspark.sql import SparkSession

from control.control_chart import ControlChart

# load data
data = pd.read_csv('examples/temperature_regulation/data/temperature_data_low.csv')

# pands to spark
spark = SparkSession.builder.appName('ControlAnalysis').getOrCreate()

df = spark.createDataFrame(data)

# Example Usage:
# Assuming df is a PySpark DataFrame with 'event_time' as the time column and 'event_value' as the value column
chart = ControlChart(df, 'Temp_F', 'Index')
result_df = chart.run_control_chart(chart_type='xmr')
result_df.show()

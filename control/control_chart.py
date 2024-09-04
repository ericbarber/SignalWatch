from pyspark.sql import DataFrame, Window
from pyspark.sql.types import StructType, StructField, BooleanType

import pyspark.sql.functions as F # when, lag, avg, stddev, col, lit, abs, sqrt, sum

class ControlChart:
    def __init__(self, df: DataFrame, value_col: str, time_col: str, unit_col: str = None):
        """
        Initialize the ControlChart class.
        
        :param df: Input DataFrame containing the data.
        :param value_col: The column containing the values to be monitored.
        :param time_col: The column containing the time or sequence indicator.
        :param unit_col: The column containing the units of inspection for the U chart (optional, only needed for U chart).
        """
        self.df = df
        self.value_col = value_col
        self.time_col = time_col
        self.unit_col = unit_col

    def calculate_moving_range(self) -> DataFrame:
        """
        Calculate the Moving Range (mR) for the data.
        
        :return: DataFrame with an additional column for moving range.
        """
        window_spec = Window.orderBy(self.time_col)
        self.df = self.df.withColumn(
            "previous_value",
            F.lag(self.value_col).over(window_spec)
        ).withColumn(
            "moving_range",
            F.abs(F.col(self.value_col) - F.col("previous_value"))
        )
        return self.df.drop("previous_value")
    
    def calculate_xmr_chart_values(self) -> DataFrame:
        """
        Calculate the values needed for XmR control chart.
        
        :return: DataFrame with control limits and XmR chart values.

        Example Usage:
        Assuming df is a PySpark DataFrame with 'event_time' as the time column and 'event_value' as the value column
        chart = ControlChart(df, 'event_value', 'event_time')
        result_df = chart.run_control_chart(chart_type='xmr')
        result_df.show()
        """
        # Calculate Moving Range
        df_with_mr = self.calculate_moving_range()

        # Calculate the average and standard deviation of the value column
        avg_value = df_with_mr.agg(F.avg(self.value_col)).first()[0]
        avg_mr = df_with_mr.agg(F.avg("moving_range")).first()[0]

        # Calculate control limits
        upper_control_limit = avg_value + 2.66 * avg_mr
        lower_control_limit = avg_value - 2.66 * avg_mr
        
        # Add control limits to DataFrame
        df_with_control_limits = df_with_mr.withColumn(
            "UCL", F.lit(upper_control_limit)
        ).withColumn(
            "LCL", F.lit(lower_control_limit)
        ).withColumn(
            "CL", F.lit(avg_value)
        )
        
        return df_with_control_limits
    
    def calculate_u_chart_values(self) -> DataFrame:
        """
        Calculate the values needed for a U chart.
        
        :return: DataFrame with control limits and U chart values.

        Example Usage:
        Assuming df is a PySpark DataFrame with 'event_time' as the time column and 'defects' as the value column
        chart = ControlChart(df, 'defects', 'event_time', unit_col='units')
        result_df = chart.run_control_chart(chart_type="u")
        result_df.show()
        """
        if not self.unit_col:
            raise ValueError("unit_col must be provided for U chart calculations")

        # Calculate the average defects per unit
        avg_u = self.df.agg(F.avg(F.col(self.value_col) / F.col(self.unit_col))).first()[0]

        # Calculate control limits
        self.df = self.df.withColumn(
            "UCL", avg_u + 3 * F.sqrt(avg_u / F.col(self.unit_col))
        ).withColumn(
            "LCL", F.lit(avg_u) - 3 * F.sqrt(avg_u / F.col(self.unit_col))
        ).withColumn(
            "LCL", F.col("LCL").cast("double")  # Ensures the LCL is not negative
        ).withColumn(
            "LCL", F.when(F.col("LCL") < 0, F.lit(0)).otherwise(F.col("LCL"))
        ).withColumn(
            "CL", F.lit(avg_u)
        )
        
        return self.df
    
    def calculate_cusum_chart_values(self, target_mean: float, k: float = 0.5) -> DataFrame:
        """
        Calculate the values needed for a CUSUM control chart.
        
        :param target_mean: The target or reference mean value for the process.
        :param k: The reference value, often chosen as half the shift to be detected (in standard deviations).
        :return: DataFrame with CUSUM values for positive and negative deviations.
   
        Example for CUSUM chart
        Assuming df is a PySpark DataFrame with 'event_time' as the time column and 'value' as the column to monitor
        chart = ControlChart(df, 'value', 'event_time')
        result_df = chart.run_control_chart(chart_type="cusum", target_mean=50)
        result_df.show()
        """
        # Calculate the CUSUM values
        df_cusum = self.df.withColumn(
            "C_plus", F.sum(F.col(self.value_col) - target_mean - k).over(Window.orderBy(self.time_col))
        ).withColumn(
            "C_minus", F.sum(-F.col(self.value_col) + target_mean - k).over(Window.orderBy(self.time_col))
        )
        
        return df_cusum
    
    def run_control_chart(self, chart_type="xmr", target_mean: float = None, k: float = 0.5) -> (DataFrame, DataFrame):
        """
        Run the control chart check on the data, and iterate until control is within limits.
        
        :param chart_type: The type of control chart to run ("xmr", "u", or "cusum").
        :param target_mean: The target mean value for CUSUM chart (only needed for CUSUM).
        :param k: The reference value for CUSUM chart (only needed for CUSUM, default is 0.5).
        :return: DataFrame with control chart values and checks for out-of-control points.
        
        # Example Usage:
        chart = ControlChart(df, 'event_value', 'event_time')
        cleaned_df, removed_points_df = chart.run_control_chart(chart_type='xmr')
        cleaned_df.show()
        removed_points_df.show()
        """

        if chart_type == "xmr":
            df_chart = self.calculate_xmr_chart_values()
        elif chart_type == "u":
            df_chart = self.calculate_u_chart_values()
        elif chart_type == "cusum":
            if target_mean is None:
                raise ValueError("target_mean must be provided for CUSUM chart calculations")
            df_chart = self.calculate_cusum_chart_values(target_mean, k)
            # For CUSUM, we return an empty DataFrame for removed_points_df
            removed_points = self.df.sparkSession.createDataFrame([], df_chart.schema)
            return df_chart, removed_points 
        else:
            raise ValueError(f"Unsupported chart_type: {chart_type}")


        # New columns and schema for out of control points
        out_of_control_type = StructField('out_of_control', BooleanType(), True)
        removed_points_schema = StructType(df_chart.schema.fields + [out_of_control_type])
        removed_points = self.df.sparkSession.createDataFrame([], removed_points_schema)
        control_check_iterations = 0

        while True:
            control_check_iterations += 1
            print(f'control iteration: {control_check_iterations}')
            df_chart = df_chart.withColumn(
                "out_of_control",
                (F.col(self.value_col) > F.col("UCL")) | (F.col(self.value_col) < F.col("LCL"))
            )
            
            out_of_control_points = df_chart.filter(F.col("out_of_control"))
            if out_of_control_points.count() == 0:
                break

            # Add out-of-control points to the log
            removed_points = removed_points.union(out_of_control_points)

            # Remove out-of-control points from the dataset for the next iteration
            self.df = df_chart.filter(~F.col("out_of_control")).drop("out_of_control")

            # Recalculate control limits after removing out-of-control points
            if chart_type == "xmr":
                df_chart = self.calculate_xmr_chart_values()
            elif chart_type == "u":
                df_chart = self.calculate_u_chart_values()
    
        return df_chart, removed_points

    def run_first_control_chart(self, chart_type="xmr", target_mean: float = None, k: float = 0.5) -> DataFrame:
        """
        Run the control chart check on the data for single iteration of control check.
        
        :param chart_type: The type of control chart to run ("xmr", "u", or "cusum").
        :param target_mean: The target mean value for CUSUM chart (only needed for CUSUM).
        :param k: The reference value for CUSUM chart (only needed for CUSUM, default is 0.5).
        :return: DataFrame with control chart values and checks for out-of-control points.
        """
        if chart_type == "xmr":
            df_chart = self.calculate_xmr_chart_values()
        elif chart_type == "u":
            df_chart = self.calculate_u_chart_values()
        elif chart_type == "cusum":
            if target_mean is None:
                raise ValueError("target_mean must be provided for CUSUM chart calculations")
            df_chart = self.calculate_cusum_chart_values(target_mean, k)
        else:
            raise ValueError(f"Unsupported chart_type: {chart_type}")

        # Check for out-of-control points (for XmR and U charts)
        if chart_type in ["xmr", "u"]:
            df_chart = df_chart.withColumn(
                "out_of_control",
                (F.col(self.value_col) > F.col("UCL")) | (F.col(self.value_col) < F.col("LCL"))
            )
        
        return df_chart

import pytest
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from control.control_chart import ControlChart  # assuming your module is named control_chart.py

@pytest.fixture(scope="module")
def spark():
    # Initialize a SparkSession
    spark = SparkSession.builder \
        .master("local[2]") \
        .appName("ControlChartTest") \
        .getOrCreate()
    yield spark
    spark.stop()

@pytest.fixture
def sample_df(spark):
    # Sample DataFrame for testing
    data = [
        (1, 10, 1.0),
        (2, 20, 1.5),
        (3, 15, 0.8),
        (4, 30, 2.2),
        (5, 25, 1.9)
    ]
    schema = StructType([
        StructField("event_time", IntegerType(), True),
        StructField("event_value", IntegerType(), True),
        StructField("units", FloatType(), True)
    ])
    return spark.createDataFrame(data, schema)

def test_calculate_moving_range(sample_df):
    chart = ControlChart(sample_df, 'event_value', 'event_time')
    result_df = chart.calculate_moving_range()
    
    # Expected results can be computed manually
    expected_data = [
        (1, 10, None),
        (2, 20, 10.0),
        (3, 15, 5.0),
        (4, 30, 15.0),
        (5, 25, 5.0)
    ]
    
    # Collect the result and compare
    result_data = [(row.event_time, row.event_value, row.moving_range) for row in result_df.collect()]
    assert result_data == expected_data

def test_calculate_xmr_chart_values(sample_df):
    chart = ControlChart(sample_df, 'event_value', 'event_time')
    result_df = chart.calculate_xmr_chart_values()
    
    # Add your logic to check if control limits and other values are calculated correctly
    assert "UCL" in result_df.columns
    assert "LCL" in result_df.columns
    assert "CL" in result_df.columns

def test_calculate_u_chart_values(sample_df):
    chart = ControlChart(sample_df, 'event_value', 'event_time', unit_col='units')
    result_df = chart.calculate_u_chart_values()
    
    # Check that UCL, LCL, and CL columns are present
    assert "UCL" in result_df.columns
    assert "LCL" in result_df.columns
    assert "CL" in result_df.columns

def test_calculate_cusum_chart_values(sample_df):
    chart = ControlChart(sample_df, 'event_value', 'event_time')
    target_mean = 20
    result_df = chart.calculate_cusum_chart_values(target_mean)
    
    # Check for C_plus and C_minus columns
    assert "C_plus" in result_df.columns
    assert "C_minus" in result_df.columns

def test_run_control_chart_xmr(sample_df):
    chart = ControlChart(sample_df, 'event_value', 'event_time')
    cleaned_df, removed_points_df = chart.run_control_chart(chart_type='xmr')
    
    # Ensure the returned DataFrames are not empty
    assert cleaned_df.count() > 0
    assert removed_points_df.count() >= 0  # Could be zero if no points were removed

def test_run_control_chart_u(sample_df):
    chart = ControlChart(sample_df, 'event_value', 'event_time', unit_col='units')
    cleaned_df, removed_points_df = chart.run_control_chart(chart_type='u')
    
    # Ensure the returned DataFrames are not empty
    assert cleaned_df.count() > 0
    assert removed_points_df.count() >= 0  # Could be zero if no points were removed

def test_run_control_chart_cusum(sample_df):
    chart = ControlChart(sample_df, 'event_value', 'event_time')
    target_mean = 20
    cleaned_df, removed_points_df = chart.run_control_chart(chart_type='cusum', target_mean=target_mean)
    
    # Ensure the returned DataFrames are not empty
    assert cleaned_df.count() > 0
    assert removed_points_df.count() >= 0  # Could be zero if no points were removed

def test_calculate_u_chart_values_without_unit_col(sample_df):
    # Initialize ControlChart without unit_col
    chart = ControlChart(sample_df, 'event_value', 'event_time')
    
    # Expect a ValueError to be raised when calculate_u_chart_values is called
    with pytest.raises(ValueError, match="unit_col must be provided for U chart calculations"):
        chart.calculate_u_chart_values()

def test_run_control_chart_cusum_without_target_mean(sample_df):
    # Initialize ControlChart
    chart = ControlChart(sample_df, 'event_value', 'event_time')
    
    # Expect a ValueError when target_mean is None
    with pytest.raises(ValueError, match="target_mean must be provided for CUSUM chart calculations"):
        chart.run_control_chart(chart_type="cusum", target_mean=None)

def test_run_control_chart_with_unsupported_type(sample_df):
    # Initialize ControlChart
    chart = ControlChart(sample_df, 'event_value', 'event_time')
    
    # Expect a ValueError when an unsupported chart_type is provided
    with pytest.raises(ValueError, match="Unsupported chart_type: invalid_type"):
        chart.run_control_chart(chart_type="invalid_type")

###############################################################################
# Test first run of control analysis function
###############################################################################
def test_run_first_control_chart_xmr(sample_df):
    chart = ControlChart(sample_df, 'event_value', 'event_time')
    result_df = chart.run_first_control_chart(chart_type="xmr")
    
    # Assert that the result DataFrame contains the expected columns
    assert "UCL" in result_df.columns
    assert "LCL" in result_df.columns
    assert "CL" in result_df.columns
    assert "out_of_control" in result_df.columns

def test_run_first_control_chart_u(sample_df):
    chart = ControlChart(sample_df, 'event_value', 'event_time', unit_col='units')
    result_df = chart.run_first_control_chart(chart_type="u")
    
    # Assert that the result DataFrame contains the expected columns
    assert "UCL" in result_df.columns
    assert "LCL" in result_df.columns
    assert "CL" in result_df.columns
    assert "out_of_control" in result_df.columns

def test_run_first_control_chart_cusum(sample_df):
    chart = ControlChart(sample_df, 'event_value', 'event_time')
    target_mean = 20
    result_df = chart.run_first_control_chart(chart_type="cusum", target_mean=target_mean)
    
    # Assert that the result DataFrame contains the expected CUSUM columns
    assert "C_plus" in result_df.columns
    assert "C_minus" in result_df.columns

def test_run_first_control_chart_cusum_without_target_mean(sample_df):
    chart = ControlChart(sample_df, 'event_value', 'event_time')
    
    # Expect a ValueError when target_mean is None for CUSUM chart
    with pytest.raises(ValueError, match="target_mean must be provided for CUSUM chart calculations"):
        chart.run_first_control_chart(chart_type="cusum", target_mean=None)

def test_run_first_control_chart_with_unsupported_type(sample_df):
    chart = ControlChart(sample_df, 'event_value', 'event_time')
    
    # Expect a ValueError when an unsupported chart_type is provided
    with pytest.raises(ValueError, match="Unsupported chart_type: invalid_type"):
        chart.run_first_control_chart(chart_type="invalid_type")

###############################################################################
# Test process of elimonating out of control data from resulting control.
###############################################################################
@pytest.fixture
def sample_df_with_out_of_control_points(spark):
    # Create a DataFrame with values that will be out-of-control in an XmR chart
    data = [
        Row(event_time=1, event_value=120),
        Row(event_time=2, event_value=500),  # Out-of-control point
        Row(event_time=3, event_value=115),
        Row(event_time=4, event_value=111),
        Row(event_time=5, event_value=3),  # Out-of-control poi0nt
        Row(event_time=6, event_value=120),
        Row(event_time=7, event_value=115),
        Row(event_time=8, event_value=117),
        Row(event_time=9, event_value=118),
        Row(event_time=10, event_value=119),
        Row(event_time=11, event_value=115),
        Row(event_time=12, event_value=116),
        Row(event_time=13, event_value=117),
        Row(event_time=14, event_value=118),
        Row(event_time=15, event_value=119),
        Row(event_time=16, event_value=120),
        Row(event_time=17, event_value=114),
        Row(event_time=18, event_value=115),
        Row(event_time=19, event_value=114),
        Row(event_time=20, event_value=115),
        Row(event_time=21, event_value=116),
        Row(event_time=22, event_value=117),
        Row(event_time=23, event_value=116),
        Row(event_time=24, event_value=117),
        Row(event_time=25, event_value=120)
    ]
    return spark.createDataFrame(data)

@pytest.fixture
def sample_df_of_in_control_points(spark):
    # Create a DataFrame with values that will be out-of-control in an XmR chart
    data = [
        Row(event_time=1, event_value=120),
        Row(event_time=3, event_value=115),
        Row(event_time=4, event_value=111),
        Row(event_time=6, event_value=120),
        Row(event_time=7, event_value=115),
        Row(event_time=8, event_value=117),
        Row(event_time=9, event_value=118),
        Row(event_time=10, event_value=119),
        Row(event_time=11, event_value=115),
        Row(event_time=12, event_value=116),
        Row(event_time=13, event_value=117),
        Row(event_time=14, event_value=118),
        Row(event_time=15, event_value=119),
        Row(event_time=16, event_value=120),
        Row(event_time=17, event_value=114),
        Row(event_time=18, event_value=115),
        Row(event_time=19, event_value=114),
        Row(event_time=20, event_value=115),
        Row(event_time=21, event_value=116),
        Row(event_time=22, event_value=117),
        Row(event_time=23, event_value=116),
        Row(event_time=24, event_value=117),
        Row(event_time=25, event_value=120)
    ]
    return spark.createDataFrame(data)

@pytest.fixture
def sample_df_of_out_of_control_points(spark):
    # Create a DataFrame with values that will be out-of-control in an XmR chart
    data = [
        Row(event_time=2, event_value=500),  # Out-of-control point
        Row(event_time=5, event_value=3),  # Out-of-control poi0nt
    ]
    return spark.createDataFrame(data)

def test_run_control_chart_xmr_with_out_of_control_points(
    sample_df_with_out_of_control_points,
    sample_df_of_out_of_control_points,
    sample_df_of_in_control_points
):
    chart = ControlChart(sample_df_with_out_of_control_points, 'event_value', 'event_time')
    cleaned_df, removed_points_df = chart.run_control_chart(chart_type="xmr")
    
    print("Cleaned DataFrame:")
    cleaned_df.show()
    
    print("Removed Points DataFrame:")
    removed_points_df.show()

    # Collect the actual removed points
    removed_points = removed_points_df.select("event_time", "event_value").collect()
    expected_removed_points = sample_df_of_out_of_control_points.select("event_time", "event_value").collect()
    
    # Ensure that removed_points_df contains the expected out-of-control points
    assert sorted([(row.event_time, row.event_value) for row in removed_points]) == sorted(
        [(row.event_time, row.event_value) for row in expected_removed_points]
    )

    # Collect the actual cleaned points
    cleaned_points = cleaned_df.select("event_time", "event_value").collect()
    expected_cleaned_points = sample_df_of_in_control_points.select("event_time", "event_value").collect()

    # Ensure that cleaned_df does not contain the out-of-control points
    assert sorted([(row.event_time, row.event_value) for row in cleaned_points]) == sorted(
        [(row.event_time, row.event_value) for row in expected_cleaned_points]
    )

    # Check that the total number of points matches the original dataset
    assert cleaned_df.count() + removed_points_df.count() == sample_df_with_out_of_control_points.count()

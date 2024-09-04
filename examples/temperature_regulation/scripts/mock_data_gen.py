import numpy as np
import pandas as pd
import os

# Set the random seed for reproducibility
np.random.seed(42)

# Ensure the 'data' directory exists
output_dir = os.path.join(os.path.dirname(__file__), 'data/parquet')
os.makedirs(output_dir, exist_ok=True)

# Function to generate temperature data with normal and special cause variations
def generate_temperature_data(mean, std_dev, precision, size=180, special_cause=False):
    # Generate normal cause variation data
    data = np.random.normal(loc=mean, scale=std_dev, size=size)
    
    if special_cause:
        # Introduce special cause variation in some random points
        special_cause_indices = np.random.choice(range(size), size=size // 10, replace=False)
        data[special_cause_indices] += np.random.normal(loc=0, scale=std_dev * 5, size=len(special_cause_indices))
    
    # Round the data according to the precision
    return np.round(data, precision)

# Parameters for the different precision levels
precision_params = {
    'low': {'mean': 72, 'std_dev': 2, 'precision': 1},     # Mean 72°F, standard deviation ±2°F, precision 1 decimal place
    'medium': {'mean': 72, 'std_dev': 0.5, 'precision': 2}, # Mean 72°F, standard deviation ±0.5°F, precision 2 decimal places
    'high': {'mean': 72, 'std_dev': 0.1, 'precision': 3}   # Mean 72°F, standard deviation ±0.1°F, precision 3 decimal places
}

# Generate datasets
datasets = {}
for precision, params in precision_params.items():
    datasets[precision] = pd.DataFrame({
        'Index': range(1, 181),
        'Temp_F': generate_temperature_data(params['mean'], params['std_dev'], params['precision'], special_cause=True)
    })

# Output the datasets
for precision, df in datasets.items():
    print(f"\n{precision.capitalize()} Precision Dataset:")
    print(df.head())  # Display the first few rows of each dataset
    # df.to_csv(f"data/temperature_data_{precision}.csv", index=False)  # Save to CSV
    df.to_parquet(f"{output_dir}/temperature_data_{precision}.parquet", index=False)  # Save to parquet


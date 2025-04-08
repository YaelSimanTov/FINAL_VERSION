import pandas as pd
import os

#Question 4:
def read_file(file_path):
    """Reads a file and returns a DataFrame."""
    if file_path.endswith('.xlsx'):
        return pd.read_excel(file_path)
    elif file_path.endswith('.parquet'):
        return pd.read_parquet(file_path)
    else:
        raise ValueError("Unsupported file format. Please provide a valid XLSX or PARQUET file.")

def validate_and_clean_data(input_file: str, output_excel: str):
    """
    Validates and cleans data in the table:
    - Checks if 'timestamp' is in the correct format
    - Removes duplicate 'timestamp' entries
    - Checks if 'value' column contains valid numeric values

    :param input_file: Path to input Excel file with data
    :param output_excel: Path to save the cleaned data
    """
    # Load data from Excel
    df = read_file(input_file)

    # Check Timestamp format (YYYY-MM-DD HH:MM:SS)
    try:
        df['timestamp'] = pd.to_datetime(df['timestamp'], format='%Y-%m-%d %H:%M:%S', errors='raise')
    except Exception as e:
        print(f"Timestamp format error: {e}")
        return

    # Remove duplicate rows based on Timestamp
    df.drop_duplicates(subset='timestamp', keep='first', inplace=True)

    # Ensure 'value' column contains numeric values
    df['value'] = pd.to_numeric(df['value'], errors='coerce')  # Convert to numeric, invalid entries become NaN

    # Drop rows where 'value' is NaN
    df.dropna(subset=['value'], inplace=True)

    # Save the cleaned data to a new Excel file
    df.to_excel(output_excel, index=False)
    print(f"Cleaned data saved to: {output_excel}")


def average_per_hour_per_date(input_file, output_file):
    """
    Reads a timestamped dataset, calculates average values per hour per day,
    and writes the result to an Excel file.

    Args:
        input_file (str): Path to the input Excel/CSV file.
        output_file (str): Path where the output Excel file will be saved.
    """
    # Load the data
    df = read_file(input_file)

    # Ensure timestamp is in datetime format
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    # Extract date and hour
    df['Date'] = df['timestamp'].dt.date
    df['Hour'] = df['timestamp'].dt.hour

    # Group by Date and Hour, then calculate mean
    result = df.groupby(['Date', 'Hour'])['value'].mean().reset_index()
    result.rename(columns={'value': 'Average Value'}, inplace=True)

    # Save to Excel
    result.to_excel(output_file, index=False)

    print(f"Saved result to {output_file}")


def split_file_by_date(input_file, output_folder):
    """
    Splits a time series XLSX file into separate files for each date.

    Args:
        input_file (str): Path to the input XLSX file. Must include 'Timestamp' and 'Value' columns.
        output_folder (str): Path to the output folder where daily files will be saved.
    """
    # Create output folder if it doesn't exist
    os.makedirs(output_folder, exist_ok=True)

    # Load the XLSX file
    df = read_file(input_file)

    # Convert 'timestamp' to datetime
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    # Extract just the date part
    df['Date'] = df['timestamp'].dt.date

    # Group by each unique date
    for date, group in df.groupby('Date'):
        output_file = os.path.join(output_folder, f"{date}.xlsx")
        group.drop(columns='Date').to_excel(output_file, index=False)
        print(f"Saved: {output_file}")


def calculate_hourly_avg_and_combine(input_folder, final_output_file):
    """
    This function processes all the files in the input folder, calculates the hourly average
    for each file, and combines the results into a final output Excel file.

    Args:
        input_folder (str): Folder containing the individual date-based files.
        final_output_file (str): Path to the final combined output Excel file.
    """
    # Initialize a list to store results from each file
    combined_results = []

    # Loop through all files in the input folder
    for file_name in os.listdir(input_folder):
        file_path = os.path.join(input_folder, file_name)

        # Only process .xlsx files
        if file_name.endswith('.xlsx'):
            # Read the Excel file into a DataFrame
            df = pd.read_excel(file_path)

            # Ensure that the columns 'Timestamp' and 'Value' are present
            if 'timestamp' not in df.columns or 'value' not in df.columns:
                print(f"Skipping {file_name}: Missing 'timestamp' or 'value' columns.")
                continue

            # Convert the 'timestamp' column to datetime format
            df['timestamp'] = pd.to_datetime(df['timestamp'])

            # Extract hour from 'Timestamp'
            df['Hour'] = df['timestamp'].dt.hour

            # Calculate hourly averages (group by 'Hour' and calculate mean of 'Value')
            hourly_avg = df.groupby('Hour')['value'].mean().reset_index()

            # Add a 'Date' column based on the file name (assuming filename is the date)
            date_str = file_name.split('.')[0]  # Assuming the file is named like '2025-06-10.xlsx'
            hourly_avg['Date'] = date_str

            # Append this file's result to the combined results list
            combined_results.append(hourly_avg)

            print(f"Processed {file_name} and calculated hourly averages.")

    # Combine all the individual hourly average DataFrames into one
    final_combined_df = pd.concat(combined_results, ignore_index=True)

    # Save the final combined DataFrame to an Excel file
    with pd.ExcelWriter(final_output_file) as writer:
        final_combined_df.to_excel(writer, index=False, sheet_name='Hourly Averages')

    print(f"Final combined hourly averages saved to {final_output_file}")


#Question 1_A:
# validate_and_clean_data('PART_1_B/time_series.xlsx', 'PART_1_B/cleaned_time_series.xlsx')
#Question 1_B:
# average_per_hour_per_date("PART_1_B/cleaned_time_series.xlsx", "PART_1_B/average_by_hour.xlsx")

#Question 2:
# split_file_by_date("PART_1_B/cleaned_time_series.xlsx", "PART_1_B/daily_files")
# calculate_hourly_avg_and_combine(input_folder="PART_1_B/daily_files", final_output_file='PART_1_B/final_hourly_averages.xlsx')

#Question 4:
# validate_and_clean_data('PART_1_B/time_series (4).parquet', 'PART_1_B/cleaned_time_series.xlsx')
# average_per_hour_per_date("PART_1_B/cleaned_time_series.xlsx", "PART_1_B/average_by_hour.xlsx")

file_path = 'PART_1_B/time_series (4).parquet'
df = pd.read_parquet(file_path)

print(df.head())

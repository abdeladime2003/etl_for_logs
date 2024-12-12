import os
import pandas as pd
from datetime import datetime
import re

# Ensure directories exist
os.makedirs("data/raw", exist_ok=True)
os.makedirs("data/transformed", exist_ok=True)

# Directory paths
input_folder = "data"
output_file = r"output\transformed_data.csv"
# Transformation Script
def transform_data(input_folder, output_file):
    try:
        # Step 1: Read and Concatenate All CSV Files
        print("Loading and concatenating all CSV files from the data folder...")
        all_files = [os.path.join(input_folder, f) for f in os.listdir(input_folder) if f.endswith('.csv')]
        raw_data = pd.concat([pd.read_csv(file) for file in all_files], ignore_index=True)
        print(f"Loaded {len(all_files)} files with a total of {len(raw_data)} rows.")

        # Step 2: Convert Date to datetime
        print("Converting Date column to datetime...")
        raw_data['Date'] = pd.to_datetime(raw_data['Date'], errors='coerce')
        #change Date to string

        # Step 3: Define Excluded Departments and Types
        excluded_departments = [
            'Admin', 'Executive', 'Facilities', 'Finance', 'Human Resources',
            'Information Technology', 'Manager', 'Marketing', 'Pipeline & Development', 'Reception'
        ]
        excluded_types = ['Meal Break', 'Time Off Unpaid']
        current_year = datetime.now().year

        # Step 4: Filter the Data
        print("Filtering data...")
        filtered_data = raw_data[
            ~raw_data['Department'].isin(excluded_departments) &  # Exclude departments
            ~raw_data['Type'].isin(excluded_types) &  # Exclude types
            (raw_data['Date'].dt.year == current_year)  # Keep only current year
        ]
        print(f"Data filtered: {len(filtered_data)} rows remain.")

        # Step 5: Retain Only the Essential Columns
        essential_columns = [
            'Id', 'Location', 'Person > Brand', 'Department', 'Type', 'Person', 'Date', 'Logged Hours'
        ]
        print("Selecting essential columns...")
        filtered_data = filtered_data[essential_columns]

        # Step 6: Add Week of Year
        print("Adding Week of Year column...")
        filtered_data['Week of Year'] = filtered_data['Date'].dt.isocalendar().week

        # Step 7: Add Utilized Column
        def map_utilized(activity_type):
            if activity_type in ['Worktime', 'Vacation', 'Holiday', 'Time Bank']:
                return 'yes'
            elif activity_type in ['Idle', 'Meeting', 'Training', 'Overhead', 'RND']:
                return 'no'
            return 'unknown'

        print("Adding Utilized column...")
        filtered_data['Utilized'] = filtered_data['Type'].apply(map_utilized)

        # Step 8: Add tmpLayoff Column
        print("Adding tmpLayoff column...")
        weekly_hours = filtered_data.groupby(['Person', 'Week of Year'])['Logged Hours'].transform('sum')
        filtered_data['tmpLayoff'] = weekly_hours.apply(lambda x: 1 if x <= 8 else '')

        # Step 9: Aggregate Data
        print("Aggregating data for the fact table...")
        def extract_numbers(s):
            match = re.search(r'\d+', str(s))
            return match.group() if match else None

        aggregated_data = filtered_data.groupby(
            ['Location', 'Person > Brand', 'Department', 'Week of Year', 'Type', 'Date'],
            as_index=False
        ).agg(
            Total_Hours=('Logged Hours', 'sum'),
            Persons=('Person', lambda x: ', '.join(map(str, sorted(set(map(extract_numbers, x)))))),
            Total_Utilized=('Utilized', lambda x: (x == 'yes').sum())
        )

        # Step 10: Save the Transformed Data
        print(f"Saving transformed data to {output_file}...")
        aggregated_data.to_csv(output_file, index=False)
        print("Data transformation complete. Transformed data saved.")

    except Exception as e:
        print(f"An error occurred during transformation: {e}")

# Execute the transformation
transform_data(input_folder, output_file)

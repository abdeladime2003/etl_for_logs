import sqlite3
import pandas as pd

def create_connection(db_file):
    """Create a database connection"""
    try:
        return sqlite3.connect(db_file)
    except sqlite3.Error as e:
        print(f"Error: {e}")
        return None

def create_tables(conn):
    """Create tables for the database"""
    try:
        cursor = conn.cursor()

        # Dimension tables
        cursor.execute("""CREATE TABLE IF NOT EXISTS location_dim (
            Location_ID INTEGER PRIMARY KEY,
            Location TEXT NOT NULL
        );""")

        cursor.execute("""CREATE TABLE IF NOT EXISTS brand_dim (
            Brand_ID INTEGER PRIMARY KEY,
            Brand TEXT NOT NULL
        );""")

        cursor.execute("""CREATE TABLE IF NOT EXISTS department_dim (
            Department_ID INTEGER PRIMARY KEY,
            Department TEXT NOT NULL
        );""")

        cursor.execute("""CREATE TABLE IF NOT EXISTS type_dim (
            Type_ID INTEGER PRIMARY KEY,
            Type TEXT NOT NULL
        );""")

        cursor.execute("""CREATE TABLE IF NOT EXISTS time_dim (
            Time_ID INTEGER PRIMARY KEY,
            Week_Of_Year INTEGER NOT NULL,
            Date TEXT NOT NULL
        );""")

        cursor.execute("""CREATE TABLE IF NOT EXISTS person_dim (
            Person_ID INTEGER PRIMARY KEY,
            Person TEXT NOT NULL
        );""")

        # Fact table
        cursor.execute("""CREATE TABLE IF NOT EXISTS resource_utilization_fact (
            Fact_ID INTEGER PRIMARY KEY,
            Location_ID INTEGER NOT NULL,
            Brand_ID INTEGER NOT NULL,
            Department_ID INTEGER NOT NULL,
            Person_ID INTEGER NOT NULL,
            Time_ID INTEGER NOT NULL,
            Type_ID INTEGER NOT NULL,
            Total_Hours REAL,
            Total_Utilized INTEGER,
            FOREIGN KEY (Location_ID) REFERENCES location_dim(Location_ID),
            FOREIGN KEY (Brand_ID) REFERENCES brand_dim(Brand_ID),
            FOREIGN KEY (Department_ID) REFERENCES department_dim(Department_ID),
            FOREIGN KEY (Time_ID) REFERENCES time_dim(Time_ID),
            FOREIGN KEY (Type_ID) REFERENCES type_dim(Type_ID),
            FOREIGN KEY (Person_ID) REFERENCES person_dim(Person_ID)
        );""")

        conn.commit()
    except sqlite3.Error as e:
        print(f"Error creating tables: {e}")

def load_data(conn, data):
    """Load data into the database"""
    try:
        cursor = conn.cursor()

        # Insert data into dimension tables
        locations = data['Location'].drop_duplicates().reset_index(drop=True)
        brands = data['Person > Brand'].drop_duplicates().reset_index(drop=True)
        departments = data['Department'].drop_duplicates().reset_index(drop=True)
        types = data['Type'].drop_duplicates().reset_index(drop=True)
        times = data[['Week of Year', 'Date']].drop_duplicates().reset_index(drop=True)
        persons = data['Persons'].explode(', ').drop_duplicates().reset_index(drop=True)

        # Load locations
        for i, location in enumerate(locations, start=1):
            cursor.execute("INSERT INTO location_dim (Location_ID, Location) VALUES (?, ?)", (i, location))

        # Load brands
        for i, brand in enumerate(brands, start=1):
            cursor.execute("INSERT INTO brand_dim (Brand_ID, Brand) VALUES (?, ?)", (i, brand))

        # Load departments
        for i, department in enumerate(departments, start=1):
            cursor.execute("INSERT INTO department_dim (Department_ID, Department) VALUES (?, ?)", (i, department))

        # Load types
        for i, type_value in enumerate(types, start=1):
            cursor.execute("INSERT INTO type_dim (Type_ID, Type) VALUES (?, ?)", (i, type_value))

        # Load time dimension
        for i, row in times.iterrows():
            cursor.execute("INSERT INTO time_dim (Time_ID, Week_Of_Year, Date) VALUES (?, ?, ?)",
                           (i + 1, row['Week of Year'], row['Date']))

        # Load persons
        for i, person in enumerate(persons, start=1):
            cursor.execute("INSERT INTO person_dim (Person_ID, Person) VALUES (?, ?)", (i, person))

        # Insert into fact table (resource_utilization_fact) using the dimension IDs
        for _, row in data.iterrows():
            location_id = int(locations[locations == row['Location']].index[0] + 1)
            brand_id = int(brands[brands == row['Person > Brand']].index[0] + 1)
            department_id = int(departments[departments == row['Department']].index[0] + 1)
            type_id = int(types[types == row['Type']].index[0] + 1)
            time_id = int(times[times['Date'] == row['Date']].index[0] + 1)
            person_id = int(persons[persons == row['Persons']].index[0] + 1)
            cursor.execute("""
                INSERT INTO resource_utilization_fact (
                    Location_ID, Brand_ID, Department_ID, Time_ID, Type_ID, Person_ID,
                    Total_Hours, Total_Utilized
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (location_id, brand_id, department_id, time_id, type_id, person_id,
             row['Total_Hours'], row['Total_Utilized']))

        conn.commit()
    except sqlite3.Error as e:
        print(f"Error loading data: {e}")
    print(locations.dtypes)
    print(brands.dtypes)
    print(departments.dtypes)
    print(types.dtypes)
    print(times.dtypes)
    print(persons.dtypes)

if __name__ == "__main__":
    # File path to your CSV
    data_file = r"output\transformed_data.csv"  # Replace with your actual file path

    # Read the CSV data
    data = pd.read_csv(data_file)

    # Database file
    database = r"databases\inventory.db"  # Replace with your actual file path

    # Connect to the database
    conn = create_connection(database)

    if conn:
        # Create tables
        create_tables(conn)

        # Load data
        load_data(conn, data)

        conn.close()

    print("Data loaded successfully!")

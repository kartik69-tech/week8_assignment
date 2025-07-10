# week8_assignment
#  NYC Taxi Dataset Analysis using PySpark 

This project demonstrates a local PySpark-based data analysis pipeline on the NYC Yellow Taxi dataset (January 2020).

##  Files and Structure


Assignment_8/
│
├── analysis_from_excel.py                   # Main PySpark script
├── yellow_tripdata_2020_01_synthetic.xlsx   # NYC Yellow Taxi data (January 2020)
├── output/                                  # Output Parquet files 
└── README.md                                # Project documentation and instructions


##  Objective

Perform the following queries using PySpark on a local machine:

1. Add a new column called `Revenue` — sum of multiple fare-related fields.
2. Count total passengers by pickup area.
3. Calculate average fare/earnings by vendor.
4. Track a moving count of payments by payment mode.
5. Identify top 2 earning vendors on a specific date with passengers and trip distance.
6. Find most traveled route by passenger count.
7. Detect top pickup locations in the last 5/10 seconds (stream simulation).
8. Save results as Parquet files for downstream use.


## Tools Used

| Tool         | Purpose                            |
|--------------|-------------------------------------|
| Python 3.x   | Base programming language           |
| PySpark      | Data processing engine (local mode) |
| Pandas       | Excel file reading                  |
| openpyxl     | Excel engine backend for Pandas     |



## ⚙️ Setup Instructions (Local Machine)

### Step 1: Install Dependencies

Make sure Python is installed, then run:

pip install pyspark pandas openpyxl


### Step 2: Download the Dataset

Place the file `yellow_tripdata_2020_01_synthetic.xlsx` in the root folder (already included if cloned).

### Step 3: Run the Script

python analysis_from_excel.py


This will:

- Load the Excel file using Pandas
- Convert it to a PySpark DataFrame
- Execute all queries
- Save a Parquet version of the dataset in the `output/` directory



##  Breakdown of Queries

###  Query 1: Add `Revenue` Column
Adds a new column that sums:
- `fare_amount`
- `extra`
- `mta_tax`
- `improvement_surcharge`
- `tip_amount`
- `tolls_amount`
- `total_amount`

###  Query 2: Passenger Count by Pickup Area
Groups by `PULocationID` and sums `passenger_count`.

###  Query 3: Average Fare and Earnings by Vendor
Groups by `VendorID` and aggregates `fare_amount` and `total_amount`.

###  Query 4: Moving Count of Payments
Uses Spark's window functions to generate a running count per `payment_type`.

###  Query 5: Top Vendors on Specific Date
Filters data for `2020-01-15`, groups by vendor, and ranks them based on total revenue, passenger count, and trip distance.

###  Query 6: Most Traveled Route
Groups by `PULocationID` and `DOLocationID` with highest total passengers.

###  Query 7: Pickup Locations in Last 10 Seconds
Simulates streaming data by filtering based on the latest pickup timestamp and counts passengers per pickup location.

###  Query 8: Write as Parquet
The final DataFrame is saved as Parquet in the `output/` directory.


##  Output

The output Parquet files are written to:


output/yellow_tripdata_2020_01_parquet/


You can read them later using:

df_parquet = spark.read.parquet("output/yellow_tripdata_2020_01_parquet")
df_parquet.show()









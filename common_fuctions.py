import csv
import random
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import avro.schema
import avro.datafile
import avro.io
from faker import Faker
import os
import configparser
import json
from datetime import datetime

fake = Faker()

def read_schema_from_csv(schema_file):
    schema = []
    with open(schema_file, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            schema.append({'name': row['column_name'], 'type': row['data_type']})
    return schema
def timestamp_to_millis(dt):
    """Convert a datetime object to milliseconds since epoch."""
    return int(dt.timestamp() * 1000)
def generate_data(schema, num_rows, file_type,database_type='bigquery',):
    data = []

    for _ in range(num_rows):
        row = {}

        for column in schema:
            name = column['name'].lower()
            dtype = column['type'].upper()

            # Context-aware data generation
            if "name" in name and dtype == 'STRING':
                row[column['name']] = fake.name()
            elif "email" in name  and dtype == 'STRING':
                row[column['name']] = fake.email()
            elif "phone" in name or "contact" in name  and dtype == 'INT64':
                row[column['name']] = fake.phone_number()
            elif "address" in name  and dtype == 'STRING':
                row[column['name']] = fake.address()
            elif "city" in name  and dtype == 'STRING':
                row[column['name']] = fake.city()
            elif "state" in name  and dtype == 'STRING': 
                row[column['name']] = fake.state()
            elif "country" in name and dtype == 'STRING':
                row[column['name']] = fake.country()
            elif "zipcode" in name or "postal" in name  and dtype == 'INT64':
                row[column['name']] = fake.zipcode()
            elif "date" in name and dtype == 'DATE':
                row[column['name']] = fake.date()
            elif "time" in name and dtype in ['TIME', 'DATETIME']:
                row[column['name']] = fake.iso8601()
            elif ("price" in name or "amount" in name) and dtype in ['NUMERIC', 'FLOAT']:
                row[column['name']] = round(random.uniform(1.0, 1000000.0), 2)
            elif "id" in name and dtype in ['INTEGER', 'INT64']:
                row[column['name']] = random.randint(1, 100000)
            elif dtype == 'BOOLEAN':
                row[column['name']] = random.choice([True, False])
            elif dtype == 'BYTES':
                row[column['name']] = fake.binary(length=10)
            elif dtype == 'GEOGRAPHY':
                row[column['name']] = f"POINT({random.uniform(-180.0, 180.0)} {random.uniform(-90.0, 90.0)})"
            elif dtype == 'BIGNUMERIC':
                row[column['name']] = round(random.uniform(1.0, 1000000000.0), 38)
            elif dtype == 'TIMESTAMP' and file_type.lower() =="avro":
                row[column['name']] = timestamp_to_millis(fake.date_time_this_year())
            elif dtype == 'TIMESTAMP' :
                row[column['name']] = fake.date_time().isoformat()               

            else:
                # Fallback for unspecified cases
                row[column['name']] = fake.word()

            # Database-specific adjustments
            if database_type == 'bigquery':
                if dtype == 'TIMESTAMP' and file_type.lower() =="avro":
                    row[column['name']] = timestamp_to_millis(fake.date_time_this_year())
                elif dtype in ['DATETIME']:
                    row[column['name']] = fake.iso8601()
                elif dtype == 'DATE':
                    row[column['name']] = fake.date()
            elif database_type == 'mysql':
                if dtype == 'DATETIME':
                    row[column['name']] = fake.date_time().strftime('%Y-%m-%d %H:%M:%S')
            elif database_type == 'postgresql':
                if dtype == 'DATETIME':
                    row[column['name']] = fake.date_time().isoformat()
            elif database_type == 'sqlserver':
                if dtype == 'DATE':
                    row[column['name']] = fake.date().replace('-', '/')
            elif database_type == 'teradata':
                if dtype in ['DATE', 'TIMESTAMP']:
                    row[column['name']] = fake.date_time().strftime('%Y-%m-%d %H:%M:%S')

        data.append(row)

    return data

def save_to_csv(data, output_file):
    df = pd.DataFrame(data)
    df.to_csv(output_file, index=False)

def save_to_parquet(data, output_file):
    df = pd.DataFrame(data)
    table = pa.Table.from_pandas(df)
    pq.write_table(table, output_file)

def get_avro_type(value):
    if value is None:
        return ["null"]
    elif isinstance(value, str):
        return "string"
    elif isinstance(value, int):
        return "long"  # Avro uses "long" for integers (int64 in BigQuery)
    elif isinstance(value, float):
        return "float"
    elif isinstance(value, bool):
        return "boolean"
    elif isinstance(value, bytes):
        return "bytes"
    elif isinstance(value, datetime):
        # Handle timestamps using Avro's logical type
        return {"type": "long", "logicalType": "timestamp-micros"}
    elif isinstance(value, str) and "T" in value:
        # Handle timestamps using Avro's logical type
        return {"type": "long", "logicalType": "timestamp-micros"}
    else:
        # For unknown types, assume string by default
        return "string"

# Function to generate the schema dynamically from the data
def generate_schema_from_data(data):
    if not data:
        raise ValueError("Data is empty, cannot generate schema.")

    # Extract columns from the first row of data (assuming all rows have the same structure)
    schema = []
    for column_name, value in data[0].items():
        avro_type = get_avro_type(value)
        schema.append({
            "name": column_name,
            "type": avro_type
        })
    return schema

# Function to save data to Avro format
def save_to_avro(data, output_file):
    # Generate the Avro schema from the input data
    avro_schema = {
        "type": "record",
        "name": "TableRecord",
        "fields": generate_schema_from_data(data)
    }

    # Parse the schema
    parsed_schema = avro.schema.parse(json.dumps(avro_schema))

    # Write the data to the Avro file
    with open(output_file, 'wb') as avro_file:
        writer = avro.datafile.DataFileWriter(avro_file, avro.io.DatumWriter(), parsed_schema)
        
        # Write each row in the data to the Avro file
        for row in data:
            avro_row = {}
            for column_name, value in row.items():
                avro_row[column_name] = value  # Directly append the data
            writer.append(avro_row)
        
        writer.close()
def create_partitioned_path(output_dir):
    now = datetime.now()
    year = now.strftime("%Y")
    month = now.strftime("%m")
    day = now.strftime("%d")
    hour = now.strftime("%H")
    partitioned_path = os.path.join(output_dir, f"year={year}", f"month={month}", f"day={day}", f"hour={hour}")
    os.makedirs(partitioned_path, exist_ok=True)
    return partitioned_path

# def main():
def generate_files(schema_file, num_rows,parent_folder, file_type, num_files, output_dir):

    config_file = r"config.ini"
    config = configparser.ConfigParser()
    config.read(config_file)

    # schema_file = config['DEFAULT']['SchemaFile']
    # num_rows = int(config['DEFAULT']['NumRows'])
    # file_type = config['DEFAULT']['FileType']
    # output_dir = config['DEFAULT']['OutputDir']
    # num_files = int(config['DEFAULT']['NumFiles'])
    out_file_list=[]
    schema = read_schema_from_csv(schema_file)

    partitioned_path = create_partitioned_path(output_dir)

    for i in range(num_files):
      data = generate_data(schema, num_rows,file_type)
      timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
      output_file = os.path.join(partitioned_path, f'{parent_folder}_{timestamp}_{i+1}.{file_type}')
      out_file_list.append(output_file)
      if file_type == 'csv':
          save_to_csv(data, output_file)
      elif file_type == 'parquet':
          save_to_parquet(data, output_file)
      elif file_type == 'avro':
          save_to_avro(data,  output_file)
      else:
          raise ValueError(f"Unsupported file type: {file_type}")
    return out_file_list

if __name__ == "__main__":
    main()

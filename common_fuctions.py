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

def generate_data(schema, num_rows, database_type='bigquery'):
    data = []

    for _ in range(num_rows):
        row = {}

        for column in schema:
            name = column['name'].lower()
            dtype = column['type'].upper()

            # Context-aware data generation
            if "name" in name:
                row[column['name']] = fake.name()
            elif "email" in name:
                row[column['name']] = fake.email()
            elif "phone" in name or "contact" in name:
                row[column['name']] = fake.phone_number()
            elif "address" in name:
                row[column['name']] = fake.address()
            elif "city" in name:
                row[column['name']] = fake.city()
            elif "state" in name:
                row[column['name']] = fake.state()
            elif "country" in name:
                row[column['name']] = fake.country()
            elif "zipcode" in name or "postal" in name:
                row[column['name']] = fake.zipcode()
            elif "date" in name and dtype == 'DATE':
                row[column['name']] = fake.date()
            elif "time" in name and dtype in ['TIME', 'DATETIME']:
                row[column['name']] = fake.iso8601()
            elif "price" in name or "amount" in name or dtype in ['NUMERIC', 'FLOAT']:
                row[column['name']] = round(random.uniform(1.0, 1000000.0), 2)
            elif "id" in name or dtype in ['INTEGER', 'INT64']:
                row[column['name']] = random.randint(1, 100000)
            elif dtype == 'BOOLEAN':
                row[column['name']] = random.choice([True, False])
            elif dtype == 'BYTES':
                row[column['name']] = fake.binary(length=10)
            elif dtype == 'GEOGRAPHY':
                row[column['name']] = f"POINT({random.uniform(-180.0, 180.0)} {random.uniform(-90.0, 90.0)})"
            elif dtype == 'BIGNUMERIC':
                row[column['name']] = round(random.uniform(1.0, 1000000000.0), 38)
            else:
                # Fallback for unspecified cases
                row[column['name']] = fake.word()

            # Database-specific adjustments
            if database_type == 'bigquery':
                if dtype in ['DATETIME', 'TIMESTAMP']:
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

def save_to_avro(data, schema, output_file):
    avro_schema = {
        "type": "record",
        "name": "TableRecord",
        "fields": [
            {"name": col['name'], "type": "string" if col['type'] in ['STRING', 'DATE', 'TIMESTAMP', 'DATETIME', 'TIME', 'GEOGRAPHY'] else "int" if col['type'] == 'INTEGER' else "float" if col['type'] in ['FLOAT', 'NUMERIC', 'BIGNUMERIC'] else "boolean" if col['type'] == 'BOOLEAN' else "bytes" if col['type'] == 'BYTES' else "null"}
            for col in schema
        ]
    }

    parsed_schema = avro.schema.parse(json.dumps(avro_schema))
    with open(output_file, 'wb') as avro_file:
        writer = avro.datafile.DataFileWriter(avro_file, avro.io.DatumWriter(), parsed_schema)
        for row in data:
            writer.append(row)
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
      data = generate_data(schema, num_rows)
      timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
      output_file = os.path.join(partitioned_path, f'{parent_folder}_{timestamp}_{i+1}.{file_type}')
      out_file_list.append(output_file)
      if file_type == 'csv':
          save_to_csv(data, output_file)
      elif file_type == 'parquet':
          save_to_parquet(data, output_file)
      elif file_type == 'avro':
          save_to_avro(data, schema, output_file)
      else:
          raise ValueError(f"Unsupported file type: {file_type}")
    return out_file_list

if __name__ == "__main__":
    main()

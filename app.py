import threading

from flask import Flask, render_template, request, send_file, jsonify
import os,time
import zipfile
from datetime import datetime
from common_fuctions import *

from flask import Flask, render_template, request
import pandas as pd
import json
import os
from werkzeug.utils import secure_filename
import pyarrow.parquet as pq
import fastavro
app = Flask(__name__)
from flask import Flask, render_template, request, jsonify
import pandas as pd
import sqlite3
import os

app = Flask(__name__)
UPLOAD_FOLDER = "uploads"
app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER

# Ensure uploads folder exists
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

@app.route("/editor", methods=["GET", "POST"])
def index3():
    if request.method == "POST":
        # Handle file upload
        if "file" in request.files:
            file = request.files["file"]
            if file.filename.endswith(".csv"):
                file_path = os.path.join(app.config["UPLOAD_FOLDER"], file.filename)
                file.save(file_path)
                return jsonify({"message": "File uploaded successfully!", "file_path": file_path})
            return jsonify({"error": "Only CSV files are allowed!"}), 400
    return render_template("editor.html")

@app.route("/execute_query", methods=["POST"])
def execute_query():
    data = request.get_json()
    query = data.get("query")
    file_path = data.get("file_path")

    try:
        # Load CSV into SQLite for SQL execution
        df = pd.read_csv(file_path)
        
        conn = sqlite3.connect(":memory:")
        df.to_sql("uploaded_data", conn, index=False, if_exists="replace")

        # Execute query
        result = pd.read_sql_query(query, conn)
        conn.close()

        # Return results as JSON
        return jsonify(result.to_dict(orient="records"))
    except Exception as e:
        return jsonify({"error": str(e)}), 400


# Update this to your main data generation logic
def generate_files1(schema_file, num_rows, file_type, num_files, output_dir):
    # Placeholder logic to generate files and save them
    # Use your previously defined functions here
    partitioned_path = create_partitioned_path(output_dir)
    file_paths = []
    for i in range(num_files):
        # Generate data and save files
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = os.path.join(partitioned_path, f'file_{timestamp}_{i+1}.{file_type}')
        # Save the generated file (e.g., save_to_csv)
        file_paths.append(output_file)
    return file_paths

@app.route("/")
def index():
    return render_template("index.html")  # Serve a styled HTML page

@app.route("/generate", methods=["POST"])
def generate():
    schema_file = request.files["schema_file"]
    parent_folder = request.form["parent_folder"]
    num_rows = int(request.form["num_rows"])
    file_type = request.form["file_type"]
    num_files = int(request.form["num_files"])
    output_dir = f"output/{parent_folder}"  # Example: Set your output directory here

    # Save schema file temporarily
    if not os.path.exists(f"uploads/{parent_folder}"):
        os.makedirs(f"uploads/{parent_folder}")
    schema_path = os.path.join(f"uploads/{parent_folder}", schema_file.filename)
    schema_file.save(schema_path)

    # Generate files
    file_paths = generate_files(schema_path, num_rows,parent_folder, file_type, num_files, output_dir)

    # Optionally zip the files for bulk download
    zip_path = os.path.join("output/", f"generated_files_{parent_folder}.zip")
    with zipfile.ZipFile(zip_path, 'w') as zipf:
        # for file_path in file_paths:
        #     zipf.write(os.path.abspath(file_path), os.path.basename(file_path))
        for root, dirs, files in os.walk(output_dir):
            for file in files:
                file_path = os.path.join(root, file)
                arcname = os.path.relpath(file_path, start=output_dir)
                zipf.write(file_path, arcname=arcname)
                os.remove(file_path) 



    return jsonify({"download_link": f"/download/{os.path.basename(zip_path)}"})

@app.route("/download/<filename>")
def download(filename):
    return send_file(os.path.join("output", filename), as_attachment=True)

@app.route("/download_schema")
def download1():
    return send_file(os.path.join("schema.csv"), as_attachment=True)

# Timer function to clean up files after 5 minutes
def cleanup_files():
    temp_dir="output"
    while True:
        # Wait for 5 minutes (300 seconds)
        time.sleep(300)
        
        # Clean up the files in the temp_dir
        for filename in os.listdir(temp_dir):
            file_path = os.path.join(temp_dir, filename)
            if os.path.isfile(file_path):
                try:
                    os.remove(file_path)
                    print(f"Deleted {file_path}")
                except Exception as e:
                    print(f"Error deleting {file_path}: {e}")
def start_cleanup_timer():
    cleanup_thread = threading.Thread(target=cleanup_files)
    cleanup_thread.daemon = True  # Daemonize the thread so it exits when the app shuts down
    cleanup_thread.start()

# #################  new page 
ALLOWED_EXTENSIONS = {'csv', 'json', 'txt', 'xlsx', 'avro', 'parquet'}

# Function to check allowed file extensions
def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def read_file(file_path, file_extension):
    if file_extension == 'csv':
        return pd.read_csv(file_path)
    elif file_extension == 'json':
        with open(file_path, 'r') as f:
            return json.load(f)
    elif file_extension == 'txt':
        with open(file_path, 'r') as f:
            return f.read()
    elif file_extension == 'xlsx':
        return pd.read_excel(file_path)  # Using pandas to read Excel files
    elif file_extension == 'avro':
        with open(file_path, 'rb') as f:
            reader = fastavro.reader(f)
            return [record for record in reader]
    elif file_extension == 'parquet':
        table = pq.read_table(file_path)
        return table.to_pandas()

@app.route('/reader', methods=['GET', 'POST'])
def index1():
    file_uploaded = False
    file_type = None
    data_html = None

    if request.method == 'POST':
        # Check if file is part of the form
        if 'file' not in request.files:
            return render_template('reader.html', file_uploaded=False)
        file = request.files['file']

        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            file_path = os.path.join('uploads', filename)

            # Save file to the 'uploads' folder
            file.save(file_path)

            # Identify file type and read the content
            file_extension = filename.rsplit('.', 1)[1].lower()
            file_type = file_extension.upper()

            data = read_file(file_path, file_extension)

            # Display file content in a readable format
            if file_extension in ['csv', 'xlsx', 'parquet']:
                data_html = data.head(1000).to_html(classes='table table-striped')
            else:
                data_html = json.dumps(data, indent=2)  # For JSON, Avro, TXT

            file_uploaded = True

    return render_template('reader.html', file_uploaded=file_uploaded, file_type=file_type, data_html=data_html)
# #################  new page 



if __name__ == "__main__":
    start_cleanup_timer()

    # app.run(debug=True)
    # Get the port from the environment variable, default to 5000 if not set
    port = int(os.environ.get('PORT', 5000))
    # Run Flask on 0.0.0.0 and the port specified by Render
    app.run(host='0.0.0.0', port=port)
import threading

from flask import Flask, render_template, request, send_file, jsonify
import os,time
import zipfile
from datetime import datetime
from common_fuctions import *
app = Flask(__name__)

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

if __name__ == "__main__":
    start_cleanup_timer()

    app.run(debug=True)

from io import StringIO
import os
from tempfile import NamedTemporaryFile
from turtle import pd
from fastapi import Depends, Form, HTTPException,UploadFile,File
import httpx
from sqlalchemy import text
import sys
sys.path.append('C:\\Users\\APRIYADARS1\\OneDrive - Rockwell Automation, Inc\\Desktop\\Project\\ADF_Backend')
from database import get_db_1, get_db_2,get_db_3
from sqlalchemy.orm import Session
import base64
import csv
import json
import pusher
from fastapi import BackgroundTasks
import time
from datetime import datetime, timedelta

pusher = pusher.Pusher(
  app_id='1879605',
  key='aeeb90d987e5e72bddbe',
  secret='543074e9650b9560798e',
  cluster='ap2',
  ssl=True
)

def get_db_by_id(id: int):
    if id == 1:
        return get_db_1
    else:
        return get_db_2
def getFileSource1(db: Session = Depends(get_db_1)):
    try:
        pusher.trigger("logs-channel", "log-event", {
            "message": f"Running node: Fetching data from source location",
            "status": "success",
            "label": "Connection Activity"
        })

        # Ensure that the FileStorage table exists
        db.execute(text("""
        IF OBJECT_ID('FileStorage', 'U') IS NULL
        BEGIN
            CREATE TABLE FileStorage (
                id INT IDENTITY PRIMARY KEY,
                filename VARCHAR(255),
                content VARBINARY(MAX),
                filetype VARCHAR(255),
                upload_time DATETIME
            );
        END
        """))

        files = db.execute(text("SELECT * FROM FileStorage")).fetchall()
        file_list = []
        for row in files:
            file_dict = dict(row._mapping)
            file_dict['content'] = base64.b64encode(file_dict['content']).decode('utf-8')
            file_list.append(file_dict)

        pusher.trigger("logs-channel", "log-event", {
            "message": f"Running node: Fetched data from source location",
            "status": "success",
            "label": "Connection Activity"
        })
        return file_list
    except Exception as e:
        pusher.trigger("logs-channel", "log-event", {
            "message": f"Error: {str(e)}",
            "status": "error",
            "label": "Connection Activity"
        })
        raise HTTPException(status_code=500, detail=str(e))
import base64

import os

ALLOWED_FILE_TYPES = [
    "text/csv", 
    "application/json", 
    "text/plain", 
    "application/xml", 
    "text/xml", 
    "application/x-ipynb+json",
    "application/octet-stream"  # Allow octet-stream if extension is .ipynb
]

def uploadFiles(file: UploadFile = File(...), db: Session = Depends(get_db_1)):
    try:
        
        pusher.trigger("logs-channel", "log-event", {
            "label":"Upload File Activity",
            "message": "Uploading file...",
            "status": "success"
        })

        file_type = file.content_type
        file_extension = os.path.splitext(file.filename)[1].lower()

        # If file type is 'application/octet-stream', ensure it is a .ipynb file
        if file_type == "application/octet-stream" and file_extension != ".ipynb":
            pusher.trigger("logs-channel", "log-event", {
                "label": "Upload File Activity",
                "message": f"Invalid file type '{file_type}' for file '{file.filename}'. Only .ipynb files are allowed for this type.",
                "status": "error"
            })
            raise HTTPException(
                status_code=400, 
                detail=f"Invalid file type '{file_type}' for file '{file.filename}'. Only .ipynb files are allowed for this type."
            )

        # Check the file type or extension
        if file_type not in ALLOWED_FILE_TYPES and file_extension != ".ipynb":
            pusher.trigger("logs-channel", "log-event", {
                "label": "Upload File Activity",
                "message": f"Invalid file type '{file_type}'. Only CSV, JSON, TXT, XML, and IPYNB files are allowed.",
                "status": "error"
            })
            raise HTTPException(
                status_code=400, 
                detail=f"Invalid file type '{file_type}'. Only CSV, JSON, TXT, XML, and IPYNB files are allowed."
            )

        # Read the file content
        file_content = file.file.read()
        db.execute(text("""
        IF OBJECT_ID('FileStorage', 'U') IS NULL
        BEGIN
            CREATE TABLE FileStorage (
                id INT IDENTITY PRIMARY KEY,
                filename VARCHAR(255),
                content VARBINARY(MAX),
                filetype VARCHAR(255),
                upload_time DATETIME
            );
        END
        """))

        # Check for duplicate file in the database
        countDuplicate = db.execute(
            text("SELECT COUNT(*) FROM FileStorage WHERE filename = :filename"), 
            {"filename": file.filename}
        ).fetchone()[0]

        if countDuplicate > 0:
            pusher.trigger("logs-channel", "log-event", {
                "label": "Upload File Activity",
                "message": "File already exists",
                "status": "error"
            })
            raise HTTPException(status_code=400, detail="File already exists")

        # Ensure that the FileStorage table exists before inserting
        
        db.commit()

        current_time = datetime.utcnow()

        # Insert the file into the database as binary content
        db.execute(text("""
        INSERT INTO FileStorage (filename, content, filetype, upload_time) 
        VALUES (:filename, :content, :filetype, :upload_time)
        """), {
            "filename": file.filename, 
            "content": file_content,  
            "filetype": file_type, 
            "upload_time": current_time
        })

        db.commit()
        pusher.trigger("logs-channel", "log-event", {
            "label":"Upload File Activity",
            "message": "File uploaded successfully",
            "status": "success"
        })
        return {"message": "File uploaded successfully"}

    except Exception as e:
        pusher.trigger("logs-channel", "log-event", {
            "label":"Upload File Activity",
            "message": f"Error: {str(e)}",
            "status": "error"   
        })
        db.rollback()
        raise HTTPException(status_code=400, detail=str(e))
def checkFileCurrupt(file: UploadFile = File(...)):
    try:
        pusher.trigger("logs-channel", "log-event", {
            "label":"Currupt File Activity",
            "message": "Checking file integrity...",
            "status": "success"
        })
       
        file.file.seek(0)  
        file_content = file.file.read()
       
        file.file.seek(0)
       
        pusher.trigger("logs-channel", "log-event", {
            "label":"Currupt File Activity",
            "message": "File is not corrupted and readable",
            "status": "success"
        })
        return False  
    except Exception:
        pusher.trigger("logs-channel", "log-event", {
            "label":"Currupt File Activity",
            "message": "File is corrupted or unreadable",
            "status": "error"
        })
        return True 



import xml.etree.ElementTree as ET




def copyDataSchedule( interval: int, background_tasks: BackgroundTasks, db1: Session = Depends(get_db_2), db2: Session = Depends(get_db_3)):
    pusher.trigger("logs-channel", "log-event", {
        "message": "Scheduled copy data task started",
        "status": "success"
    })
    def copy_data_task():
        while True:
            try:
                temp_data = db1.execute(text("SELECT * FROM FileStorage")).fetchall()
                if temp_data:
                    for row in temp_data:
                        checkExist = db2.execute(text("SELECT COUNT(*) FROM FileStorage WHERE filename = :filename"), {"filename": row.filename}).fetchone()[0]
                        if checkExist > 0:
                            raise HTTPException(status_code=400, detail="File already exists")
                        db2.execute(text("""
                            INSERT INTO FileStorage (filename, content, filetype)
                            VALUES (:filename, :content, :filetype)
                        """), {
                            "filename": row.filename,
                            "content": row.content,
                            "filetype": row.filetype
                        })
                    db2.commit()
                time.sleep(interval)
            except Exception as e:
                pusher.trigger("logs-channel", "log-event", {
                    "message": f"Error: {str(e)}",
                    "status": "error"
                })
                db2.rollback()
                raise HTTPException(status_code=400, detail=str(e))

    background_tasks.add_task(copy_data_task)
   
    return {"message": "Scheduled copy data task started"}


def csv_to_json(csv_content):
    """Convert CSV content to JSON format."""
    pusher.trigger("logs-channel", "log-event", {
        "message": "Converting CSV to JSON",
        "status": "success"
    })
    csv_file = StringIO(csv_content)
    reader = csv.DictReader(csv_file)
    json_data = json.dumps([row for row in reader])
    pusher.trigger("logs-channel", "log-event", {
        "message": "CSV converted to JSON",
        "status": "success"
    })
    return json_data

def json_to_csv(json_content):
    """Convert JSON content to CSV format."""
    pusher.trigger("logs-channel", "log-event", {
        "message": "Converting JSON to CSV",
        "status": "success"
    })
    json_data = json.loads(json_content)
    output = StringIO()
    writer = csv.DictWriter(output, fieldnames=json_data[0].keys())
    writer.writeheader()
    writer.writerows(json_data)
    pusher.trigger("logs-channel", "log-event", {
        "message": "Converted JSON to CSV",
        "status": "success"
    })
    return output.getvalue()

def copy_data(
    source: int = Form(...),  
    filename: str = Form(...),  
    filetype: str = Form(...), 
    file: UploadFile = File(...), 
    db1: Session = Depends(get_db_2), 
    db2: Session = Depends(get_db_3)
):
    pusher.trigger("logs-channel", "log-event", {
        "message": "Copying data...",
        "status": "success",
        "label": "Copy Data Activity"
    })
    # Choose the database based on the source
    db = db1 if source == 1 else db2

    # Read the file content
    file_content = file.file.read()

    # Process the file data and store it in the database
    result = copyData(filename, file_content, filetype, db)
    pusher.trigger("logs-channel", "log-event", {
        "message": "Data copied successfully",
        "status": "success",
        "label": "Copy Data Activity"
    })

    return result

def copyData(filename: str, content: bytes, filetype: str, db: Session):
    try:        
        # Check if the file already exists in the database
        pusher.trigger("logs-channel", "log-event", {
            "label": "Copy Data Activity",
            "message": "Checking if file already exists...",
            "status": "success"
        })
        checkExist = db.execute(
            text("SELECT COUNT(*) FROM FileStorage WHERE filename = :filename"),
            {"filename": filename}
        ).fetchone()[0]

        if checkExist > 0:
            pusher.trigger("logs-channel", "log-event", {
                "label": "Copy Data Activity",
                "message": "File already exists",
                "status": "error"
            })
            raise HTTPException(status_code=400, detail="File already exists")
        pusher.trigger("logs-channel", "log-event", {
            "label": "Copy Data Activity",
            "message": f"Inserting {filename} data to new location...",
            "status": "success"
        })
        # Insert the file data into the database
        db.execute(text("""
            INSERT INTO FileStorage (filename, content, filetype)
            VALUES (:filename, :content, :filetype)
        """), {
            "filename": filename,
            "content": content,
            "filetype": filetype
        })
        pusher.trigger("logs-channel", "log-event", {
            "label": "Copy Data Activity",
            "message": f"Data copied successfully ",
            "status": "success"
        })

        db.commit()

        return {"message": f"Data copied successfully for filename {filename}"}

    except Exception as e:
        pusher.trigger("logs-channel", "log-event", {
            "label": "Copy Data Activity",
            "message": f"Error: {str(e)}",
            "status": "error"
        })
        db.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    



def csv_to_json(csv_content):
    """Convert CSV content to JSON format."""
    csv_file = StringIO(csv_content)
    reader = csv.DictReader(csv_file)
    return json.dumps([row for row in reader])

def csv_to_txt(csv_content):
    """Convert CSV content to plain text."""
    return csv_content  # CSV is already in a textual format, so we return it as-is

def csv_to_xml(csv_content):
    """Convert CSV content to XML format."""
    csv_file = StringIO(csv_content)
    reader = csv.DictReader(csv_file)
    root = ET.Element("root")
    for row in reader:
        item = ET.SubElement(root, "item")
        for key, value in row.items():
            child = ET.SubElement(item, key)
            child.text = value
    return ET.tostring(root).decode()

def json_to_csv(json_content):
    """Convert JSON content to CSV format."""
    json_data = json.loads(json_content)
    output = StringIO()
    writer = csv.DictWriter(output, fieldnames=json_data[0].keys())
    writer.writeheader()
    writer.writerows(json_data)
    return output.getvalue()

def json_to_txt(json_content):
    """Convert JSON content to plain text."""
    return json_content  # JSON is already text format

def json_to_xml(json_content):
    """Convert JSON content to XML format."""
    json_data = json.loads(json_content)
    root = ET.Element("root")
    for item in json_data:
        item_elem = ET.SubElement(root, "item")
        for key, value in item.items():
            child = ET.SubElement(item_elem, key)
            child.text = str(value)
    return ET.tostring(root).decode()

def txt_to_json(txt_content):
    """Convert plain text content to JSON format."""
    str_obj = txt_content.decode('utf-8')

    json_obj = json.loads(str_obj)
    return json_obj

def txt_to_csv(txt_content):
    """Convert plain text content to CSV format."""
    output = StringIO()
    output.write(txt_content)
    return output.getvalue()

def txt_to_xml(txt_content):
    """Convert plain text content to XML format."""
    root = ET.Element("root")
    content_elem = ET.SubElement(root, "content")
    content_elem.text = txt_content
    return ET.tostring(root).decode()

def xml_to_json(xml_content):
    """Convert XML content to JSON format."""
    root = ET.fromstring(xml_content)
    result = []
    for item in root.findall("item"):
        item_data = {child.tag: child.text for child in item}
        result.append(item_data)
    return json.dumps(result)

def xml_to_csv(xml_content):
    """Convert XML content to CSV format."""
    json_data = json.loads(xml_to_json(xml_content))
    return json_to_csv(json.dumps(json_data))

def xml_to_txt(xml_content):
    """Convert XML content to plain text."""
    root = ET.fromstring(xml_content)
    output = []
    for item in root.findall("item"):
        for child in item:
            output.append(f"{child.tag}: {child.text}")
    return "\n".join(output)

# Main function for fetching data and converting formats


def getDataWithFormatChange(body, db1: Session = Depends(get_db_2)):
    try:
        pusher.trigger("logs-channel", "log-event", {
            "label":"Format Activity",
            "message": f"Running node: Fetching data from source database for id {body.id}",
            "status": "success"
        })
        # Fetch data from the source database
        temp_data = db1.execute(text("SELECT * FROM FileStorage WHERE id = :id").bindparams(id=body.id)).fetchall()

        if not temp_data:
            pusher.trigger("logs-channel", "log-event", {
                "label":"Format Activity",
                "message": f"No data found for id {body.id}",
                "status": "error"
            })
            raise HTTPException(status_code=404, detail="No data found for the given id")

        result = []
        pusher.trigger("logs-channel", "log-event", {
            "label":"Format Activity",
            "message": f"Data fetched successfully for id {body.id}",
            "status": "success"
        })
        # Iterate over the fetched rows and perform format conversion if needed
        for row in temp_data:
            content = row.content
            filetype = row.filetype
            if isinstance(content, bytes):
                content=content.decode('utf-8')
            target_format =body.format
            # Check if format conversion is needed
                # Convert based on current filetype
            if filetype == 'text/csv':
                if target_format == 'json':
                    content = csv_to_json(content)
                elif target_format == 'txt':
                    content = csv_to_txt(content)
                elif target_format == 'xml':
                    content = csv_to_xml(content)
            elif filetype == 'application/json':
                if target_format == 'csv':
                    content = json_to_csv(content)
                elif target_format == 'txt':
                    content = json_to_txt(content)
                elif target_format == 'xml':
                    content = json_to_xml(content)
            elif filetype == 'text/plain':
                if target_format == 'csv':
                    content = txt_to_csv(row.content)
                elif target_format == 'json':
                    content = txt_to_json(row.content)
                elif target_format == 'xml':
                    content = txt_to_xml(row.content)
            elif filetype == "application/xml" or filetype == "text/xml":
                if target_format == 'json':
                    content = xml_to_json(row.content)
                elif target_format == 'txt':
                    content = xml_to_txt(row.content)
                elif target_format == 'csv':
                    content = xml_to_csv(row.content)
            else:
                pusher.trigger("logs-channel", "log-event", {
                    "label":"Format Activity",
                    "message": f"Unsupported format conversion",
                    "status": "error"})
                raise HTTPException(status_code=400, detail="Unsupported format conversion")
            

            with NamedTemporaryFile(delete=False, suffix=f".{body.format}") as temp_file:
                temp_file.write(content.encode('utf-8'))
                temp_file_path = temp_file.name

            # Read the file content
            with open(temp_file_path, 'rb') as file:
                file_content = file.read()
              
            # Clean up the temporary file
            os.remove(temp_file_path)
            pusher.trigger("logs-channel", "log-event", {
                "label":"Format Activity",
                "message": f"Converted data to {body.format} format",
                "status": "success"
            })

            # Prepare the result
            result.append({
                "filename": body.fileName.split('.')[0] + f".{body.format}",
                "content": content,
                "filetype": body.format == 'xml' and 'application/xml' or body.format == 'json' and 'application/json' or body.format == 'csv' and 'text/csv' or 'text/plain',
                "content1": file_content
            })

        return {"message": f"Data retrieved successfully for id {body.id}", "data": result}

    except Exception as e:
        pusher.trigger("logs-channel", "log-event", {
            "label":"Format Activity",
            "message": f"Error: {str(e)}",
            "status": "error"
        })
        raise HTTPException(status_code=400, detail=str(e))

async def extract_unique(db1: Session = Depends(get_db_2), db2: Session = Depends(get_db_3)):
    file = db1.execute(text("SELECT filename FROM FileStorage")).fetchall()
    files_to_copy = db2.execute(text("SELECT filename FROM FileStorage")).fetchall()
    
    file_set = {row.filename for row in file}
    files_to_copy_set = {row.filename for row in files_to_copy}
    
    unique_files = file_set - files_to_copy_set
    
    return list(unique_files)

async def eventBased(dbs: Session, dbd: Session):
    try:
        pusher.trigger("logs-channel", "log-event", {
            "message": "Copying data...",
            "status": "success",
            "label": "Event Based Activity"
        })

        unique_files = await extract_unique(dbs, dbd)
        
        if not unique_files:
            raise HTTPException(status_code=404, detail="No unique files found to copy")

        # Fetch the first unique file ordered by upload_time desc
        query = text("SELECT * FROM FileStorage WHERE filename = :filename ORDER BY upload_time DESC")
        file = dbs.execute(query, {"filename": unique_files[0]}).fetchone()

        if not file:
            raise HTTPException(status_code=404, detail="No data found for the given filename")

        checkExist = dbd.execute(text("SELECT COUNT(*) FROM FileStorage WHERE filename = :filename"), {"filename": file.filename}).fetchone()[0]
        if checkExist > 0:
            raise HTTPException(status_code=400, detail="File already exists")

        dbd.execute(text("""
            INSERT INTO FileStorage (filename, content, filetype, upload_time)
            VALUES (:filename, :content, :filetype, :upload_time)
        """), {
            "filename": file.filename,
            "content": file.content,
            "filetype": file.filetype,
            "upload_time": datetime.utcnow()
        })

        dbd.commit()
        pusher.trigger("logs-channel", "log-event", {
            "status": "success",
            "label": "Event Based Activity",
            "message": "Data copied successfully"
        })
        return {"message": "Data copied successfully"}
    except Exception as e:
        pusher.trigger("logs-channel", "log-event", {
            "message": f"Error: {str(e)}",
            "status": "error",
            "label": "Event Based Activity"
        })
        dbd.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    



async def tumblingWindow(intervals: int, dbs: Session, dbd: Session):
    try:
        pusher.trigger("logs-channel", "log-event", {
            "message": "Copying data...",
            "status": "success",
            "label": "Tumbling Window Activity"
        })
        uniqueList = await extract_unique(dbs, dbd)
        if len(uniqueList) == 0:
            pusher.trigger("logs-channel", "log-event", {
                "message": f"{intervals - len(uniqueList)} files to be uploaded before tumbling",
                "status": "error",
                "label": "Tumbling Window Activity"
            })
            return {"message": f"{intervals - len(uniqueList)} files to be uploaded before tumbling"}
        placeholders = ', '.join([':filename' + str(i) for i in range(len(uniqueList))])
        query = f"SELECT * FROM FileStorage WHERE filename IN ({placeholders})"
        params = {f'filename{i}': filename for i, filename in enumerate(uniqueList)}
        
        file = dbs.execute(text(query), params).fetchall()
        # print(file)
        if len(file) < intervals:
            pusher.trigger("logs-channel", "log-event", {
                "message": f"{intervals - len(file)} files to be uploaded before tumbling",
                "status": "error",
                "label": "Tumbling Window Activity"
            })
            return {"message": f"{intervals - len(file)} files to be uploaded before tumbling"}
        
        if not file:
            raise HTTPException(status_code=404, detail="No data found for the given id")
        
        for row in file:
            checkExist = dbd.execute(text("SELECT COUNT(*) FROM FileStorage WHERE filename = :filename"), {"filename": row.filename}).fetchone()
            if checkExist[0] > 0:
                raise HTTPException(status_code=400, detail="File already exists")
            
            dbd.execute(text("""
                INSERT INTO FileStorage (filename, content, filetype, upload_time)
                VALUES (:filename, :content, :filetype, :upload_time)
            """), {
                "filename": row.filename,
                "content": row.content,
                "filetype": row.filetype,
                "upload_time": datetime.utcnow()
            })
        
        dbd.commit()
        pusher.trigger("logs-channel", "log-event", {
            "message": "Files copied successfully",
            "status": "success",
            "label": "Tumbling Window Activity"
        })
        return {"message": f"Files copied successfully"}
    except Exception as e:
        pusher.trigger("logs-channel", "log-event", {
            "status": "error",
            "message": f"Error: {str(e)}",
            "label": "Tumbling Window Activity"
        })
        dbd.rollback()
        raise HTTPException(status_code=400, detail=str(e))


def fetch_new_records(tablename: str, target_table_name: str, primary_key_column: str, last_load_time: datetime, db: Session):
    print(tablename, last_load_time, target_table_name, primary_key_column)
    adjusted_last_load_time = last_load_time + timedelta(seconds=1)
    records = db.execute(text(f"""
        SELECT src.*
        FROM {tablename} src
        LEFT OUTER JOIN {target_table_name} tgt
        ON src.{primary_key_column} = tgt.{primary_key_column}
        WHERE src.UpdatedOn > :adjusted_last_load_time
    """), {'adjusted_last_load_time': adjusted_last_load_time}).fetchall()

    db.execute(text("""
        INSERT INTO LoadTracking (table_name, last_load_time, message)
        VALUES (:table_name, :last_load_time, :message)
    """), {
        'table_name': target_table_name, 
        'last_load_time': datetime.now() + timedelta(seconds=1),  # Add extra 1 second
        'message': "made record inactive"
    })

    if records is None:
        records = []

    # Print the fetched records
    for record in records:
        print("Fetched record: ", dict(record._mapping))

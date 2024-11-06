
from contextlib import redirect_stderr, redirect_stdout
import io
import time
from fastapi import BackgroundTasks, FastAPI, Depends, Form, UploadFile, File, HTTPException,Request
from typing import List
from fastapi.middleware.cors import CORSMiddleware
import nbformat
from sqlalchemy.orm import Session
from sqlalchemy import text
from .services.activity.copy.main import eventBased, getFileSource1, uploadFiles , copyData  ,getDataWithFormatChange ,tumblingWindow
from .database import get_db_1, get_db_2, get_db_3, insert_to_mongo, setup_databases
from pydantic import BaseModel
from datetime import timezone
import logging
import httpx
from enum import Enum
from pydantic import constr
import traceback
import pusher
from datetime import datetime
import base64
from pydantic import BaseModel
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import PolynomialFeatures
from sklearn.svm import SVR
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, r2_score
import matplotlib.pyplot as plt
from io import BytesIO
from fastapi.responses import JSONResponse

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

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


@app.get("/getfileSource1/{id}")
def getupload_fileSource1(id: int, db1: Session = Depends(get_db_2),db2: Session = Depends(get_db_3)):
    pusher.trigger("logs-channel", "log-event", {
        "message": f"Running node: Connection Node",
        "status": "success"
    })

    if(id==1):
        file_list = getFileSource1( db1)
    else:
        file_list = getFileSource1( db2)
    return file_list

@app.post("/fileSourceUrl/")
def getupload_fileSource1(s: str = Form(...), db1: Session = Depends(get_db_2), db2: Session = Depends(get_db_3)):
    pusher.trigger("logs-channel", "log-event", {
        "message": f"Running node: Connection Node",
        "status": "success"
    })
    SessionLocal = setup_databases(s)
    db3 = SessionLocal()

    try:
        file_list = getFileSource1(db3)
        return file_list
    finally:
        db3.close()


@app.post("/upload-file/{id}")
def upload_file(id: int, file: UploadFile = File(...), db2: Session = Depends(get_db_2), db3: Session = Depends(get_db_3)):
    if id == 1:
        file = uploadFiles(file, db2)
    else:
        file = uploadFiles(file, db3)
        
    return file

class OperationType(str, Enum):
    one_time = "one_time"
    schedule = "schedule"
    event_change = "event_change"
    tumbling_window = "tumbling_window"

class CopyData(BaseModel):                         
    source: int
    filename: str
    filetype: str
    content: str
    
@app.post("/copy-data/")
def copy_data(
    source: int = Form(...),  
    filename: str = Form(...), 
    filetype: str = Form(...),  
    file: UploadFile = File(...), 
    url: str = Form(...), 
    db1: Session = Depends(get_db_2), 
    db2: Session = Depends(get_db_3)
):
    
 
    if len(url) > 0:
        SessionLocal = setup_databases(url)
        db = SessionLocal()
        try:
            file_content = file.file.read()
            result = copyData(filename, file_content, filetype, db)
            return result
        finally:
             db.close()
    # Choose the database based on the source
    db = db1 if source == 1 else db2
    # Read the file content
    file_content = file.file.read()
    # Process the file data and store it in the database
    result = copyData(filename, file_content, filetype, db)

    return result


class FormatFile(BaseModel):
    id: int                              
    source: int
    format: str
    fileName: str 
@app.post("/FileConvert/")
async def file_convert(body: FormatFile, db1: Session = Depends(get_db_2), db2: Session = Depends(get_db_3)):
    try:
        res = getDataWithFormatChange(body,db1)
        return res
    except Exception as e:
        
        raise HTTPException(status_code=400, detail=str(e))

    

logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)

def get_temp_data(db1: Session = Depends(get_db_2), db2: Session = Depends(get_db_3)):
    try:
        files_db1 = db1.execute(text("SELECT * FROM FileStorage")).fetchall()
        temp_data = []
        print(temp_data)
        for row in files_db1:
            check_exist = db2.execute(text("SELECT COUNT(*) FROM FileStorage WHERE filename = :filename"), {"filename": row.filename}).fetchone()[0]
            if check_exist == 0:
                temp_data.append(row)
        return temp_data
    except Exception as e:
        logger.error("Error in get_temp_data: %s", str(e))
        return []
def copy_data_task(temp_data: List, interval, db2: Session = Depends(get_db_2)):
    try:
        print("copy_data_task")
        if temp_data:
            for row in temp_data:
                db2.execute(text("""
                    INSERT INTO FileStorage (filename, content, filetype)
                    VALUES (:filename, :content, :filetype)
                """), {
                    "filename": row.filename,
                    "content": row.content,
                    "filetype": row.filetype
                })
                logger.info("Record copied from db1 to db2: %s", row.filename)
            db2.commit()
        time.sleep(interval)
    except Exception as e:
        db2.rollback()
        logger.error("Error in copy_data_task: %s", str(e))

@app.post("/startCopyDataTask/", response_model=None)
def start_copy_data_task(background_tasks: BackgroundTasks, db1: Session = Depends(get_db_2), db2: Session = Depends(get_db_3), interval: int = 1000):
    temp_data = get_temp_data(db1, db2)

    background_tasks.add_task(copy_data_task, temp_data, interval, db2)
    return {"message": "Scheduled copy data task started"}



class ExecuteApiRequest(BaseModel):
    title: str
    url: str
    method: str = "GET"
    headers: dict = {}
    data: dict = None
@app.post("/executeApi")
async def execute_api(body: ExecuteApiRequest):
    try:
        # Extract data from the request body
        url = body.url
        method = body.method.upper()  # Ensure method is uppercase
        headers = body.headers
        data = body.data
        pusher.trigger("logs-channel", "log-event", {
            "label" : "API Activity",
            "message": f"Executing API request: {method} {url}",
            "status": "success"
        })
        # Validate the HTTP method
        if method not in ["GET", "POST", "PUT", "DELETE"]:
            raise HTTPException(status_code=405, detail="HTTP method not supported")

        # Create an HTTP client to execute the request
        async with httpx.AsyncClient() as client:
            if method == 'GET':
                response = await client.get(url, headers=headers)
            elif method == 'POST':
                response = await client.post(url, headers=headers, json=data)
            elif method == 'PUT':
                response = await client.put(url, headers=headers, json=data)
            elif method == 'DELETE':
                response = await client.delete(url, headers=headers)
                # Prepare the result
        print(response.json())
        pusher.trigger("logs-channel", "log-event", {
            "label" : "API Activity",
            "message": f"Request successful: {response.status_code}",
            "status": "success"
        })
        result = {
                    "filename": body.title.split('.')[0] + f".json",
                    "content":  response.json(),
                    "filetype": 'application/json'
        }
        return {"message": "Data retrieved successfully", "data": result}

    except httpx.RequestError as e:
        pusher.trigger("logs-channel", "log-event", {
            "label" : "API Activity",
            "message": f"Request failed: {str(e)}",
            "status": "error"
        })
        raise HTTPException(status_code=500, detail=f"Request failed: {str(e)}")
    except Exception as e:
        pusher.trigger("logs-channel", "log-event", {
            "label" : "API Activity",
            "message": f"Error encountered during execution: {str(e)}",
            "status": "error"
        })
        raise HTTPException(status_code=400, detail=str(e))
    


def run_code_cell(code, cell_position):
    try:
        # Create a string buffer to capture output
        output_buffer = io.StringIO()
        error_buffer = io.StringIO()
        
        # Redirect stdout and stderr to the buffers
        with redirect_stdout(output_buffer), redirect_stderr(error_buffer):
            exec(code)
        
        # Retrieve the output and error messages
        output = output_buffer.getvalue()
        errors = error_buffer.getvalue()
        
        if errors:
            raise Exception(errors)
        
        return {
            "success": True,
            "cell_position": cell_position,
            "output": output
        }
    
    except SyntaxError as e:
        # Handle syntax errors explicitly
        return {
            "success": False,
            "cell_position": cell_position,
            "error": {
                "type": "SyntaxError",
                "message": str(e),
                "traceback": f"Line {e.lineno}: {e.text.strip()}",
            }
        }
    
    except Exception as e:
        # Handle all other exceptions and capture traceback as string
        pusher.trigger("logs-channel", "log-event", {
            "label": "Jupyter Notebook Activity",
            "message": f"Error encountered during execution",
            "detail": {
                "type": type(e).__name__,
                "message": str(e),
                "traceback": traceback.format_exc()
            },
            "status": "error"
        })
        error_traceback = traceback.format_exc()  # Format the full traceback as a string
        return {
            "success": False,
            "cell_position": cell_position,
            "error": {
                "type": type(e).__name__,
                "message": str(e),
                "traceback": error_traceback,  # Store traceback as a string
            }
        }

@app.post("/compileNotebook/")
async def compile_notebook(file: UploadFile = File(...)):

    try:
        pusher.trigger("logs-channel", "log-event", {
            "label":"Jupyter Notebook Activity",
            "message": f"Running node: Compile Notebook",
            "status": "success"
            })
        # Ensure the uploaded file is a .ipynb file
        if not file.filename.endswith(".ipynb"):
            pusher.trigger("logs-channel", "log-event", {
                "label":"Jupyter Notebook Activity",
                "message": f"File must be a Jupyter Notebook (.ipynb)",
                "status": "error"})
            raise HTTPException(status_code=400, detail="File must be a Jupyter Notebook (.ipynb)")

        if file.spool_max_size > 5 * 1024 * 1024:
            pusher.trigger("logs-channel", "log-event", {
                "label": "Jupyter Notebook Activity",
                "message": "File size exceeds 5MB limit",
                "status": "error"
            })
            raise HTTPException(status_code=400, detail="File size exceeds 5MB limit")
        # Read the notebook
        content = await file.read()
        notebook = nbformat.reads(content.decode("utf-8"), as_version=nbformat.NO_CONVERT)

        if notebook["nbformat"] < 4:
            pusher.trigger("logs-channel", "log-event", {
                "label":"Jupyter Notebook Activity",
                "message": f"Unsupported notebook format. Use a format >= 4.",
                "status": "error"})   
            raise HTTPException(status_code=400, detail="Unsupported notebook format. Use a format >= 4.")
        

        pusher.trigger("logs-channel", "log-event", {
            "label":"Jupyter Notebook Activity",
            "message": f"Notebook loaded successfully",
            "status": "success"})
        # Extract and run code cells
        errors = []
        last_successful_cell = None
        
        for index, cell in enumerate(notebook.cells):
            if cell.cell_type == "code":
                code = cell.source 
                result = run_code_cell(code, index + 1)  

                if not result["success"]:
                    # Return structured error details along with the last successful cell
                    return {
                        "label":"Jupyter Notebook Activity",
                        "message": "Error encountered during execution",
                        "last_successful_cell": last_successful_cell,
                        "error_details": result["error"]
                    }
                else:
                    last_successful_cell = result
        
        pusher.trigger("logs-channel", "log-event", {
            "label":"Jupyter Notebook Activity",
            "message": f"Notebook ran successfully, no errors encountered.",
            "status":"success"})

        # If no errors, return the last successful cell
        return {
            "message": "Notebook ran successfully, no errors encountered.",
            "last_successful_cell": last_successful_cell
        }

    except Exception as e:
        pusher.trigger("logs-channel", "log-event", {
            "message": f"Error processing notebook",
            "detail": {
                "message": "Error processing notebook",
                "error_type": type(e).__name__,
                "error_message": str(e),
                "traceback": error_traceback
            }
        })
        error_traceback = traceback.format_exc()
        # Return structured error response
        raise HTTPException(
            status_code=500,
            detail={
                "message": "Error processing notebook",
                "error_type": type(e).__name__,
                "error_message": str(e),
                "traceback": error_traceback
            }
        )

class Log(BaseModel):
    message: str
    status: str
    label: str 
    timestamp: datetime = datetime.now()

@app.post("/catching-logs/")
async def logs(req: Log, db: Session = Depends(get_db_2)):
    try:
        # Ensure the logs table exists in SQL Server
        db.execute(text("""
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'logs')
            CREATE TABLE logs (
                id INT IDENTITY(1,1) PRIMARY KEY,
                message TEXT NOT NULL,
                status TEXT NOT NULL,
                label TEXT NOT NULL,
                timestamp DATETIME NOT NULL
            )
        """))
        
        # Check if the log entry already exists
        last_log_id = db.execute(text("SELECT MAX(id) FROM logs")).fetchone()[0]
        last_log = db.execute(text("SELECT * FROM logs WHERE id = :id"), {"id": last_log_id}).fetchone()

        if last_log and last_log.message == req.message and last_log.status == req.status and last_log.label == req.label:
            return {"message": "Log already exists"}

        # Insert the log entry (without the extra comma)
        db.execute(text("""
            INSERT INTO logs (message, status, label, timestamp)
            VALUES (:message, :status, :label, :timestamp)
        """), {
            "message": req.message,
            "status": req.status,
            "label": req.label,
            "timestamp": req.timestamp
        })

        convert = {
            "Username": "Akarsh",
            "Logid": db.execute(text("SELECT MAX(id) FROM logs")).fetchone()[0],
            "Timestamp": req.timestamp.astimezone(timezone.utc),
            "Values": {
                "iserrorlog": 1 if req.status == "error" else 0,
                "Message": req.message,
                "Label": req.label
            }
        }
        insert_to_mongo(convert)
        db.commit()
        return {"message": "Log added successfully"}
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=400, detail=str(e))
class Schedule(BaseModel):
    source:int
    destination:int
    label: str
    interval: int = None
    schedular: int = None

@app.post("/scheduling")
async def schedule_task(req:Schedule,db1: Session = Depends(get_db_2), db2: Session = Depends(get_db_3)):
    try:
        if req.source==1:
            dbs=db1
        else:
            dbs=db2
        if req.destination==1:
            dbd=db1
        else:
            dbd=db2
        if req.label == 'Event Based':
            response = await eventBased(dbs,dbd)
        elif req.label == 'Tumbling Window':
            response = await tumblingWindow(req.interval,dbs,dbd)
        elif req.label == 'Scheduler':
            response = 'To be implemented'
        return response
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


# def run_regression_models(data, target_column):
#     # Drop non-numeric columns
#     data = data.select_dtypes(include=['float64', 'int64'])
#     if target_column not in data.columns:
#         raise ValueError(f"Target column '{target_column}' is not numeric or not found in data")

#     X = data.drop(target_column, axis=1)
#     y = data[target_column]

#     results = {}
#     X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

#     # 1. Simple Linear Regression
#     linear_regressor = LinearRegression()
#     linear_regressor.fit(X_train, y_train)
#     y_pred = linear_regressor.predict(X_test)
#     results["Linear Regression"] = {
#         "MSE": mean_squared_error(y_test, y_pred),
#         "R2 Score": r2_score(y_test, y_pred)
#     }

#     # 2. Polynomial Regression
#     poly_features = PolynomialFeatures(degree=2)
#     X_poly = poly_features.fit_transform(X_train)
#     poly_regressor = LinearRegression()
#     poly_regressor.fit(X_poly, y_train)
#     y_pred_poly = poly_regressor.predict(poly_features.transform(X_test))
#     results["Polynomial Regression"] = {
#         "MSE": mean_squared_error(y_test, y_pred_poly),
#         "R2 Score": r2_score(y_test, y_pred_poly)
#     }

#     # 3. Support Vector Regression
#     svr_regressor = SVR(kernel='rbf')
#     svr_regressor.fit(X_train, y_train)
#     y_pred_svr = svr_regressor.predict(X_test)
#     results["Support Vector Regression"] = {
#         "MSE": mean_squared_error(y_test, y_pred_svr),
#         "R2 Score": r2_score(y_test, y_pred_svr)
#     }

#     # 4. Decision Tree Regression
#     tree_regressor = DecisionTreeRegressor(random_state=42)
#     tree_regressor.fit(X_train, y_train)
#     y_pred_tree = tree_regressor.predict(X_test)
#     results["Decision Tree Regression"] = {
#         "MSE": mean_squared_error(y_test, y_pred_tree),
#         "R2 Score": r2_score(y_test, y_pred_tree)
#     }

#     # 5. Random Forest Regression
#     forest_regressor = RandomForestRegressor(n_estimators=100, random_state=42)
#     forest_regressor.fit(X_train, y_train)
#     y_pred_forest = forest_regressor.predict(X_test)
#     results["Random Forest Regression"] = {
#         "MSE": mean_squared_error(y_test, y_pred_forest),
#         "R2 Score": r2_score(y_test, y_pred_forest)
#     }

#     return results

# @app.post("/run_regression")
# async def run_regression(file: UploadFile = File(...), target_column: str = Form(...)):
#     # Load the CSV file into a DataFrame
#     try:
#         data = pd.read_csv(io.BytesIO(await file.read()))
#     except Exception as e:
#         raise HTTPException(status_code=400, detail=f"Failed to read CSV file: {str(e)}")

#     # Run regression models
#     try:
#         results = run_regression_models(data, target_column)
#     except ValueError as e:
#         raise HTTPException(status_code=400, detail=str(e))
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Error in regression model execution: {str(e)}")

#     return results


def run_regression_plot_models(data, target_column):
    # Drop non-numeric columns
    data = data.select_dtypes(include=['float64', 'int64'])
    
    if target_column not in data.columns:
        raise ValueError(f"Target column '{target_column}' is not numeric or not found in data")

    X = data.drop(target_column, axis=1)
    y = data[target_column]

    results = {}
    pusher.trigger("logs-channel", "log-event", {
            "label":"ML Regression Activity",
            "message": f"Splitting Data and Running it through Regression Models",
            "status": "error"
            }),
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    models = {
        "Linear Regression": LinearRegression(),
        "Polynomial Regression": LinearRegression(),  
        "Support Vector Regression": SVR(kernel='rbf'),
        "Decision Tree Regression": DecisionTreeRegressor(random_state=42),
        "Random Forest Regression": RandomForestRegressor(n_estimators=100, random_state=42)
    }
    
    for name, model in models.items():
        if name == "Polynomial Regression":
            poly_features = PolynomialFeatures(degree=2)
            X_poly_train = poly_features.fit_transform(X_train)
            model.fit(X_poly_train, y_train)
            y_pred = model.predict(poly_features.transform(X_test))
        else:
            model.fit(X_train, y_train)
            y_pred = model.predict(X_test)
        
        # Calculate MSE and R2
        mse = mean_squared_error(y_test, y_pred)
        r2 = r2_score(y_test, y_pred)
        
        results[name] = (y_test, y_pred, mse, r2)  
    return results

@app.post("/plot_regression")
async def plot_regression(file: UploadFile = File(...), target_column: str = Form(...)):
    try:
        pusher.trigger("logs-channel", "log-event", {
            "label":"ML Regression Activity",
            "message": f"Reading CSV file and preparing data",
            "status": "success"
            }),
        data = pd.read_csv(io.BytesIO(await file.read()))
        data = data.select_dtypes(include=['float64', 'int64'])  
    except Exception as e:
        pusher.trigger("logs-channel", "log-event", {
            "label":"ML Regression Activity",
            "message": f"Failed to read CSV file",
            "status": "error"
            }),
        raise HTTPException(status_code=400, detail=f"Failed to read CSV file: {str(e)}")

    if target_column not in data.columns:
        pusher.trigger("logs-channel", "log-event", {
            "label":"ML Regression Activity",
            "message": f"Target Column Not found or is not numeric",
            "status": "error"
            }),
        raise HTTPException(status_code=400, detail=f"Target column '{target_column}' not found or is not numeric")

    try:

        results = run_regression_plot_models(data, target_column)
    except Exception as e:
        pusher.trigger("logs-channel", "log-event", {
            "label":"ML Regression Activity",
            "message": f"Error in regression model execution",
            "status": "error"
            }),
        raise HTTPException(status_code=500, detail=f"Error in regression model execution: {str(e)}")
    pusher.trigger("logs-channel", "log-event", {
            "label":"ML Regression Activity",
            "message": f"Plotting regression model ",
            "status": "success"
            }),
    fig, axs = plt.subplots(3, 2, figsize=(15, 20))
    fig.suptitle("Regression Model Predictions vs Actual Values", fontsize=20, weight='bold')
    axs = axs.ravel()

    for idx, (model_name, (y_test, y_pred, mse, r2)) in enumerate(results.items()):
        axs[idx].scatter(y_test, y_pred, color='blue', alpha=0.5, label='Predicted Values')
        axs[idx].plot([y_test.min(), y_test.max()], [y_test.min(), y_test.max()], 'r--', lw=2, label='Ideal Fit')
        axs[idx].set_title(f"{model_name}", fontsize=16, weight='bold')
        axs[idx].set_xlabel("Actual Values", fontsize=12)
        axs[idx].set_ylabel("Predicted Values", fontsize=12)
        axs[idx].text(0.05, 0.95, f"MSE: {mse:.2f}\nR²: {r2:.2f}",
                      transform=axs[idx].transAxes, fontsize=12, verticalalignment='top',
                      bbox=dict(boxstyle="round,pad=0.3", edgecolor="black", facecolor="lightgray"))
        axs[idx].legend()
        axs[idx].grid(True)

    fig.delaxes(axs[-1])

    buf = BytesIO()
    plt.tight_layout(rect=[0, 0.03, 1, 0.95])
    plt.savefig(buf, format='png')
    buf.seek(0)
    img_base64 = base64.b64encode(buf.read()).decode('utf-8')
    plt.close(fig)
    
    pusher.trigger("logs-channel", "log-event", {
            "label":"ML Regression Activity",
            "message": f"Plotted successfull ",
            "status": "success"
            }),
    return JSONResponse(content={"image":img_base64})
  
# @app.post("/plot_regression")
# async def plot_regression(file: UploadFile = File(...), target_column: str = Form(...)):
#     try:
#         data = pd.read_csv(io.BytesIO(await file.read()))
#         data = data.select_dtypes(include=['float64', 'int64'])  # Drop non-numeric columns
#     except Exception as e:
#         raise HTTPException(status_code=400, detail=f"Failed to read CSV file: {str(e)}")

#     if target_column not in data.columns:
#         raise HTTPException(status_code=400, detail=f"Target column '{target_column}' not found or is not numeric")

#     try:
#         results = run_regression_plot_models(data, target_column)
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Error in regression model execution: {str(e)}")

#     fig, axs = plt.subplots(3, 2, figsize=(15, 20))
#     fig.suptitle("Regression Model Predictions vs Actual Values", fontsize=20, weight='bold')
#     axs = axs.ravel()

#     for idx, (model_name, (y_test, y_pred, mse, r2)) in enumerate(results.items()):
#         axs[idx].scatter(y_test, y_pred, color='blue', alpha=0.5, label='Predicted Values')
#         axs[idx].plot([y_test.min(), y_test.max()], [y_test.min(), y_test.max()], 'r--', lw=2, label='Ideal Fit')
#         axs[idx].set_title(f"{model_name}", fontsize=16, weight='bold')
#         axs[idx].set_xlabel("Actual Values", fontsize=12)
#         axs[idx].set_ylabel("Predicted Values", fontsize=12)
#         axs[idx].text(0.05, 0.95, f"MSE: {mse:.2f}\nR²: {r2:.2f}",
#                       transform=axs[idx].transAxes, fontsize=12, verticalalignment='top',
#                       bbox=dict(boxstyle="round,pad=0.3", edgecolor="black", facecolor="lightgray"))
#         axs[idx].legend()
#         axs[idx].grid(True)

#     fig.delaxes(axs[-1])

#     buf = io.BytesIO()
#     plt.tight_layout(rect=[0, 0.03, 1, 0.95])
#     plt.savefig(buf, format='png')
#     buf.seek(0)
#     plt.close(fig)
    
#     return StreamingResponse(buf, media_type="image/png")
  
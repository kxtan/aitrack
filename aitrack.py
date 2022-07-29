from src.MongoWrapper import MongoWrapper
from fastapi import FastAPI, HTTPException

app = FastAPI(title="aitrack",
    description="Simple backend to track data for ML Projects",
    version="0.0.1",
)

mongo = MongoWrapper()

@app.post("/upload")
def upload(username:str, project:str, data:dict):
    """Upload data to be stored. (model metadata, metrics to monitor etc)

    Args:
        username (str): username
        project (str): project name
        data (dict): data to be stored
    """
    mongo.upload_doc(username, project, data)

@app.get("/download")
def load_data(username=None, project=None) -> list:
    """Load data based on username

    Args:
        username (str): username

    Returns:
        list: list of dictionaries
    """
    if not username and not project:
        raise HTTPException(status_code=404, detail="must have at least one input: username or project")
    if username and not project:
        return mongo.load_by_username(username)
    if not username and project:
        return mongo.load_by_project(project)
    return mongo.load_by_username_project(username, project)
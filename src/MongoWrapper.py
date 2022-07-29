from pymongo import (
    MongoClient,
    results
)
from datetime import datetime

UPLOAD_TIME = "uploadTime"
USERNAME = "username"
PROJECT = "project"

class MongoWrapper():
    """Wrapper for MongoDB interactions for tracking ML projects
    """
    
    def __init__(self, host="localhost", port=27017, default_init=True) -> None:
        """Wrapper for mongodb interaction for ML related tracking

        Schema is not strict, however documents are internally 
        enforced to have timestamp field in case filtering is needed in future.

        Args:
            host (str, optional): hostname for mongodb. Defaults to "localhost".
            port (int, optional): port of the host. Defaults to 27017.
        """
        self.client = MongoClient(host, port)

        if default_init:
            self.init_db()
            self.init_collection()

    def init_db(self, dbname="mlops-shared"):
        """initialize db"""
        self.db = self.client[dbname]

    def init_collection(self, colname="mlops-shared"):
        """initialize collection"""        
        self.collection = self.db[colname]

    def _time_enforce(self, input_dict:dict) -> dict:
        """Call this to insert time on documents, error if fail to do so.

        Args:
            input_dict (dict): input dictionary

        Returns:
            dict: dictionary with time field
        """
        input_dict[UPLOAD_TIME] = datetime.now()
        return input_dict
    
    def upload_doc(self, username:str, project:str, input_dict:dict) -> results.InsertOneResult:
        """
        Args:
            input_dict (dict): input dictionary.

        Returns:
            results.InsertOneResult: insert operation results.
        """
        input_dict = self._time_enforce(input_dict)
        input_dict[USERNAME] = username
        input_dict[PROJECT] = project

        return self.collection.insert_one(input_dict)

    def _exclude_id(self):
        """To exclude id from results"""
        return {"_id": False}

    def load_by_username(self, username:str) -> list:
        """load data by username
            sql equivalent: SELECT * FROM DB WHERE username=<username>
        """
        return [x for x in self.collection.find({USERNAME : username}, self._exclude_id())]

    def load_by_username_project(self, username:str, project:str) -> list:
        """load data by username and project
            sql equivalent: SELECT * FROM DB WHERE username=<username> AND project=<project>
        """
        return [x for x in self.collection.find({USERNAME : username, PROJECT: project}, self._exclude_id())]

    def load_by_project(self, project:str) -> list:
        """load data by project
            sql equivalent: SELECT * FROM DB WHERE project=<project>
        """
        return [x for x in self.collection.find({PROJECT: project}, self._exclude_id())]
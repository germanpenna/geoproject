import time 
import requests 

class AirbyteConnection():
    def __init__(
        self, 
        connection_id:str, 
        auth_id:str,
        host="localhost:8000", 
        ): 
        self.host = host
        self.connection_id = connection_id
        self.auth_id = auth_id

    def run(self)->bool:
        """
        Triggers an airbyte sync. 

        Input: 
        - connection_id: the id of the connection 

        Returns: 
        - bool: true if the sync succeeds, an error otherwise. 
        """
        url = f"http://{self.host}/api/v1/connections/sync"
        data = {
            "connectionId": self.connection_id
        }
        headers = {
            "Authorization": self.auth_id
        }
        sync_response = requests.post(url=url, json=data, headers=headers)
        job_id = sync_response.json()["job"]["id"]
        job_status = "running"
        while job_status == "running": 
            time.sleep(5)
            url = f"http://{self.host}/api/v1/jobs/get"
            data = {
                "id": job_id
            }
            job_response = requests.post(url=url, json=data, headers=headers)
            job_status = job_response.json()["job"]["status"]
            if job_status == "failure": 
                raise Exception(f"Run failed. {job_response.text}")
        return True 

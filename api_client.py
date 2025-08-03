import requests
import logging

class COVIDAPIService:
    def __init__(self, url_base, token=None):
        self.url_base = url_base
        self.token = token

    def get_data(self, query_params=None, resource_path=""):
        request_headers = {'X-Api-Key': self.token} if self.token else {}
        target_url = f"{self.url_base}"
        if resource_path:
            target_url += f"/{resource_path}"

        try:
            reply = requests.get(target_url, params=query_params, headers=request_headers)
            reply.raise_for_status()
            return reply.json()
        except requests.exceptions.RequestException as err:
            logging.error(f"Failed to fetch data from API: {err}")
            return []

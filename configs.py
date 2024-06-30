from os import environ

source_url = f"http://{environ.get('SOURCE_URL')}"
source_path = environ.get("SOURCE_PATH")
tables_schema = {}
base_rebuild = False

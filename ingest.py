import logging
from pathlib import Path
import os
from dotenv import load_dotenv, find_dotenv
import requests
import json

def setup_environment():
  """Updates sys.path and reads the .env settings.
  Called by do_ingest."""
  logging.info("setting up environment")
  ## allows bdr_tools to be imported ------------------------------
  load_dotenv(find_dotenv())
  # Get the COLLECTION_PID from the .env file
  COLLECTION_PID = os.environ["COLLECTION_PID"]
  logging.debug(f"COLLECTION_PID, ``{COLLECTION_PID}``")
  API_IDENTITY = os.environ["API_IDENTITY"]
  logging.debug(f"API_IDENTITY, ``{API_IDENTITY}``")
  API_URL = os.environ["API_URL"]
  logging.debug(f"API_URL, ``{API_URL}``")
  OWNER_ID = os.environ["OWNER_ID"]
  logging.debug(f"OWNER_ID, ``{OWNER_ID}``")
  API_KEY = os.environ["API_KEY"]
  logging.debug(f"API_KEY, ``{API_KEY}``")

  # TEMPORARY
  INPUT_FILES_DIR = os.environ["INPUT_FILES_DIR"]
  logging.debug(f"INPUT_FILES_DIR, ``{INPUT_FILES_DIR}``")

  env_vars = {}
  env_vars["collection_pid"] = COLLECTION_PID
  env_vars["api_identity"] = API_IDENTITY
  env_vars["api_url"] = API_URL
  env_vars["owner_id"] = OWNER_ID
  env_vars["api_key"] = API_KEY

  # TEMPORARY
  env_vars["INPUT_FILES_DIR"] = INPUT_FILES_DIR

  return env_vars

def set_basic_params(env_vars):
  """Sets forthright params.
  Called by run_create_metadata_only_object()"""
  params = {
    "identity": env_vars["api_identity"],
    "authorization_code": env_vars["api_key"],
    "rights": json.dumps(
      {
        "parameters": {
          "owner_id": env_vars["owner_id"],
          "additional_rights": 'BDR_PUBLIC#discover,display'
        }
      }
    ),
    "rels": json.dumps({"isMemberOfCollection": env_vars["collection_pid"]}),
  }
  return params

def perform_post(api_url, params, files=None):
  try:
    if files:
      r = requests.post(api_url, data=params, files=files)
    else:
      r = requests.post(api_url, data=params)
  except Exception as e:
    logging.exception(f"error creating metadata object: {e}")
    raise
  if r.ok:
    return r.json()["pid"]
  else:
    msg = f"error creating metadata object: {r.status_code} - {r.text}"
    logging.error(msg)
    raise Exception(msg)

def ingest_files(
    mods_path,
    file_path,
    allowed_streams:dict,
    parent_relationship=None
  ) -> str:
  """
  Ingests files into a system.
  Args:
    mods_path (str): The path to the MODS file.
    file_path (str): The path to the file to ingest.
    allowed_streams (dict): A dictionary mapping file extensions to content streams.
    parent_relationship (tuple): The pid and relationship to parent. Defaults to None.
  Returns:
    (str): The PID of the ingested files.
  """

  env_vars = setup_environment()
  params = set_basic_params(env_vars)

  if parent_relationship:
    (parent_pid, rel_type) = parent_relationship
    if rel_type not in ['isPartOf', 'isTranslationOf', 'isTranscriptOf']:
      raise ValueError(f"Invalid relationship type: {rel_type}")
    # Read params['rels'] into a dict
    temp_rels = json.loads(params["rels"])

    # Set the parent pid and page number
    temp_rels[rel_type] = parent_pid

    # Convert params['rels'] back to a string
    params["rels"] = json.dumps(temp_rels)

  mods_path = Path(mods_path)
  with open(mods_path, "r") as mods_file:
    mods_file_obj = mods_file.read()

  params["mods"] = json.dumps({"xml_data": mods_file_obj})

  logging.debug(f"ingesting {file_path}")
  if not file_path:
    pid = 'fake12345'
    pid = perform_post(api_url=env_vars["api_url"], params=params)
    return pid

  file = Path(file_path)
  content_streams = []
  files = {}

  if file.suffix not in allowed_streams.keys():
    input(f"File extension {file.suffix} not allowed. Press enter to continue.")
    return
  # params["content_model"] = allowed_streams[file.suffix]

  with open(file, "rb") as file_obj:
    content_streams.append({
      "dsID": allowed_streams[file.suffix],
      "file_name": file.name
    })

    files[file.name] = file_obj

  logging.debug(f"{params=}")
  logging.debug(f"{content_streams=}")

  pid = "fake12345"
  pid = perform_post(api_url=env_vars["api_url"], params=params, files=files)

  return pid

if __name__ == "__main__":
  logging.info("__name__ is `main`")

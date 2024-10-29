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

  # TEMPORARY
  INPUT_FILES_DIR = os.environ["INPUT_FILES_DIR"]
  logging.debug(f"INPUT_FILES_DIR, ``{INPUT_FILES_DIR}``")

  env_vars = {}
  env_vars["collection_pid"] = COLLECTION_PID
  env_vars["api_identity"] = API_IDENTITY
  env_vars["api_url"] = API_URL
  env_vars["owner_id"] = OWNER_ID

  # TEMPORARY
  env_vars["INPUT_FILES_DIR"] = INPUT_FILES_DIR

  return env_vars

def set_basic_params(env_vars):
  """Sets forthright params.
  Called by run_create_metadata_only_object()"""
  params = {
    "identity": env_vars["api_identity"],
    "rights": json.dumps(
      {
        "parameters": {
          "owner_id": env_vars["owner_id"],
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

def ingest_files(mods_path, file_path, allowed_streams:dict, parent_pid=None):
  """
  Ingests files into a system.
  Args:
    mods_path (str): The path to the MODS file.
    allowed_streams (dict): A dictionary mapping file extensions to content streams.
    parent_pid (str, optional): The parent PID. Defaults to None.
    numbered (bool, optional): Whether the files are numbered. Defaults to False.
  Returns:
    str: The PID of the ingested files.
  """

  env_vars = setup_environment()
  params = set_basic_params(env_vars)

  mods_path = Path(mods_path)
  file = Path(file_path)
  with open(mods_path, "r") as mods_file:
    mods_file_obj = mods_file.read()

  params["mods"] = json.dumps({"xml_data": mods_file_obj})

  if parent_pid:
    # Read params['rels'] into a dict
    temp_rels = json.loads(params["rels"])

    # Set the parent pid and page number
    temp_rels["isPartOf"] = parent_pid

    # Convert params['rels'] back to a string
    params["rels"] = json.dumps(temp_rels)

  content_streams = []
  files = {}

  if file.suffix not in allowed_streams.keys():
    input(f"File extension {file.suffix} not allowed. Press enter to continue.")
    return

  with open(file, "rb") as file_obj:
    content_streams.append({
      "dsID": allowed_streams[file.suffix],
      "file_name": file.name
    })

    files[file.name] = file_obj.read()

  pid = perform_post(api_url=env_vars["api_url"], params=params, files=files)

  return pid

if __name__ == "__main__":
  logging.info("__name__ is `main`")

  ## -------------------------------------------------------------
  ## NOTE: This script is meant to be called by `hallhoag_pre_ingest.py`.
  ## It can be called directly, primarily for testing. Uncomment the below as needed.
  ## Be sure to check the hard-coded values.
  ## -------------------------------------------------------------

  # env_vars = setup_environment()
  # base_dir = pathlib.Path( env_vars['INPUT_FILES_DIR'] )
  # test_dir = base_dir.joinpath( 'HH020005/HH020005_0001/' )
  # # pid = do_ingest(test_dir,'',is_child=False)              # THIS IS WHERE WORK IS DONE
  # pid = do_ingest(test_dir,'',is_child=True,parent_pid='test:5ksjfyb5')              # THIS IS WHERE WORK IS DONE
  # print( f'pid, ``{pid}``' )
  # logging.debug( 'eof' )
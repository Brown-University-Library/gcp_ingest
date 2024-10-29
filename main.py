import os
from pathlib import Path, PureWindowsPath
import csv
from argparse import ArgumentParser
from dotenv import load_dotenv
from ingest import ingest_files
import logging

stream_map = {
  ".mov": "VIDEO-MASTER",
  ".mp4": "VIDEO-MASTER",
  ".pdf": "PDF",
}

def get_mnt_path_from_windows_path(windows_path):
  winpath = PureWindowsPath(windows_path)
  new_root = Path(f"/mnt/{winpath.drive[0].lower()}")
  filepath = new_root.joinpath(*winpath.parts[1:]).resolve()

  return filepath

def dict_from_row(row):
  # Get the filepath from the row and replace the drive letter
  filepath = get_mnt_path_from_windows_path(row['filepath'])
  filename = row['identifierFileName']
  if not filepath.exists():
    logging.error(f"File {filepath} does not exist")
    return None
  if not filepath.is_dir():
    logging.error(f"File {filepath} is not a directory")
    return None

  files = []
  for file in filepath.glob(str(filename).strip() + '.*'):
    if file.suffix.lower() not in stream_map.keys():
      logging.debug(f"Skipping {file.suffix[1:].upper()} file {file.name}")
      continue
    logging.debug(f"Found {file.suffix[1:].upper()} file {file.name}")
    files.append(file)

  if len(files) == 0:
    logging.error(f"No files found for {filename} in {filepath}")
    return None
  if len(files) > 1:
    logging.error(f'Multiple files found for {filename}:', files)
    return None

  return {
    'filepath': files[0],
    'filename': filename,
  }

def make_ingestable(data_file:Path):
  data = []
  with data_file.open() as f:
    reader = csv.DictReader(f, delimiter='\t')
    next(reader) # skip header
    data = [row for row in reader if row['identifierFileName'] != '']

  parented_data = [
    {
      **dict_from_row(row),
      'children': [
        dict_from_row(child)
        for child in data if child['parent'] == row['identifierFileName']
      ],
    } for row in data if row['parent'] == ''
  ]

  return parented_data

def ingest_data(data, mods_dir):
  for row in data:
    logging.info(f'Ingesting parent {row["filename"]}')
    filepath = Path(row['filepath'])
    filename = row['filename']
    mods = Path(mods_dir).joinpath(f'{filename}.xml')
    # pid = ingest_files(mods, filepath, stream_map)
    pid = '12345'

    for child in row['children']:
      logging.info(f'Ingesting {child["filename"]} with parent {pid}')
      # ingest_files(mods, child['filepath'], stream_map, pid)

def main(data_file: Path):
  load_dotenv()
  mods_dir = os.environ['MODS_DIR'] 
  data = make_ingestable(data_file)
  ingest_data(data, mods_dir)

if __name__ == '__main__':
  logging.basicConfig(level=logging.ERROR)
  parser = ArgumentParser()
  parser.add_argument('data_file', type=Path)
  args = parser.parse_args()
  main(args.data_file)


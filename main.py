import os
from pathlib import Path, PureWindowsPath
import csv
from argparse import ArgumentParser
from dotenv import load_dotenv
from ingest import ingest_files
import logging
import pandas as pd
from numpy import nan

stream_map = {
  ".mov": "VIDEO-MASTER",
  ".mp4": "VIDEO-MASTER",
  ".pdf": "PDF",
}

def get_mnt_path_from_windows_path(windows_path):
  logging.debug(f"Getting mnt path from windows path {windows_path}")
  winpath = PureWindowsPath(windows_path)
  new_root = Path(f"/mnt/{winpath.drive[0].lower()}")
  filepath = new_root.joinpath(*winpath.parts[1:]).resolve()

  return filepath

def dict_from_row(row):
  logging.debug(f"Creating dict from row {row.get('identifierFileName')}")
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

  result_dict = {
    'filepath': files[0],
    'filename': filename,
  }

  # return early if there is no parent
  if 'parent' not in row:
    return result_dict

  # add parent relationship to dict
  if 'transcriptions (document)' in row['genreAAT']:
    parent_relationship = 'isTranscriptOf'
  elif 'translations (document)' in row['genreAAT']:
    parent_relationship = 'isTranslationOf'
  else:
    parent_relationship = 'isPartOf'
  result_dict.update({
    'relationship': parent_relationship,
  })
  return result_dict

def make_ingestable(data: pd.DataFrame):
  logging.debug("Making data ingestable")

  data_dict = data.to_dict('records')
  data_dict.pop(0)
  logging.debug([{"parent":row['parent'], "filename":row['identifierFileName']} for row in data_dict[:2]])

  parented_data = [
    {
      **dict_from_row(row),
      'children': [
        dict_from_row(row)
        for child in data_dict
        if child['identifierFileName'] and child['parent'] == row['identifierFileName']
      ],
    }
    for row in data_dict
    if row['identifierFileName']
    and not row['parent'] or type(row['parent']) != str
  ]

  return parented_data

def ingest_data(data, mods_dir):
  logging.debug("Ingesting data")
  for row in data:
    parent = {key:value for key,value in row.items() if key != 'children'}
    if not parent:
      continue
    logging.info(f'Ingesting parent {row["filename"]}')
    filepath = Path(parent['filepath'])
    filename = parent['filename']
    mods = Path(mods_dir).joinpath(f'{filename}.xml')
    pid = ingest_files(mods, filepath, stream_map)
    pid = '12345'

    for child in row['children']:
      if not child:
        continue
      logging.info(f'Ingesting {child["filename"]} with parent {pid}')
      # ingest_files(mods, child['filepath'], stream_map, (pid,child['relationship']))

def check_cols(filepath):
  with open(filepath, 'rb') as f:
    data = pd.read_excel(f)
    data = data.fillna('')
    # Remove empty rows
    data.dropna(how='all', inplace=True)
    # Check for empty column headers in pandas dataframe
    headers = data.columns
    logging.debug(f"Headers: {headers}")
    second_row = data.iloc[0]
    for i, header in enumerate(headers):
      if 'Unnamed' in header:
        if "parent" in second_row[i].lower():
          data.rename(columns={header: 'parent'}, inplace=True)
          continue
        if "filepath" in second_row[i].lower():
          data.rename(columns={header: 'filepath'}, inplace=True)
          continue
        print(f"Column {i + 1} is missing, second row value is {second_row[i]}")
        new_header = input(f"Enter the column header for column {i + 1}: ")
        if not new_header.isidentifier():
          raise ValueError(f"'{new_header}' is not a valid column header")
        data.rename(columns={header: new_header}, inplace=True)
    return data

def main(data_file: Path):
  load_dotenv()
  mods_dir = os.environ['MODS_DIR']
  sheet = check_cols(data_file)
  data = make_ingestable(sheet)
  ingest_data(data, mods_dir)

if __name__ == '__main__':
  logging.basicConfig(
    level=logging.DEBUG,
    format='[%(asctime)s] %(levelname)s [%(module)s-%(funcName)s()::%(lineno)d] %(message)s',
    datefmt='%d/%b/%Y %H:%M:%S'
  )
  parser = ArgumentParser()
  parser.add_argument('data_file', type=Path)
  args = parser.parse_args()
  main(args.data_file)


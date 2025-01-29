import os
from pathlib import Path, PureWindowsPath
from argparse import ArgumentParser
from pprint import pformat
from dotenv import load_dotenv
from ingest import ingest_files
import logging
import pandas as pd

stream_map = {
  ".mov": "VIDEO-MASTER",
  ".mp4": "VIDEO-MASTER",
  ".pdf": "PDF",
}
cache = {}

def abbr_path(path:str, length:int, sep:str='/',abbr_len:int=2):
  if len(path) < length:
    return path

  split_path = [part for part in path.split(sep) if part]
  abbr_split_path = [part[:abbr_len] for part in split_path]

  test_path = ""
  if sep == '/':
    test_path += sep

  for i in range(len(split_path)):
    test_path = sep.join(abbr_split_path[:i])+sep+sep.join(split_path[i:])
    if len(test_path) > length:
      continue
    return test_path
  return '...'+test_path[length-3:]

def get_cache_options(split_path:list):
  # logging.debug(f"Getting cache options from split path {split_path}")
  cache_options = []
  for i in range(len(split_path), 0, -1):
    cache_options.append('\\'.join(split_path[:i+1]))
  return cache_options

def get_mnt_path_from_windows_path(windows_path:str, cache={'mntdir':{'path':Path('/mnt')}}):
  sep='\\'
  logging.debug(f"Getting mnt path from windows path {abbr_path(windows_path,40,sep)}")
  if windows_path in cache:
    return cache[windows_path]['path']

  winpath = PureWindowsPath(windows_path)
  new_root = cache['mntdir']['path'].joinpath(winpath.drive[0].lower())

  split_path = windows_path.split('\\')
  cache_options = get_cache_options(split_path)
  # logging.debug(f"Cache options: {cache_options}")

  for option in cache_options:
    if option in cache:
      # get the remaining path after the cached path
      remaining_path = '\\'.join(split_path[len(option.split('\\')):])
      return cache[option]['path'].joinpath(remaining_path)
    else:
      filepath = new_root.joinpath(*option.split('\\')[1:])
      cache[option] = {'path':filepath}

  cache[windows_path] = {'path': filepath}

  return filepath

def dict_from_row(row, pid=None):
  logging.debug(f"Creating dict from row {row.get('identifierFileName')}")
  # Get the filepath from the row and replace the drive letter
  filepath_str = row['filepath']
  filepath = get_mnt_path_from_windows_path(filepath_str,cache)
  # add path to cache
  cache[filepath_str]['path'] = filepath
  filename = str(row['identifierFileName']).strip()
  if not filepath.exists():
    logging.warning(f"File {filepath} does not exist")
    return {}
  if not filepath.is_dir():
    logging.warning(f"File {filepath} is not a directory")
    return {}

  if not cache[filepath_str].get('glob', None):
    cache[filepath_str]['glob'] = list(filepath.glob('*'))
  fileglob = cache[filepath_str]['glob']
  logging.debug(f"Fileglob: {fileglob}")
  files = file_from_glob(filename, fileglob,allowed_streams=stream_map.keys())

  if len(files) == 0:
    logging.warning(f"No files found for {filename} in {filepath}")
    return {}
  if len(files) > 1:
    logging.warning(f'Multiple files found for {filename}: {files}')
    return {}

  result_dict = {
    'filepath': files[0],
    'filename': filename,
  }

  # return early if there is no parent
  if 'parent' not in row:
    return result_dict

  # add parent relationship to dict
  logging.debug(f"Genre: {row.get('genreAAT')}")
  if 'transcriptions (documents)' in row['genreAAT']:
    parent_relationship = 'isTranscriptOf'
  elif 'translations (documents)' in row['genreAAT']:
    parent_relationship = 'isTranslationOf'
  else:
    parent_relationship = 'isPartOf'
  result_dict.update({
    'relationship': parent_relationship,
  })

  if pid:
    result_dict.update({
      "pid":pid,
      "children":[]
    })
  return result_dict

def file_from_glob(filename, fileglob,allowed_streams=[]):
  files = []
  for file in fileglob:
    if filename == file.name:
      return [file]
    if file.stem != filename:
      logging.debug(f"Skipping file {file.name} because {file.stem} != {filename}")
      continue
    if allowed_streams and file.suffix.lower() not in allowed_streams:
      logging.debug(f"Skipping {file.suffix[1:].upper()} file {file.name}")
      continue
    logging.debug(f"Found {file.suffix[1:].upper()} file {file.name}")
    files.append(file)
  return files

def make_ingestable(data: pd.DataFrame):
  logging.info("Making data ingestable")

  data_dict = data.to_dict('records')
  data_dict.pop(0)
  logging.debug([
      { "parent":row['parent'],
        "filename":row['identifierFileName']
      } for row in data_dict[:4]
  ])

  parented_data = []
  for row in data_dict:
    if row.get('ingestcomplete') and not row.get('pid'):
      logging.debug(f'ingest already completed for {row["itemTitle"]}')
      continue
    if not row['identifierFileName'] or not row["filepath"]:
      logging.warning(f"Row has no filename and/or path: {row['itemTitle']}")
      continue
    if row['parent'] and type(row['parent']) is str:
      continue

    if row.get("pid"):
      for child in data_dict:
        if child.get('ingestcomplete'):
          logging.debug(f"ingest already completed for {child['itemTitle']}")
          continue
        if not child['identifierFileName']:
          continue
        if child['parent'] == row['identifierFileName']:
          parented_data.append(dict_from_row(child,row['pid']))
      continue
    parent = {
      "filename": row['identifierFileName'],
      "filepath": None,
      'children': [dict_from_row(row)],
    }
    for child in data_dict:
      if not child['identifierFileName']:
        continue
      if child['parent'] == row['identifierFileName']:
        parent['children'].append(dict_from_row(child))
    parented_data.append(parent)

  logging.debug(pformat(parented_data,sort_dicts=False,))
  return parented_data

def ingest_data(data, mods_dir):
  logging.info("Ingesting data")
  for item in data:
    if not item:
      continue
    filepath = item['filepath']
    filename = item['filename'].strip()
    parent_pid = item.get('pid',None)

    mods = Path(mods_dir).joinpath(f'{filename}.mods.xml')

    if parent_pid:
      logging.info(f'Ingesting {item["filename"]} with parent {parent_pid}')
      ingest_files(mods, filepath,stream_map,(parent_pid,item['relationship']))
      continue

    logging.info(f'Ingesting parent item {item["filename"]}')
    pid = ingest_files(mods, filepath, stream_map)
    if not pid:
      logging.warning(f"ingest failed, no pid for ingest of {filename}")
    # pid = '12345'
    logging.info(f'ingested. {pid=}')

    for child in item['children']:
      if not child:
        continue
      mods = Path(mods_dir).joinpath(f'{child["filename"]}.mods.xml')
      logging.info(f'Ingesting {child["filename"]} with parent {pid}')
      ingest_files(mods, child['filepath'], stream_map, (pid,child['relationship']))

def check_ingestable_for_mods(data, mods_dir):
  logging.info("Ingesting data")
  for item in data:
    if not item:
      continue
    filename = item['filename'].strip()

    mods = Path(mods_dir).joinpath(f'{filename}.mods.xml')
    if not mods.exists():
        logging.warning(f"mods {mods.name} does not exist")

    if "children" not in item.keys():
      logging.warning(f"item has no key 'children': {item}")

    for child in item['children']:
      if not child:
        continue
      mods = Path(mods_dir).joinpath(f'{child["filename"]}.mods.xml')
      if not mods.exists():
        logging.warning(f"mods {mods.name} does not exist")

def get_sheet_name(filepath):
  sheets = pd.ExcelFile(filepath).sheet_names
  for i, sheet in enumerate(sheets):
    print(i,sheet)
  sheet_num = int(input("Enter the number of the sheet you want to ingest: "))
  return sheets[sheet_num]

def check_cols(filepath,sheet_name=None):
  with open(filepath, 'rb') as f:
    # print names of sheets
    if not sheet_name:
      sheet_name = get_sheet_name(filepath)
    data = pd.read_excel(f,sheet_name)
    # Remove empty rows
    data.dropna(how='all', inplace=True)
    data.fillna('',inplace=True)
    # Check for empty column headers in pandas dataframe
    headers = data.columns
    # logging.debug(f"Headers: {headers}")
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

def main(args):
  load_dotenv()
  mods_dir = os.environ['MODS_DIR']
  sheet = check_cols(args.data_file, args.sheet)
  data = make_ingestable(sheet)
  if args.mock:
    check_ingestable_for_mods(data, mods_dir)
    logging.debug(pformat(data,sort_dicts=False))
    logging.info("Mock run, not ingesting")
    return
  ingest_data(data, mods_dir)

def parse_arguments():
  parser = ArgumentParser()
  parser.add_argument('data_file',
    type=Path,
    help='Path to the data file'
  )
  parser.add_argument('--mntdir',
    type=str,
    default='/mnt',
    help='Parent dir of mount(s), win drive letter is used as actual mountpoint'
  )
  parser.add_argument('--sheet',
    type=str,
    help='Sheet name in the excel file'
  )
  parser.add_argument('--mock',
    action='store_true',
    help='Run without ingesting'
  )
  parser.add_argument("-l", "--log",
    dest="loglevel",
    choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
    default='INFO',
    help="Set the logging level")
  args = parser.parse_args()
  return args

if __name__ == '__main__':
  args = parse_arguments()
  logging.basicConfig(
    level=args.loglevel,
    format='[%(asctime)s] %(levelname)s [%(module)s-%(funcName)s()::%(lineno)d] %(message)s',
    datefmt='%d/%b/%Y %H:%M:%S',
    handlers=[
        logging.FileHandler("../gcp_ingest.log"),
        logging.StreamHandler()
    ]
  )
  mount_dirpath = Path(args.mntdir)
  cache.update({
    'mntdir': {'path':mount_dirpath},
    args.mntdir: {'path':mount_dirpath}
  })
  main(args)


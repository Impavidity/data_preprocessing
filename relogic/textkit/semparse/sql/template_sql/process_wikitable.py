import argparse
import multiprocessing as mp
from tqdm import tqdm
import json
from collections import defaultdict
import random
import spacy
nlp = spacy.load("en_core_web_sm")

def get_table_name(table):
  pg_title, section_title, table_caption = "", "", ""
  if "pgTitle" in table:
    pg_title = table["pgTitle"]
  if "sectionTitle" in table:
    section_title = table["sectionTitle"]
  if "tableCaption" in table:
    table_caption = table["tableCaption"]
  return (pg_title, section_title, table_caption)

def table_process(wiki_table):
  print(wiki_table["_id"])
  table_caption, section_title, pg_title = get_table_name(wiki_table)
  header = [header["text"] for header in wiki_table["tableHeaders"][0]]
  if len(header) <= 3:
    return None
  if len(header) != len(set(header)):
    return None
  if any([len(h.split()) > 10 for h in header]):
    return None
  table = []
  for row in wiki_table["tableData"]:
    row_values = []
    for col in row:
      row_values.append(col["text"])
    table.append(row_values)
  if len(table) <= 2:
    return None
  obj = {"caption": (table_caption, section_title, pg_title),
         "header": header,
         "table": table,
         "_id": wiki_table["_id"]}
  return obj

def main(data_path, output_path):
  pool = mp.Pool(90)
  tables = []
  with open(data_path) as fin:
    for line in tqdm(fin):
      tables.append(json.loads(line))

  processed_tables = pool.map(table_process, tables)
  with open(output_path, "w") as fout:
    for table in processed_tables:
      if table:
        fout.write(json.dumps(table) + "\n")

def is_digit(val):
  try:
    float(val)
    return True
  except:
    return False

def get_column_type(column_values):
  counter_empty = 0
  counter_digit = 0
  for item in column_values:
    if item == "":
      counter_empty += 1
    elif is_digit(item):
      counter_digit += 1
  if counter_empty / len(column_values) > 0.5:
    return None
  if counter_digit + counter_empty == len(column_values):
    return "number"
  return "text"

def is_valid_column_name(column_name):
  if is_digit(column_name):
    # Remove number column
    return False
  if any([x in column_name for x in ["+", "*", "/"]]):
    return False
  return True

def clean_rows(rows):
  kept_rows = []
  for row in rows:
    counter = 0
    for item in row:
      if item == "":
        counter += 1
    if counter == len(row):
      continue
    kept_rows.append(row)
  return kept_rows

def get_root_phrase(sent):
  doc = nlp(sent)
  return list(doc.sents)[0].root

def filter_tokens(tokens):
  filtered_tokens = []
  if len(tokens) > 2 and tokens[0].lower() == "list" and tokens[1].lower() == "of":
    tokens = tokens[2:]
  for token in tokens:
    if "(" in token or ")" in token or "+" in token:
      continue
    filtered_tokens.append(token)
  return filtered_tokens

def process_name(table_caption):
  tokens = table_caption.strip().split()
  tokens = filter_tokens(tokens)
  table_name = " ".join(tokens)
  if len(tokens) > 2:
    table_name = get_root_phrase(table_name)
  return str(table_name)

def process_table_name(table):
  (pg_title, section_title, table_caption) = table["caption"]
  if table_caption != "":
    table_name = process_name(table_caption)
  elif section_title != "":
    table_name = process_name(section_title)
  elif pg_title != "":
    table_name = process_name(pg_title)
  else:
    table_name = "table"
  return table_name

def refine_table(table):
  kept_column_names = []
  kept_column_values = []
  column_types = []
  for column_name, column_values in zip(table["header"], zip(*table["table"])):
    column_type = get_column_type(column_values)
    if column_type is None:
      # Remove no value columns
      continue

    if not is_valid_column_name(column_name):
      continue
    if column_name == "":
      # Empty header fill in
      if column_type == "number":
        column_name = "id"
      if column_type == "text":
        column_name = "name"
    kept_column_names.append(column_name)
    column_types.append(column_type)
    kept_column_values.append(list(column_values))
  table["header"] = kept_column_names
  table["table"] = clean_rows(list(zip(*kept_column_values)))
  table["column_type"] = column_types
  table["table_name"] = process_table_name(table)
  return table

def refine(data_path, output_path):
  fout = open(output_path, "w")
  with open(data_path) as fin:
    for idx, line in tqdm(enumerate(fin)):
      example = json.loads(line)
      table = refine_table(example)
      if len(table["header"]) > 10 or len(table["header"]) < 4:
        continue
      fout.write(json.dumps(table) + "\n")

def sample_one_database(args):
  table_idx, table_candidates = args
  table_to_add_size = random.randint(1, 2)
  return table_idx, random.sample(table_candidates, min(len(table_candidates), table_to_add_size))


def create_database(data_path, output_path):
  tables = []
  with open(data_path) as fin:
    for idx, line in tqdm(enumerate(fin)):
      example = json.loads(line)
      example["idx"] = idx
      tables.append(example)
  primary_key_to_table_ids = defaultdict(list)
  for table in tables:
    primary_key_to_table_ids[table["header"][0]].append(table["idx"])
  table_pairs = defaultdict(list)
  for table in tqdm(tables):
    for column in table["header"][1:]:
      if column in primary_key_to_table_ids:
        table_pairs[table["idx"]].extend(primary_key_to_table_ids[column])

  args = []
  for table_idx in table_pairs.keys():
    args.append((table_idx, table_pairs[table_idx]))
  pool = mp.Pool(90)
  table_pairs_list = pool.map(sample_one_database, args)

  with open(output_path, "w") as fout:
    for pair in table_pairs_list:
      fout.write(" ".join(map(lambda x:str(x), [pair[0]] + pair[1])) + "\n")


if __name__ == "__main__":
  parser = argparse.ArgumentParser()
  parser.add_argument("--data_path", type=str)
  parser.add_argument("--output_path", type=str)
  parser.add_argument("--stage", type=str)

  args = parser.parse_args()
  if args.stage == "main":
    main(args.data_path, args.output_path)
  elif args.stage == "refine":
    refine(args.data_path, args.output_path)
  # create_database(args.data_path, args.output_path)
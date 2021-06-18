import argparse
import json
import string
from tqdm import tqdm
import random
import multiprocessing as mp



KEYWORDS = ["SELECT", "WHERE", "_VALUE_", "_VALUES_", "GROUP", "BY", "ORDER", "LIMIT", "FROM", "JOIN", "ON",
            "COUNT", "DISTINCT", "AND", "DESC", "AVG", "HAVING", "MAX", "IN", "<", "SUM", "INTERSECT", "NOT", "MIN",
            "EXCEPT", "ALL", "OR", "ASC", "LIKE", "!=", "UNION", "BETWEEN", "INTO",
            "WHEN", "ELSE", "CASE", "THEN", "TRUE", "FALSE", "END", "AS", "LEFT", "RIGHT", "NATURAL", "FULL", "CONVERT", "CAST",
            "IS", "NULL"]
LOWER_KEYWORDS = [k.lower() for k in KEYWORDS]

PUNC = ['<=>', '/', '(', '.', '$', '=>', '_', ']', '[', '>', '#', '!', ',', '*', '&', '|', '?', '~', '-', '<=', "'",
        ')', '}', '+', '"', '{', '=', '^', '@', '<', ">="]

COUNTER = {"value": 0}
total_extra = set()
total_extra_keyword = set()
negative_pool = set()
def verify(sql, columns, tables):
  extra = set()
  tokens = sql.split()
  for token in tokens:
    token = token.lower()
    if token in PUNC:
      continue
    elif token in LOWER_KEYWORDS:
      continue
    elif token in columns:
      continue
    elif token in tables:
      continue
    else:
      extra.add(token)
  if len(extra) != 0:
    global COUNTER
    COUNTER["value"] += 1
    total_extra.update(extra)
  return list(extra)

def add_negative(example):
  if len(example["processed_sql"]) > 300:
    return None
  neg_size = random.randrange(5, 15)
  example["negative"] = random.sample(negative_pool, neg_size)
  return example

if __name__ == "__main__":
  parser = argparse.ArgumentParser()
  parser.add_argument("--input_file", type=str)
  parser.add_argument("--only_keep_clean", default=False, action="store_true")
  parser.add_argument("--length_limit", type=int, default=-1)
  parser.add_argument("--output_file", type=str)

  args = parser.parse_args()

  fout = open(args.output_file, "w")
  examples = []
  with open(args.input_file) as fin:
    for line in tqdm(fin):
      example = json.loads(line)
      extra = verify(example["processed_sql"], example["columns"], example["tables"])

      example["extra"] = extra
      examples.append(example)
      negative_pool.update(set(extra))
      negative_pool.update(set(example["tables"]))
      negative_pool.update(set(example["columns"]))
    pool = mp.Pool(90)


    # for example in tqdm(examples):
    #   if args.length_limit > 0:
    #     if len(example["processed_sql"]) > args.length_limit:
    #       continue
    #   neg_size = random.randrange(5, 15)
    #   example["negative"] = random.sample(negative_pool, neg_size)
    #   if args.only_keep_clean:
    #     if len(example["extra"]) == 0:
    #       fout.write(json.dumps(example) + "\n")
    #   else:
    #     fout.write(json.dumps(example) + "\n")
    processed = pool.map(add_negative, examples)
    for example in tqdm(processed):
      if example is not None:
        fout.write(json.dumps(example) + "\n")
  # print(COUNTER)

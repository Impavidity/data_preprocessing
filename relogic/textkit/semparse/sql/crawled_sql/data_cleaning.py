import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--input_file", type=str)
parser.add_argument("--output_file", type=str)

args = parser.parse_args()

from relogic.textkit.semparse.sql.crawled_sql.sql_preprocess import process
import json
from tqdm import tqdm
import multiprocessing as mp

examples = []
fout = open(args.output_file, "w")
with open(args.input_file) as fin:
  for line in tqdm(fin):
    examples.append(json.loads(line))

def process_ex(ex):
  output = process(ex)
  if output is not None:
    ex.update(output)
  return ex

pool = mp.Pool(2)
index = 0
size = 2
while index < len(examples):
  # segment = examples[index:index+size]
  # parsed = pool.map(process_ex, segment)
  print(examples[index])
  try:
    parsed = [process_ex(examples[index])]
  except:
    continue
  for example in parsed:
    if "processed_sql" in example:
      fout.write(json.dumps(example) + "\n")
  index += size
  print("Processed {}".format(index))
# parsed = []
# for example in tqdm(examples):
#   parsed.append(process_ex(example))








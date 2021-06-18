## Instruction

### Dependency
```
pip install moz_sql_parser
pip install sqlparse
```

### Run github SQL cleaning

```
python -m relogic.textkit.semparse.sql.crawled_sql.data_cleaning \
--input_file data/github_sql_examples.jsonl \
--output_file data/clean.jsonl
```

It is okay to see the warning in the log.
You will get normalized sql in the `data/clean.jsonl`. Basically the normalization is to masked out the 
values and some functions that are not common used.



from relogic.textkit.semparse.sql.crawled_sql.sql_helper import get_query_tables, get_query_columns, get_query_tokens, generalize_sql
from relogic.textkit.semparse.sql.crawled_sql.verify_sequence import verify
import sqlparse
import copy
from moz_sql_parser import format, parse
import re
import json

def extract_tables(sql):
    return get_query_tables(sql)

def extract_columns(sql):
    return get_query_columns(sql)


def _finditem(obj, key):
  if isinstance(obj, str) or isinstance(obj, int) or isinstance(obj, float):
    return
  for k, v in obj.items():
    if isinstance(v, dict):
      item = _finditem(v, key)
      if item is not None:
        yield from item
    elif isinstance(v, list):
      for list_item in v:
        item = _finditem(list_item, key)
        if item is not None:
          yield from item
  if key in obj: yield obj[key]

def _read_items(items, name_to_alias):
  for item in items:
    if isinstance(item, str):
      continue
    elif isinstance(item, dict):
      if "value" not in item:
        continue
      if isinstance(item["value"], str):
        if "name" in item and isinstance(item["name"], str):
          # if item["name"] in name_to_alias and name_to_alias[item["name"]] != item["value"]:
          name_to_alias[item["name"]] = item["value"]
      else:
        if "name" in item and isinstance(item["name"], str):
          name_to_alias[item["name"]] = item["name"]
    elif isinstance(item, list):
      _read_items(item, name_to_alias)

def extract_table_alias(parse):
    alias_to_table = {}
    items = _finditem(parse, "from")
    _read_items(items, alias_to_table)
    return alias_to_table

def _deleteitem(obj, key):
  if isinstance(obj, str) or isinstance(obj, int) or isinstance(obj, float):
    return
  for k, v in obj.items():
    if isinstance(v, dict):
      _deleteitem(obj[k], key)
    elif isinstance(obj[k], list):
      for idx, list_item in enumerate(obj[k]):
        _deleteitem(obj[k][idx], key)

  if key in obj:
    name = obj.pop(key)
    assert(isinstance(name, str))

def extract_column_alias(parse):
    alias_to_column = {}
    items = _finditem(parse, "select")
    _read_items(items, alias_to_column)
    return alias_to_column

def remove_alias(parse):
  copy_parse = copy.deepcopy(parse)
  _deleteitem(copy_parse, "name")
  processed_sql = format(copy_parse)
  return processed_sql

def high_level_process(sql):
  # query_tokens = sqlparse.parse(sql)[0].tokens

  # print(query_tokens)
  formatted_sql = sqlparse.format(sql, reindent=False, keyword_case='upper', identifier_case="lower",
                                  use_space_around_operators=True)
  generalized_sql = generalize_sql(formatted_sql)
  # generalized_sql = formatted_sql
  # generalized_sql = formatted_sql
  # print(generalized_sql)
  tables = extract_tables(generalized_sql)
  columns = extract_columns(generalized_sql)

  # print(generalized_sql)
  # obj = {"select": [{"value": "patients_tested.gender", "name": "Gender"}, {"value": "patients_tested.patients_count", "name": "TB Patients Tested for HIV"}], "from": {"value": {"select": [{"value": "person_gender.gender"}, {"value": {"count": {"distinct": "person.person_id"}}, "name": "patients_count"}], "from": ["visit", {"inner join": "person", "on": {"and": [{"eq": ["visit.patient_id", "person.person_id"]}, {"between": [{"date": "visit.date_started"}, "@start_date", "@end_date"]}]}}, {"inner join": "encounter", "on": {"eq": ["visit.visit_id", "encounter.visit_id"]}}, {"inner join": "coded_obs_view", "on": {"and": [{"eq": ["coded_obs_view.person_id", "person.person_id"]}, {"eq": ["coded_obs_view.concept_full_name", {"literal": "Coded Diagnosis"}]}, {"in": ["coded_obs_view.value_concept_full_name", {"literal": ["Tuberculosis", "Multi Drug Resistant Tuberculosis", "Extremely Drug Resistant Tuberculosis"]}]}, {"between": ["coded_obs_view.obs_datetime", "@start_date", "@end_date"]}]}}, {"inner join": {"name": "certainty_obs", "value": "coded_obs_view"}, "on": {"and": [{"eq": ["coded_obs_view.obs_group_id", "certainty_obs.obs_group_id"]}, {"eq": ["certainty_obs.concept_full_name", {"literal": "Diagnosis Certainty"}]}, {"eq": ["certainty_obs.value_concept_full_name", {"literal": "Confirmed"}]}]}}, {"inner join": "orders", "on": {"and": [{"eq": ["orders.patient_id", "person.person_id"]}, {"eq": ["orders.order_type_id", 3]}, {"in": ["orders.order_action", {"literal": ["NEW", "REVISED"]}]}, {"between": ["orders.date_created", "@start_date", "@end_date"]}]}}, {"inner join": "concept_view", "on": {"and": [{"eq": ["orders.concept_id", "concept_view.concept_id"]}, {"in": ["concept_view.concept_full_name", {"literal": ["HIV (Blood)", "HIV (Serum)"]}]}]}}, {"right outer join": {"name": "person_gender", "value": {"select": {"value": {"distinct": "gender"}}, "from": "person", "where": {"neq": ["gender", {"literal": ""}]}}}, "on": {"eq": ["person_gender.gender", "person.gender"]}}], "groupby": {"value": "person_gender.gender"}}, "name": "patients_tested"}}
  # parse = {"select": [{"value": "d.id", "name": "id"}, {"value": {"concat": ["se.name", {"literal": "_"}, "sr.name", {"literal": "_"}, "g.name", {"literal": "_"}, "r.name"]}, "name": "name"}, {"value": "d.description", "name": "description"}, {"value": "se.name", "name": "satellite"}, {"value": "sr.name", "name": "sensor"}, {"value": "g.name", "name": "geometric_processing"}, {"value": "r.name", "name": "radiometric_processing"}], "from": [{"value": "_dataset", "name": "d"}, {"value": "satellite", "name": "se"}, {"value": "sensor", "name": "sr"}, {"value": "geometric_processing", "name": "g"}, {"value": "radiometric_processing", "name": "r"}], "where": {"and": [{"eq": ["d.satellite_id", "se.id"]}, {"eq": ["d.sensor_id", "sr.id"]}, {"eq": ["d.geometric_processing_id", "g.id"]}, {"eq": ["d.radiometric_processing_id", "r.id"]}]}, "orderby": {"value": "d.id"}}
  tree = parse(sql)
  alias_to_table = extract_table_alias(tree)
  for alias in alias_to_table:
    alias_to_table[alias.lower()] = alias_to_table[alias]
  column_changes = {}
  normalized_columns = []
  alias_to_column = extract_column_alias(tree)
  for column in columns:
    if "." in column:
      table_alias, column_name = column.split(".")
      table_alias = table_alias.lower()
      if table_alias in alias_to_table:
        column = alias_to_table[table_alias] + "." + column_name
        column_changes["{}.{}".format(table_alias, column_name)] = column
      else:
        column = table_alias + "." + column_name
    if column not in alias_to_column:
      normalized_columns.append(column)



  clean_sql = remove_alias(tree)
  formatted_sql = sqlparse.format(clean_sql, reindent=False, keyword_case='upper', identifier_case="lower",
                                  use_space_around_operators=True)
  generalized_sql = generalize_sql(formatted_sql)
  generalized_sql = " ".join(generalized_sql.replace("(", " ( ").replace(")", " ) ").replace(",", " , ").split()) + " "
  for column in column_changes:
    generalized_sql = generalized_sql.replace(" {} ".format(column), " {} ".format(column_changes[column]))
  # filtered_columns = []
  # filtered_tables = []
  #
  # for token in generalized_sql.split():
  #   if token in normalized_columns:
  #     filtered_columns.append(token)
  #   if token in tables:
  #     filtered_tables.append(token)
  # print(generalized_sql)
  # print(tables)
  # print(normalized_columns)
  # print(len(set(tables) & set(normalized_columns)) == 0)
  # print("----")
  return {
    "processed_sql": generalized_sql.strip(),
    "columns": normalized_columns,
    "tables": tables
  }

def low_level_process(sql):
  formatted_sql = sqlparse.format(sql, reindent=False, keyword_case='upper', identifier_case="lower",
                                  use_space_around_operators=True)
  generalized_sql = generalize_sql(formatted_sql)
  # generalized_sql = formatted_sql
  # generalized_sql = formatted_sql
  # print(generalized_sql)
  # print(generalized_sql)
  tables = extract_tables(generalized_sql)
  columns = extract_columns(generalized_sql)
  generalized_sql = " ".join(generalized_sql.replace("(", " ( ").replace(")", " ) ").replace(",", " , ").split())
  return {
    "processed_sql": generalized_sql,
    "columns": columns,
    "tables": tables
  }

def add_negative(example, related_columns):
  negatives = []
  if len(related_columns) > 3:
    for column in related_columns:
      table_name, column_name = column.split(".", 1)
      if table_name not in example["tables"] and table_name not in negatives:
        negatives.append(table_name)
      if not any(column_name in x for x in example["columns"]) and column not in negatives:
        negatives.append(column)
    example["negative"] = negatives
    return example
  else:
    return None

def process(ex):
  sql = ex["sql"]
  related_columns = ex.get("related_columns", None)
  try:
    example = high_level_process(sql)
  except:
    try:
      example = low_level_process(sql)
    except:
      return None
  extra = verify(example["processed_sql"], example["columns"], example["tables"])
  example["extra"] = extra
  if related_columns is not None:
    example = add_negative(example, related_columns)
  return example


if __name__ == "__main__":
  sqls = []
  sql = "SELECT feature_id AS glycine_trna_primary_transcript_id, feature.* FROM feature INNER JOIN cvterm ON (feature.type_id = cvterm.cvterm_id) WHERE cvterm.name = 'glycine_tRNA_primary_transcript'"
  sqls.append(sql)
  sql = "select * from students except all (select StudentID, StudentName, GroupID from students natural join marks natural join courses where (courses.coursename = 'Bazy dannykh'))"
  sqls.append(sql)
  sql = "SELECT feature_id AS glycine_trna_primary_transcript_id, feature.* FROM feature INNER JOIN cvterm ON (feature.type_id = cvterm.cvterm_id) WHERE cvterm.name = 'glycine_tRNA_primary_transcript'"
  sqls.append(sql)
  sql = "SELECT film.film_id ,film.title ,inventory.inventory_id FROM film LEFT OUTER JOIN inventory ON film.film_id = inventory.film_id"
  sqls.append(sql)
  sql = "SELECT first_name, replace(phone_number, '.', '-') from employees"
  sqls.append(sql)
  sql = "select v_EntGid, v_ModelGid, v_TaskGid_T4, '**CreateGid**', '**CreateCode**', '@@', 1 from dual"
  sqls.append(sql)
  sql = "select deleteXML(V_RESOURCE_XML,'/r Resource/r Parents',XDB_NAMESPACES.RESOURCE_PREFIX_R) into V_RESOURCE_XML from dual"
  sqls.append(sql)
  sql = "select * from table(DBMS_XPLAN.DISPLAY_CURSOR('4suk9kmn1wjh5', null, 'SERIAL'))"
  sqls.append(sql)
  sql = "SELECT B.YWLSH INTO V_YWLSH FROM UTB_YH_FUNDTRADE_DETAIL B WHERE B.PLLSH = V_D.PLLSH AND ROWNUM = 1"
  sqls.append(sql)
  sql = "SELECT _tenantId,id,'ALun ','50','50'/*Pai Xu */, '1', SYSDATE(), 1, '0' FROM dictionary WHERE value ='dic_project_round' AND foo IN ('880987','882618','708228','522330')"
  sqls.append(sql)
  sql = "select firstname, lastname, Description, salary from job join employee on employee.jobid = job.id where description = @JobDescription"
  sqls.append(sql)
  sql = "select sum(gets) \"Gets\", avg(getmisses) \"Get Misses\", (1-(sum(getmisses)/sum(gets))) * 100 \"Hit Ratio\" from v$rowcache"
  sqls.append(sql)
  sql = "SELECT * FROM json_test WHERE JSON_LENGTH( JSON_KEYS( col_jsonvalue ) ) = 4 LIMIT 0 /* QNO 417 CON_ID 14 */"
  sqls.append(sql)
  sql = "SELECT DATE( ( SUBDATE( col_varchar_64_key , col_varchar_1_key ) ) ) AS field1, -6184005238333112320 / 'owpqdtjcxesnizzfscpdejljmtjjobtqvwgjsqfuhsxzqyeimorouyryszsaheqttgayltcuslluunjvtfaz' AS field2 FROM table1000_int_autoinc WHERE ADDDATE( col_time , '2026-11-16 14 43 00.008148' ) ORDER BY field1, field2 LIMIT 3 /* QNO 915 CON_ID 164 */"
  sqls.append(sql)
  sql = "select count(p1.line#) as lines_present from plsql_profiler_lines_cross_run p1 where (p1.unit_type in ( 'PACKAGE BODY', 'TYPE BODY', 'PROCEDURE', 'FUNCTION' ) )"
  sqls.append(sql)
  # We will ignore all sql that contains #.
  sql = "SELECT @AUDIT_LOG_TRANSACTION_ID, convert(nvarchar(1500), IsNull('Cust_ID='+CONVERT(nvarchar(4000), NEW.Cust_ID, 0), 'Cust_ID Is Null')), 'City', CONVERT(nvarchar(4000), NEW.City, 0), 'A' , CONVERT(nvarchar(500), CONVERT(nvarchar(4000), NEW.Cust_ID, 0)) FROM inserted NEW WHERE NEW.City Is Not Null"
  sqls.append(sql)
  sql = "SELECT id,(SELECT app_roles.id FROM toasthub_client1.app_roles WHERE role_name='user' AND domain='toasthub-social') as roleid from toasthub_client1.app_users where username='freddy.jones@gmail.com'"
  sqls.append(sql)
  sql = "SELECT l.AD_Language,t.AD_Column_ID, t.Name, 'N',t.AD_Client_ID,t.AD_Org_ID,t.Created,t.Createdby,t.Updated,t.UpdatedBy FROM AD_Language l, AD_Column t WHERE l.IsActive='Y' AND l.IsSystemLanguage='Y' AND l.IsBaseLanguage='N' AND t.AD_Column_ID=1120193 AND NOT EXISTS (SELECT * FROM AD_Column_Trl tt WHERE tt.AD_Language=l.AD_Language AND tt.AD_Column_ID=t.AD_Column_ID)"
  sqls.append(sql)
  sql = "SELECT patients_tested.gender AS \"Gender\", patients_tested.patients_count AS \"TB Patients Tested for HIV\"\nFROM\n(SELECT person_gender.gender, COUNT(DISTINCT person.person_id) AS patients_count\nFROM visit\nINNER JOIN person ON visit.patient_id = person.person_id\nAND DATE(visit.date_started) BETWEEN @start_date AND @end_date\nINNER JOIN encounter ON visit.visit_id = encounter.visit_id\nINNER JOIN coded_obs_view ON coded_obs_view.person_id = person.person_id\nAND coded_obs_view.concept_full_name = 'Coded Diagnosis'\nAND coded_obs_view.value_concept_full_name IN ('Tuberculosis','Multi Drug Resistant Tuberculosis', 'Extremely Drug Resistant Tuberculosis')\nAND coded_obs_view.obs_datetime BETWEEN @start_date AND @end_date\nINNER JOIN coded_obs_view AS certainty_obs ON coded_obs_view.obs_group_id = certainty_obs.obs_group_id\nAND certainty_obs.concept_full_name = 'Diagnosis Certainty'\nAND certainty_obs.value_concept_full_name = 'Confirmed'\nINNER JOIN orders ON orders.patient_id = person.person_id\nAND orders.order_type_id = 3\nAND orders.order_action IN ('NEW', 'REVISED')\nAND orders.date_created BETWEEN @start_date AND @end_date\nINNER JOIN concept_view ON orders.concept_id = concept_view.concept_id\nAND concept_view.concept_full_name IN ('HIV (Blood)', 'HIV (Serum)')\nRIGHT OUTER JOIN (SELECT DISTINCT gender FROM person WHERE gender != '' ) AS person_gender ON person_gender.gender = person.gender\nGROUP BY person_gender.gender) AS patients_tested"
  sqls.append(sql)
  sql = "SELECT\nd.id id,\nCONCAT(se.name, '_', sr.name, '_', g.name, '_', r.name) name,\nd.description description,\nse.name satellite,\nsr.name sensor,\ng.name geometric_processing,\nr.name radiometric_processing\nFROM\n_dataset d,\nsatellite se,\nsensor sr,\ngeometric_processing g,\nradiometric_processing r\nWHERE\nd.satellite_id = se.id\nAND d.sensor_id = sr.id\nAND d.geometric_processing_id = g.id\nAND d.radiometric_processing_id = r.id\nORDER BY d.id"
  sqls.append(sql)
  sql = "SELECT title, AVG(stars) AS average\nFROM Movie\nINNER JOIN Rating USING(mId)\nGROUP BY mId\nHAVING average = (\nSELECT MAX(average_stars)\nFROM (\nSELECT title, AVG(stars) AS average_stars\nFROM Movie\nINNER JOIN Rating USING(mId)\nGROUP BY mId\n)\n)"
  sqls.append(sql)
  sql = 'SELECT (abs(case when 11 not between t1.d and case coalesce((select case c+case when b in (case when a<=t1.b then (t1.a) when f not in (d,t1.f, -(a)) then (t1.a) else c end,t1.a,b) then 11 when (t1.c not between 17 and  -d) then t1.d else e end+e*t1.a when 19 then b else t1.e end from t1 where not exists(select 1 from t1 where not exists(select 1 from t1 where  -(19)=d))),t1.d) when c then a else  -17 end or a>=19 then 17 else b end+ -13)/abs(b)) FROM t1 WHERE not exists(select 1 from t1 where not e+case 13 when 11 then +t1.d else t1.f+11 end*a-19+t1.c+a in (~e | coalesce((select ~a*+(abs( -case case b*e when f then e else  -13 end when t1.d then t1.b else t1.b end)/abs(17))+t1.b from t1 where t1.a=e),d),e,e))'
  sqls.append(sql)
  for sql in sqls:
    sql = " ".join(sql.split())
    output = process({"sql": sql})
    if output is not None:
      print(json.dumps(output, indent=2))
    else:
      print(sql)
    print("----")



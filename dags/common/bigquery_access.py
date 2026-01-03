#DBアクセス機能全般

from pandas_gbq import to_gbq
from google.cloud import bigquery
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from datetime import datetime, date
import pandas as pd
import logging

#▼▼▼SELECT▼▼▼

#単一テーブルの全レコード取得
def fetch_all_record(dataset: str, table: str) -> pd.DataFrame:
	hook = BigQueryHook(gcp_conn_id="google_cloud_default", use_legacy_sql=False, project_id=None)
	project_id = hook.project_id
	return hook.get_pandas_df(f"SELECT * FROM `{project_id}.{dataset}.{table}`;")


#1テーブルの特定のカラムを全レコード取得
def fetch_columns(dataset: str, table: str, columns: list) -> pd.DataFrame:
	hook = BigQueryHook(gcp_conn_id="google_cloud_default", use_legacy_sql=False, project_id=None)
	project_id = hook.project_id
	colmuns_str = ", ".join(columns)
	return hook.get_pandas_df(f"SELECT {colmuns_str} FROM `{project_id}.{dataset}.{table}`;")


#エラーseq番号の最大値を取得
def get_max_error_seq(dataset: str, table: str) -> int:
	hook = BigQueryHook(gcp_conn_id="google_cloud_default", use_legacy_sql=False, project_id=None)
	project_id = hook.project_id
	result = hook.get_pandas_df(f"SELECT MAX(error_seq) AS max_error_seq FROM `{project_id}.{dataset}.{table}`;")
	max_error_seq = result["max_error_seq"].iloc[0]
	if pd.isna(max_error_seq):
		max_error_seq = 0  #テーブルが空の場合は0始まり
	return max_error_seq


#エラーレコード登録
def insert_error_records(df_invalid: pd.DataFrame, dataset: str, table: str):
	#エラーテーブルの既存seq番号の最大値を取得
	max_error_seq = get_max_error_seq(dataset, table)
	
	#既存の最大値+1からの連番で、エラーテーブル用のseq番号を設定
	df_invalid = df_invalid.reset_index(drop=True)
	df_invalid["error_seq"] = df_invalid.index + 1 + max_error_seq

	#エラーテーブルに存在しない列は削除
	# ※エラーテーブルとワークテーブルの定義の差異は、常にseq(データ取込時に振った連番)とerror_seq(エラーデータとしての連番)の有無のみ
	df_to_insert = df_invalid.drop(columns=["seq"], errors="ignore")

	#エラーテーブルへINSERT
	hook = BigQueryHook(gcp_conn_id="google_cloud_default", use_legacy_sql=False, project_id=None)
	project_id = hook.project_id
	to_gbq(
		df_to_insert,
		destination_table=f"{dataset}.{table}",
		project_id=project_id,
		if_exists='append'
	)
	logging.info("Insert %d rows into %s.%s", len(df_to_insert), dataset, table)


#判定に用いるキーワード群をマスタから取得
def get_keywords(keyword_category: str) -> pd.Series:
	hook = BigQueryHook(gcp_conn_id="google_cloud_default", use_legacy_sql=False, project_id=None)
	project_id = hook.project_id

	df_keywords = hook.get_pandas_df(f"SELECT keyword FROM `conf.m_keyword` WHERE keyword_category = '{keyword_category}';")
	return df_keywords["keyword"]


#文字列変換マスタ取得
def get_replace_strings(category: str) -> pd.DataFrame:
	hook = BigQueryHook(gcp_conn_id="google_cloud_default", use_legacy_sql=False, project_id=None)
	sql = f"""
SELECT before_string, after_string
FROM `conf.m_replace_string`
WHERE category_code = '{category}'
"""
	return hook.get_pandas_df(sql)


#2テーブル結合した情報を取得
def fetch_join_tables(mapping: dict) -> pd.DataFrame:
	hook = BigQueryHook(gcp_conn_id="google_cloud_default")
	client = hook.get_client()

	src = mapping["from"]
	jn = mapping["join"]
	cols = mapping["columns"]

	#SELECT句作成
	select_cols = ",\n".join(
		f"{c['expr']} AS {c['as']}" if "as" in c else c["expr"] for c in cols
	)

	#ON句作成
	on_clause = " AND ".join(
		f"{l} = {r}" for l, r in jn["on"]
	)

	sql = f"""
SELECT
{select_cols}
FROM `{src['dataset']}.{src['table']}` {src['alias']}
{jn.get('type', 'INNER')} JOIN `{jn['dataset']}.{jn['table']}` {jn['alias']}
  ON {on_clause}
"""

	return client.query(sql).result().to_dataframe()


#▼▼▼INSERT▼▼▼

#DFに基づいて単純INSERT
def insert_by_df(df: pd.DataFrame, dataset: str, table: str):
	hook = BigQueryHook(gcp_conn_id="google_cloud_default")
	credentials = hook.get_credentials()
	project_id = hook.project_id 

	to_gbq(
		df,
		destination_table=f"{dataset}.{table}",
		project_id=project_id,
		credentials=credentials,
		if_exists="append",
	)
	logging.info("Insert %d rows into %s.%s", len(df), dataset, table)


#カラムの対応表に基づいてINSERT-SELECT（主にデータレイクからワークテーブルへのデータ取込用）
#更新日時が"本日"となっている元データを対象とする
def insert_select_work(mapping: dict, ds):
	hook = BigQueryHook(gcp_conn_id="google_cloud_default")
	client = hook.get_client()
	project_id = hook.project_id

	src = mapping["source"]
	tgt = mapping["target"]
	cols = mapping["columns"]

	#サブクエリ用にROW_NUMBER列とその他列を分ける
	inner_cols = []
	outer_cols = []

	for c in cols:
		if c["src"] is None:
			#srcがNoneの場合は関数や計算式と想定して処理
			inner_cols.append(f"{c['dst']}")
			outer_cols.append(c['dst'].split(" AS ")[-1])
		else:
			inner_cols.append(f"{c['src']}")
			outer_cols.append(c['dst'])

	inner_select = ",\n".join(inner_cols)
	outer_select = ",\n".join(outer_cols)

	sql = f"""
INSERT INTO `{project_id}.{tgt['dataset']}.{tgt['table']}` (
	{outer_select}
)
SELECT {outer_select}
FROM (
SELECT {inner_select}
FROM `{project_id}.{src['dataset']}.{src['table']}`
WHERE DATE(update_datetime) = @today
) t
"""
	job_config = bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("today", "DATE", ds)])

	job = client.query(sql, job_config=job_config)
	job.result()
	cnt = job.num_dml_affected_rows or 0
	logging.info("Insert %d rows into %s.%s", cnt, tgt["dataset"], tgt["table"])


#▼▼▼UPDATE▼▼▼

#UPDATE（実際にはtmpテーブルへのINSERT->本テーブルにMERGE -> tmpテーブルのDELETE）
def update_by_df(df: pd.DataFrame, src_dataset: str, src_table: str, tgt_dataset: str, tgt_table: str, cols: list, merge_key: str):
	hook = BigQueryHook(gcp_conn_id="google_cloud_default")
	client = hook.get_client()
	project_id = hook.project_id

	#tmpテーブルにINSERT
	insert_by_df(df, src_dataset, src_table)

	#DWH側のテーブル定義を取得
	table = client.get_table(f"{tgt_dataset}.{tgt_table}")
	schema_map = {f.name: f.field_type for f in table.schema}

	#UPDATE SET句を生成
	#原則src側の値をSET。ただしsrc側がNULLまたは空文字の場合は、tgt側の値をSET。
	update_set_sql = ",\n	".join(f"""
{col} = COALESCE(
{f"NULLIF(S.{col}, '')" if schema_map[col] == "STRING" else f"S.{col}"},
T.{col})
"""
	for col in cols if col != merge_key
	)
	
	#MERGE実行
	update_sql=f"""
MERGE `{tgt_dataset}.{tgt_table}` T
USING `{src_dataset}.{src_table}` S
ON T.{merge_key} = S.{merge_key}
WHEN MATCHED THEN
UPDATE SET
{update_set_sql}
"""
	job = client.query(update_sql)
	job.result()

	cnt = job.num_dml_affected_rows or 0
	logging.info("Update %d rows into %s.%s", cnt, tgt_dataset, tgt_table)

	#tmpテーブル全件DELETE
	client.query(f"DELETE FROM `{src_dataset}.{src_table}` WHERE TRUE").result()


#▼▼▼DELETE▼▼▼

#数値型のキー項目をWHERE IN条件に設定してDELETE
def delete_by_int_key(df: pd.DataFrame, dataset: str, table: str, key_column: str):
	if df.empty:
		logging.info(f"DELETE対象なし")
		return

	keys_to_delete = df[key_column].tolist()

	hook = BigQueryHook(gcp_conn_id="google_cloud_default")
	client = hook.get_client()
	project_id = hook.project_id

	sql = f"""
DELETE FROM `{project_id}.{dataset}.{table}`
WHERE {key_column} IN UNNEST(@keys)
"""
	job_config = bigquery.QueryJobConfig(
		query_parameters=[
			bigquery.ArrayQueryParameter("keys", "INT64", keys_to_delete)
		]
	)

	job = client.query(sql, job_config=job_config)
	job.result()

	cnt = job.num_dml_affected_rows or 0
	logging.info("Delete %d rows into %s.%s", cnt, dataset, table)


#文字列型のキー項目をWHERE IN条件に設定してDELETE
def delete_by_str_key(df: pd.DataFrame, dataset: str, table: str, key_column: str):
	if df.empty:
		logging.info(f"DELETE対象なし")
		return

	keys_to_delete = df[key_column].astype(str).tolist()

	hook = BigQueryHook(gcp_conn_id="google_cloud_default")
	client = hook.get_client()
	project_id = hook.project_id

	sql = f"""
DELETE FROM `{project_id}.{dataset}.{table}`
WHERE {key_column} IN UNNEST(@keys)
"""
	job_config = bigquery.QueryJobConfig(
		query_parameters=[
			bigquery.ArrayQueryParameter("keys", "STRING", keys_to_delete)
		]
	)

	job = client.query(sql, job_config=job_config)
	job.result()

	cnt = job.num_dml_affected_rows or 0
	logging.info("Delete %d rows into %s.%s", cnt, dataset, table)


#work系テーブル削除
#全件削除は誤ってwork以外のdatasetに対して実行させないよう、datasetはwork固定とする
def delete_work_table(table: str):
	hook = BigQueryHook(gcp_conn_id="google_cloud_default")
	client = hook.get_client()

	job = client.query(f"DELETE FROM `work.{table}` WHERE TRUE")
	job.result()

	cnt = job.num_dml_affected_rows or 0
	logging.info("Delete %d rows into %s.%s", cnt, "work", table)

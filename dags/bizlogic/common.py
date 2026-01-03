#各ビジネスロジック共通処理

from common.utils import *
from common.name_collection import *
from common.bigquery_access import *
from datetime import datetime
from airflow.configuration import conf
import pandas as pd
import logging
import os

#CSV読み込みのファイルパス取得
def get_csv_filepath(filename: str) -> str:
	airflow_home = os.environ.get("AIRFLOW_HOME", os.path.expanduser("~/airflow"))
	csv_path = os.path.join(
		airflow_home,
		"csv",
		filename
	)
	return csv_path


#m_companyのUPSERT処理
#
#第1引数のDataFrameと既存m_companyテーブルを突合し、
#「会社名」「住所」「代表電話番号」「代表メールアドレス」のうち2項目が完全一致しているレコードがあれば、既存データと判定。
#既存データにあればUPDATE、なければINSERTする。
#
#返却値：
# 元の処理対象データ全件が格納されたDataFrame対して、既存の会社コード情報を反映したも（後続のm_person登録処理用）
#
#引数：
# df_company_new  :第3引数の"df"に対して、m_companyに必要な列だけ抽出、必須項目追加、列名変換を行ったDataFrame
# update_col_list :更新カラム名一覧
# df              :workおよびnormalizedテーブルから正規化済み情報をまとめて取得したDataFrame
#
def upsert_m_company(df_company_new: pd.DataFrame, update_col_list: list, df: pd.DataFrame) -> pd.DataFrame:
	#会社内での重複行をまとめる
	company_match_combinations = [
		["company_name","full_address"],
		["company_name","main_phone_number"],
		["company_name","main_email_address"],
		["full_address","main_phone_number"],
		["full_address","main_email_address"],
		["main_phone_number","main_email_address"],
	]
	df_company_new_clean = merge_rows(df_company_new, company_match_combinations)

	#既存会社マスタ取得
	df_m_company = fetch_all_record("dwh","m_company")

	#更新対象の会社を抽出
	df_company_update = create_update_df(df_m_company, df_company_new_clean, company_match_combinations, "company_code")

	#INSERT対象の会社を抽出
	df_company_insert = df_company_new_clean.loc[~df_company_new_clean.index.isin(df_company_update.index)].copy()

	#INSERT用のcompany_code作成
	if not df_company_insert.empty:
		df_company_insert.loc[:, "company_code"] = create_new_codes(df_m_company["company_code"], len(df_company_insert)).values

	#会社単位の全コード
	df_company_all = pd.concat([df_company_insert, df_company_update], ignore_index=True)

	#個人行に会社コードを反映
	df_company_new["company_code"] = None
	for cols in company_match_combinations:
		#必要な列が両方にあるか確認
		if all(c in df_company_new.columns for c in cols) and all(c in df_company_all.columns for c in cols):
			df_mapping = df_company_all[list(cols) + ["company_code"]].copy()
			df_merge = df_company_new.merge(df_mapping, on=list(cols), how="left", suffixes=("", "_new"))
			#company_codeが空の行だけ更新
			df_company_new["company_code"] = df_company_new["company_code"].combine_first(df_merge["company_code_new"])
		else:
			logging.warning(f"Skipping combination {cols} : Missing column")

	df_company_mapping = df_company_new[["seq", "company_code"]].copy()
	df = df.merge(df_company_mapping, on="seq", how="left")

	#INSERT実行
	if not df_company_insert.empty:
		df_company_insert_to_db = df_company_insert.drop(columns=["seq"], errors="ignore")
		insert_by_df(df_company_insert_to_db, "dwh", "m_company")
	else:
		logging.info("Insert 0 rows into %s.%s", "dwh", "m_company")

	#UPDATE実行
	if not df_company_update.empty:
		df_company_update_to_db = df_company_update.drop(columns=["seq"], errors="ignore")
		update_col_list = [
			"company_name",
			"postal_code",
			"prefecture_code",
			"full_address",
			"main_phone_number",
			"main_email_address",
			"update_datetime",
		]
		update_by_df(
			df_company_update_to_db,
			"work",
			"tmp_m_company_update",
			"dwh",
			"m_company",
			update_col_list,
			"company_code"
		)
	else:
		logging.info("Update 0 rows into %s.%s", "dwh", "m_company")

	return df


#m_personのUPSERT処理
#
#第1引数のDataFrameと既存m_personテーブルを突合し、
#「会社名」「住所」「代表電話番号」「代表メールアドレス」のうち2項目が完全一致しているレコードがあれば、既存データと判定。
#既存データにあればUPDATE、なければINSERTする。
#
#返却値：
# 元の処理対象データ全件が格納されたDataFrame対して、既存の会社コード情報を反映したも（後続のm_person登録処理用）
#
#引数：
# df_personal_new :workおよびnormalizedテーブルから正規化済み情報をまとめて取得した結果に対して、m_companyに必要な列だけ抽出、必須項目追加、列名変換を行ったDataFrame
# update_col_list :更新カラム名一覧
#
def upsert_m_person(df_personal_new: pd.DataFrame, update_col_list: list):
	#個人dfの中で、「氏名」「個人別電話番号」「個人別メールアドレス」の2項目一致チェック。個人df内に該当あればCOALESCEして、不要行削除
	personal_match_combinations =  [
		["name","phone_number"],
		["name","email_address"],
		["phone_number","email_address"],
	]
	df_personal_new = merge_rows(df_personal_new, personal_match_combinations)

	#m_personの既存データ取得
	df_m_person = fetch_all_record("dwh","m_person")

	#個人dfと既存データで2項目一致チェック。該当あればCOALESCE（新しく取り込む方を主に生かす）して、UPDATE用dfへと抽出
	df_personal_update = create_update_df(df_m_person, df_personal_new, personal_match_combinations, "person_id")

	#↑で該当なかったものはINSERT用dfへと抽出
	df_personal_insert = df_personal_new.loc[~df_personal_new.index.isin(df_personal_update.index)].copy()

	#既存の人物IDのMAXを取ってINSERT用dfに埋め込む
	df_personal_insert.loc[:, "person_id"] = create_new_codes(df_m_person["person_id"], len(df_personal_insert)).values
	#df_company_insert.loc[:, "company_code"] = create_new_codes(df_m_company["company_code"], len(df_company_insert)).values

	if not df_personal_insert.empty:
		#INSERT実行
		df_personal_insert = df_personal_insert.drop(columns=["seq"])
		insert_by_df(df_personal_insert, "dwh", "m_person")

	#UPDATE実行
	df_personal_update = df_personal_update.drop(columns=["seq"], errors="ignore")
	update_col_list = [
		"name",
		"name_kana",
		"company_code",
		"department",
		"job_post",
		"phone_number",
		"email_address",
		"update_datetime",
	]
	update_by_df(df_personal_update, "work", "tmp_m_person_update", "dwh", "m_person", update_col_list, "person_id")
	
	#m_personの更新結果を元に、m_companyの社長情報を更新
	update_president()


#m_company社長情報更新
# ※m_company登録⇒m_person登録の順で処理するので、m_person登録後に改めてm_companyを登録する必要がある
def update_president():
	#m_personを全件取得
	df_m_person = fetch_all_record("dwh","m_person")
	
	#社長属性の人だけに絞り込む
	df_president = extract_president(df_m_person)
	
	#更新先にカラム名を合わせる
	df_president_update = pd.DataFrame(index=df_president.index)
	df_president_update["president_person_id"] = df_president["person_id"]
	df_president_update["company_code"] = df_president["company_code"]

	#「社長属性の人物一覧」に基づいて、会社マスタ上の社長人物IDを更新
	update_col_list = ["president_person_id"]
	update_by_df(df_president_update, "work", "tmp_m_company_update", "dwh", "m_company", update_col_list, "company_code")


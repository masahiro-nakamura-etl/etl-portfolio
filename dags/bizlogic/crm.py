#CRM（営業支援・顧客管理システム）関係のビジネスロジック

from common.utils import *
from common.validation import *
from common.name_collection import *
from common.bigquery_access import *
from bizlogic.common import *
from datetime import datetime
import pandas as pd
import logging
import os

CSV_FILE_NAME = "crm.csv"

#CSVからデータレイクへINSERT
def insert_crm_lead_from_csv(now):
	#パスを設定
	csv_path = get_csv_filepath(CSV_FILE_NAME)

	#ファイル存在確認、なければ処理中断
	if not os.path.exists(csv_path):
		return

	#CSVを読み込み、登録日付・更新日付を付与
	df = pd.read_csv(
		csv_path,
		parse_dates=["register_datetime", "last_contact_datetime"]
	)
	df = df.assign(
		insert_datetime=now,
		update_datetime=now
	)

	#単純INSERT
	insert_by_df(df, "lake", "crm_lead")


#データレイクからworkテーブルへ取込
def insert_work_crm_lead(now):
	#日付取得
	ds = now.date()
	#INSERT-SELECTの取得元テーブル、登録先テーブル、列名の紐づけ設定値
	insert_select_map = {
		"source": {
			"dataset" : "lake",
			"table" : "crm_lead",
		},
		"target": {
			"dataset" : "work",
			"table" : "work_crm_lead",
		},
		"columns": [
			{"src": None,                 "dst": "ROW_NUMBER() OVER (ORDER BY update_datetime) AS seq"},
			{"src": "company_name",       "dst": "company_name"},
			{"src": "department",         "dst": "department"},
			{"src": "name",               "dst": "name"},
			{"src": "name_kana",          "dst": "name_kana"},
			{"src": "job_post",           "dst": "job_post"},
			{"src": "email",              "dst": "email"},
			{"src": "phone_number",       "dst": "phone_number"},
			{"src": "url",                "dst": "url"},
			{"src": "postal_code",        "dst": "postal_code"},
			{"src": "prefecture",         "dst": "prefecture"},
			{"src": "address",            "dst": "address"},
			{"src": "industry",           "dst": "industry"},
			{"src": "employees_volume",   "dst": "employees_volume"},
			{"src": "annual_revenue",     "dst": "annual_revenue"},
			{"src": "contact_permission", "dst": "contact_permission"},
		]
	}
	#INSERT-SELECT実行
	insert_select_work(insert_select_map, ds)


#取り込んだデータのバリデーション
def validate_work_crm_lead(now):
	src_dataset = "work"
	src_table = "work_crm_lead"
	tgt_dataset = "error"
	tgt_table = "error_crm_lead"
	
	df = fetch_all_record(src_dataset, src_table)
	error_rows = []

	#メールアドレス不正チェック
	df_email_ng = validate_mail_address(df, "email")
	if not df_email_ng.empty:
		error_rows.append(df_email_ng)

	#電話番号不正チェック
	df_phone_ng = validate_phone_number(df, "phone_number")
	if not df_phone_ng.empty:
		error_rows.append(df_phone_ng)

	#都道府県名チェック
	df_pref_ng = validate_prefecture(df, "prefecture")
	if not df_pref_ng.empty:
		error_rows.append(df_pref_ng)

	#氏名以外の個人情報なしチェック
	df_name_only_ng = validate_name_only(df, "email", ["phone_number"])
	if not df_name_only_ng.empty:
		error_rows.append(df_name_only_ng)

	#エラーリストを1件のDFに統合
	if not error_rows:
		logging.info("No Validation Error")
		return
	df_invalid = pd.concat(error_rows, ignore_index=True)#.drop_duplicates(subset="seq", keep="first")

	#エラー登録日時を付与
	df_invalid = df_invalid.assign(insert_datetime=now)

	#エラーテーブルにINSERT
	insert_error_records(df_invalid, tgt_dataset, tgt_table)

	#Workテーブルからエラーデータを削除
	delete_by_int_key(df_invalid, src_dataset, src_table, "seq")


#住所・氏名・電話番号の正規化および会社データ・個人データの切り分け
def normalize_work_crm_lead():
	src_dataset = "work"
	src_table = "work_crm_lead"
	tgt_dataset = "work"
	tgt_table = "normalized_crm_lead"

	#workテーブルに登録した情報を取得
	df = fetch_columns(src_dataset, src_table, ["seq", "company_name", "name", "name_kana", "phone_number", "prefecture", "address", "email", "department", "job_post"])
	df_to_insert = pd.DataFrame(index=df.index)

	#電話番号正規化
	df["phone_number"] = normalize_phone_number(df["phone_number"])
	
	#個人電話番号/会社電話番号を判定
	df_to_insert["normalized_personal_phone_number"] = extract_personal_phone(df["phone_number"])
	df_to_insert["normalized_company_phone_number"] = extract_company_phone(df["phone_number"])

	#氏名正規化
	df_to_insert["normalized_personal_name"] = normalize_personal_name(df["name"])

	#氏名カナ正規化
	df_to_insert["normalized_personal_name_kana"] = normalize_personal_name_kana(df["name_kana"])

	#会社名正規化
	df_to_insert["normalized_company_name"] = normalize_company_name(df["company_name"])

	#住所正規化
	#  ※crm_leadはprefectureに都道府県、addressに市町村以下の情報を持つ
	df_to_insert["normalized_full_address"] = normalize_address(df["prefecture"] + df["address"])

	#メールアドレス正規化
	df["email"] = normalize_email_address(df["email"])

	#会社代表メールアドレス/個人別メールアドレスを判定
	df_to_insert["normalized_company_email"] = extract_company_email(df["email"])
	df_to_insert["normalized_personal_email"] = extract_personal_email(df["email"])
	
	#部署名正規化
	df_to_insert["normalized_department"] = normalize_department(df["department"])

	#役職名正規化
	df_to_insert["normalized_job_post"] = normalize_job_post(df["job_post"])

	#元のdfとキー情報を同期
	df_to_insert["seq"] = df["seq"]
	
	#正規化済み情報テーブルにINSERT
	insert_by_df(df_to_insert, tgt_dataset, tgt_table)


#マスタ登録更新処理
#マスタ上に既存であればUPDATE、なければINSERTする
def upsert_crm_lead(now):
	#まずworkから、正規化済み情報をまとめて取得(1レコードに会社レベル・人レベルの情報が混在)
	join_select_map = {
		"from": {
			"dataset": "work",
			"table": "work_crm_lead",
			"alias": "t1",
		},
		"join": {
			"dataset": "work",
			"table": "normalized_crm_lead",
			"alias": "t2",
			"type": "INNER",
			"on": [
				("t1.seq", "t2.seq"),
			],
		},
		"columns": [
			{"expr": "t1.seq"},
			{"expr": "t2.normalized_company_name"},
			{"expr": "t1.postal_code"},
			{"expr": "t1.prefecture"},
			{"expr": "t2.normalized_full_address"},
			{"expr": "t1.employees_volume"},
			{"expr": "t1.annual_revenue"},
			{"expr": "t2.normalized_company_phone_number"},
			{"expr": "t2.normalized_company_email"},
			{"expr": "t1.url"},
			{"expr": "t1.industry"},
			{"expr": "t1.contact_permission"},
			{"expr": "t2.normalized_personal_name"},
			{"expr": "t2.normalized_personal_name_kana"},
			{"expr": "t2.normalized_department"},
			{"expr": "t2.normalized_job_post"},
			{"expr": "t2.normalized_personal_phone_number"},
			{"expr": "t2.normalized_personal_email"},
		],
	}
	df = fetch_join_tables(join_select_map)

	#▼以下、会社単位のマスタ登録更新処理
	#m_conpanyのテーブルレイアウトに合わせた会社dfを作る
	df_company_new = pd.DataFrame(index=df.index)
	df_company_new["seq"]                = df["seq"].copy()
	df_company_new["company_name"]       = df["normalized_company_name"].copy()
	df_company_new["postal_code"]        = df["postal_code"].copy()
	df_company_new["prefecture_code"]    = get_pref_code(df["prefecture"])
	df_company_new["full_address"]       = df["normalized_full_address"].copy()
	df_company_new["employees_volume"]   = df["employees_volume"].copy()
	df_company_new["annual_revenue"]     = df["annual_revenue"].copy()
	df_company_new["main_phone_number"]  = df["normalized_company_phone_number"].copy().astype(str).replace("nan", "")
	df_company_new["main_email_address"] = df["normalized_company_email"].copy()
	df_company_new["url"]                = df["url"].copy()
	df_company_new["industry"]           = df["industry"].copy()
	df_company_new["contact_permission"] = df["contact_permission"].copy()
	df_company_new["customer_flag"]      = True
	df_company_new["supplier_flag"]      = False
	df_company_new["payment_flag"]       = False
	df_company_new = df_company_new.assign(
		insert_datetime=now,
		update_datetime=now
	)

	#UPDATEの対象カラム
	update_col_list = [
		"company_name",
		"postal_code",
		"prefecture_code",
		"full_address",
		"employees_volume",
		"annual_revenue",
		"main_phone_number",
		"main_email_address",
		"url",
		"industry",
		"contact_permission",
		"customer_flag",
		"update_datetime",
	]
	
	#m_companyの登録・更新
	df = upsert_m_company(df_company_new, update_col_list, df)
	
	#▼以下、人物単位のマスタ登録更新処理
	#m_personのテーブルレイアウトに合わせた個人dfを作る
	df_personal_new = pd.DataFrame(index=df.index)
	df_personal_new["seq"]             = df["seq"].copy()
	df_personal_new["name"]            = df["normalized_personal_name"].copy()
	df_personal_new["name_kana"]       = df["normalized_personal_name_kana"].copy()
	df_personal_new["company_code"]    = df["company_code"].copy()
	df_personal_new["department"]      = df["normalized_department"].copy()
	df_personal_new["job_post"]        = df["normalized_job_post"].copy()
	df_personal_new["phone_number"]    = df["normalized_personal_phone_number"].copy().astype(str).replace("nan", "")
	df_personal_new["email_address"]   = df["normalized_personal_email"].copy()
	df_personal_new = df_personal_new.assign(
		insert_datetime=now,
		update_datetime=now
	)
	
	#UPDATEの対象カラム
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
	
	#m_personの登録・更新
	upsert_m_person(df_personal_new, update_col_list)
	
	#work系テーブル全件削除
	delete_work_table("work_crm_lead")
	delete_work_table("normalized_crm_lead")


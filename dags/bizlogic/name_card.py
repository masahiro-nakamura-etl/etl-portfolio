#名刺管理ソフト関係のビジネスロジック

from common.utils import *
from common.validation import *
from common.name_collection import *
from common.bigquery_access import *
from bizlogic.common import *
from datetime import datetime
import pandas as pd
import logging
import os

CSV_FILE_NAME = "namecard.csv"

#CSVからデータレイクへINSERT
def insert_name_card_from_csv(now):
	#パスを設定
	csv_path = get_csv_filepath(CSV_FILE_NAME)

	#ファイル存在確認、なければ処理中断
	if not os.path.exists(csv_path):
		return

	#CSVを読み込み、登録日付・更新日付を付与
	df = pd.read_csv(csv_path)
	df = df.assign(
		insert_datetime=now,
		update_datetime=now
	)

	#単純INSERT
	insert_by_df(df, "lake", "name_card")


#データレイクからworkテーブルへ取込
def insert_work_name_card(now):
	#日付取得
	ds = now.date()
	#INSERT-SELECTの取得元テーブル、登録先テーブル、列名の紐づけ設定値
	insert_select_map = {
		"source": {
			"dataset" : "lake",
			"table" : "name_card",
		},
		"target": {
			"dataset" : "work",
			"table" : "work_name_card",
		},
		"columns": [
			{"src": None,                 "dst": "ROW_NUMBER() OVER (ORDER BY update_datetime) AS seq"},
			{"src": "family_name",          "dst": "family_name"},
			{"src": "first_name",           "dst": "first_name"},
			{"src": "family_name_kana",     "dst": "family_name_kana"},
			{"src": "first_name_kana",      "dst": "first_name_kana"},
			{"src": "company_name",         "dst": "company_name"},
			{"src": "department_name",      "dst": "department_name"},
			{"src": "position_name",        "dst": "position_name"},
			{"src": "postal_code",          "dst": "postal_code"},
			{"src": "address",              "dst": "address"},
			{"src": "building",             "dst": "building"},
			{"src": "phone_number",         "dst": "phone_number"},
			{"src": "company_phone_number", "dst": "company_phone_number"},
			{"src": "email",                "dst": "email"},
			{"src": "company_email",        "dst": "company_email"},
		]
	}
	insert_select_work(insert_select_map, ds)


#取り込んだデータのバリデーション
def validate_work_name_card(now):
	src_dataset = "work"
	src_table = "work_name_card"
	tgt_dataset = "error"
	tgt_table = "error_name_card"
	
	df = fetch_all_record(src_dataset, src_table)
	error_rows = []

	#メールアドレス不正チェック
	df_email_ng = validate_mail_address(df, "email")
	if not df_email_ng.empty:
		error_rows.append(df_email_ng)

	df_email_ng = validate_mail_address(df, "company_email")
	if not df_email_ng.empty:
		error_rows.append(df_email_ng)

	#電話番号不正チェック
	df_phone_ng = validate_phone_number(df, "phone_number")
	if not df_phone_ng.empty:
		error_rows.append(df_phone_ng)

	df_phone_ng = validate_phone_number(df, "company_phone_number")
	if not df_phone_ng.empty:
		error_rows.append(df_phone_ng)

	#都道府県名チェック
	df_pref_ng = validate_prefecture(df, "address")
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


#住所・氏名・電話番号等の正規化
def normalize_work_name_card():
	src_dataset = "work"
	src_table = "work_name_card"
	tgt_dataset = "work"
	tgt_table = "normalized_name_card"

	df = fetch_columns(src_dataset, src_table, ["seq", "family_name", "first_name", "family_name_kana", "first_name_kana", "company_name", "department_name", "position_name", "postal_code", "address", "building", "phone_number", "company_phone_number", "email", "company_email"])
	df_to_insert = pd.DataFrame(index=df.index)

	#電話番号正規化
	df_to_insert["normalized_personal_phone_number"] = normalize_phone_number(df["phone_number"])
	df_to_insert["normalized_company_phone_number"] = normalize_phone_number(df["company_phone_number"])

	#氏名正規化
	df_to_insert["normalized_personal_name"] = normalize_personal_name(df["family_name"].str.cat(df["first_name"], na_rep=""))
	
	#氏名カナ正規化
	df_to_insert["normalized_personal_name_kana"] = normalize_personal_name_kana(df["family_name_kana"].str.cat(df["first_name_kana"], na_rep=""))

	#会社名正規化
	df_to_insert["normalized_company_name"] = normalize_company_name(df["company_name"])

	#住所正規化
	#  ※name_cardでは、都道府県から番地・号までの住所がaddress、建物名・部屋番号がbuilding
	df_to_insert["normalized_full_address"] = normalize_address(df["address"].str.cat(df["building"], na_rep=""))

	#メールアドレス正規化
	df_to_insert["normalized_personal_email"] = normalize_email_address(df["email"])
	df_to_insert["normalized_company_email"] = normalize_email_address(df["company_email"])
	
	#部署名正規化
	df_to_insert["normalized_department"] = normalize_department(df["department_name"])

	#役職名正規化
	df_to_insert["normalized_job_post"] = normalize_job_post(df["position_name"])
	
	#郵便番号正規化
	df_to_insert["normalized_postal_code"] = normalize_postal_code(df["postal_code"])

	#元のdfとキー情報を同期
	df_to_insert["seq"] = df["seq"]
	
	#正規化済み情報テーブルにINSERT
	insert_by_df(df_to_insert, tgt_dataset, tgt_table)


#マスタ登録更新処理
#マスタ上に既存であればUPDATE、なければINSERTする
def upsert_name_card(now):
	#まずworkから、正規化済み情報をまとめて取得(1レコードに会社レベル・人レベルの情報が混在)
	join_select_map = {
		"from": {
			"dataset": "work",
			"table": "work_name_card",
			"alias": "t1",
		},
		"join": {
			"dataset": "work",
			"table": "normalized_name_card",
			"alias": "t2",
			"type": "INNER",
			"on": [
				("t1.seq", "t2.seq"),
			],
		},
		"columns": [
			{"expr": "t1.seq"},
			{"expr": "t2.normalized_company_name"},
			{"expr": "t2.normalized_postal_code"},
			{"expr": "t2.normalized_full_address"},
			{"expr": "t2.normalized_company_phone_number"},
			{"expr": "t2.normalized_company_email"},
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
	df_company_new["postal_code"]        = df["normalized_postal_code"].copy()
	df_company_new["prefecture_code"]    = get_pref_code(df["normalized_full_address"].apply(extract_prefecture))
	df_company_new["full_address"]       = df["normalized_full_address"].copy()
	df_company_new["main_phone_number"]  = df["normalized_company_phone_number"].copy().astype(str).replace("nan", "")
	df_company_new["main_email_address"] = df["normalized_company_email"].copy()
	df_company_new["contact_permission"] = False
	df_company_new["customer_flag"]      = False
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
		"main_phone_number",
		"main_email_address",
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
	delete_work_table("work_name_card")
	delete_work_table("normalized_name_card")

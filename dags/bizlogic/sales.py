#会計ソフト売上帳簿関係のビジネスロジック

from common.utils import *
from common.validation import *
from common.name_collection import *
from common.bigquery_access import *
from bizlogic.common import *
from datetime import datetime
import pandas as pd
import logging
import os

CSV_FILE_NAME = "sales_ledger.csv"

#CSVからデータレイクへINSERT
def insert_sales_from_csv(now):
	#パスを設定
	csv_path = get_csv_filepath(CSV_FILE_NAME)

	#ファイル存在確認、なければ処理中断
	if not os.path.exists(csv_path):
		return

	#CSVを読み込み、登録日付・更新日付を付与
	df = pd.read_csv(
		csv_path,
		parse_dates=["sales_date", "scheduled_delivery_date", "actual_delivery_date", "deadline_deposit_date", "actual_deposit_date"]
	)
	df = df.assign(
		insert_datetime=now,
		update_datetime=now
	)

	#単純INSERT
	insert_by_df(df, "lake", "sales_ledger")


#データレイクからworkテーブルへ取込
def insert_work_sales(now):
	#日付取得
	ds = now.date()
	#INSERT-SELECTの取得元テーブル、登録先テーブル、列名の紐づけ設定値
	insert_select_map = {
		"source": {
			"dataset" : "lake",
			"table" : "sales_ledger",
		},
		"target": {
			"dataset" : "work",
			"table" : "work_sales_ledger",
		},
		"columns": [
			{"src": None,                    "dst": "ROW_NUMBER() OVER (ORDER BY update_datetime) AS seq"},
			{"src": "customer_company_name", "dst": "customer_company_name"},
			{"src": "customer_person_name",  "dst": "customer_person_name"},
			{"src": "phone_number1",         "dst": "phone_number1"},
			{"src": "phone_number2",         "dst": "phone_number2"},
			{"src": "email_address",         "dst": "email_address"},
			{"src": "address",               "dst": "address"},
		]
	}
	insert_select_work(insert_select_map, ds)


#取り込んだデータのバリデーション
def validate_work_sales(now):
	src_dataset = "work"
	src_table = "work_sales_ledger"
	tgt_dataset = "error"
	tgt_table = "error_sales_ledger"
	
	df = fetch_all_record(src_dataset, src_table)
	error_rows = []

	#メールアドレス不正チェック
	df_email_ng = validate_mail_address(df, "email_address")
	if not df_email_ng.empty:
		error_rows.append(df_email_ng)

	#電話番号不正チェック
	df_phone_ng = validate_phone_number(df, "phone_number1")
	if not df_phone_ng.empty:
		error_rows.append(df_phone_ng)

	#電話番号不正チェック
	df_phone_ng = validate_phone_number(df, "phone_number2")
	if not df_phone_ng.empty:
		error_rows.append(df_phone_ng)

	#都道府県名チェック
	df_pref_ng = validate_prefecture(df, "address")
	if not df_pref_ng.empty:
		error_rows.append(df_pref_ng)

	#氏名以外の個人情報なしチェック
	df_name_only_ng = validate_name_only(df, "email_address", ["phone_number1", "phone_number2"])
	if not df_name_only_ng.empty:
		error_rows.append(df_name_only_ng)

	#エラーリストを1件のDFに統合
	if not error_rows:
		logging.info("No Validation Error")
		return
	df_invalid = pd.concat(error_rows, ignore_index=True)

	#エラー登録日時を付与
	df_invalid = df_invalid.assign(
		insert_datetime=now,
	)

	#エラーテーブルにINSERT
	insert_error_records(df_invalid, tgt_dataset, tgt_table)

	#Workテーブルからエラーデータを削除
	delete_by_int_key(df_invalid, src_dataset, src_table, "seq")


#住所・氏名・電話番号の正規化および会社データ・個人データの切り分け
def normalize_work_sales():
	src_dataset = "work"
	src_table = "work_sales_ledger"
	tgt_dataset = "work"
	tgt_table = "normalized_sales_ledger"

	df = fetch_columns(src_dataset, src_table, ["seq", "customer_company_name", "customer_person_name", "phone_number1", "phone_number2", "email_address", "address"])
	df_to_insert = pd.DataFrame(index=df.index)

	#会社名正規化
	df_to_insert["normalized_company_name"] = normalize_company_name(df["customer_company_name"])

	#氏名正規化
	df_to_insert["normalized_personal_name"] = normalize_personal_name(df["customer_person_name"])

	#電話番号正規化
	s_phone_number_1 = normalize_phone_number(df["phone_number1"])
	s_phone_number_2 = normalize_phone_number(df["phone_number2"])
	
	#個人電話番号/会社電話番号を判定
	s_personal_phone_number_1 = extract_personal_phone(s_phone_number_1)
	s_personal_phone_number_2 = extract_personal_phone(s_phone_number_2)
	s_company_phone_number_1 = extract_company_phone(s_phone_number_1)
	s_company_phone_number_2 = extract_company_phone(s_phone_number_2)
	
	#個人電話番号/会社電話番号をマージ
	personal_phone = s_personal_phone_number_1.replace("", np.nan).combine_first(s_personal_phone_number_2)
	company_phone = s_company_phone_number_1.replace("", np.nan).combine_first(s_company_phone_number_2)
	
	df_to_insert["normalized_personal_phone_number"] = personal_phone
	df_to_insert["normalized_company_phone_number"] = company_phone

	#メールアドレス正規化
	df["email_address"] = normalize_email_address(df["email_address"])

	#会社代表メールアドレス/個人別メールアドレスを判定
	df_to_insert["normalized_company_email"] = extract_company_email(df["email_address"])
	df_to_insert["normalized_personal_email"] = extract_personal_email(df["email_address"])

	#住所と郵便番号を分離
	#  ※インターフェースファイル上、「address」項目に「郵便番号＋住所」を持っているため
	df_address = split_postcode_address_series(df["address"])
	s_address = df_address["address"]
	s_postal_code = df_address["postal_code"]
	
	#住所正規化
	df_to_insert["normalized_full_address"] = normalize_address(s_address)
	
	#郵便番号正規化
	df_to_insert["normalized_postal_code"] = normalize_postal_code(s_postal_code)

	#元のdfとキー情報を同期
	df_to_insert["seq"] = df["seq"]

	#正規化済み情報テーブルにINSERT
	insert_by_df(df_to_insert, tgt_dataset, tgt_table)


#マスタ登録更新処理
#マスタ上に既存であればUPDATE、なければINSERTする
def upsert_sales(now):
	#まずworkから、正規化済み情報をまとめて取得(1レコードに会社レベル・人レベルの情報が混在)
	join_select_map = {
		"from": {
			"dataset": "work",
			"table": "work_sales_ledger",
			"alias": "t1",
		},
		"join": {
			"dataset": "work",
			"table": "normalized_sales_ledger",
			"alias": "t2",
			"type": "INNER",
			"on": [
				("t1.seq", "t2.seq"),
			],
		},
		"columns": [
			{"expr": "t1.seq"},
			{"expr": "t2.normalized_company_name"},
			{"expr": "t2.normalized_company_phone_number"},
			{"expr": "t2.normalized_company_email"},
			{"expr": "t2.normalized_personal_name"},
			{"expr": "t2.normalized_personal_phone_number"},
			{"expr": "t2.normalized_personal_email"},
			{"expr": "t2.normalized_postal_code"},
			{"expr": "t2.normalized_full_address"},
		],
	}
	df = fetch_join_tables(join_select_map)

	#▼以下、会社単位のマスタ登録更新処理
	#m_conpanyのテーブルレイアウトに合わせた会社dfを作る
	df_company_new = pd.DataFrame(index=df.index)
	df_company_new["seq"]                    = df["seq"].copy()
	df_company_new["company_name"]           = df["normalized_company_name"].copy()
	df_company_new["postal_code"]            = df["normalized_postal_code"].copy()
	df_company_new["prefecture_code"]        = get_pref_code(df["normalized_full_address"].apply(extract_prefecture))
	df_company_new["full_address"]           = df["normalized_full_address"].copy()
	df_company_new["main_phone_number"]      = df["normalized_company_phone_number"].copy().astype(str).replace("nan", "")
	df_company_new["main_email_address"]     = df["normalized_company_email"].copy()
	df_company_new["contact_permission"]     = False
	df_company_new["customer_flag"]          = True
	df_company_new["supplier_flag"]          = False
	df_company_new["payment_flag"]           = False
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
	df_personal_new["company_code"]    = df["company_code"].copy()
	df_personal_new["phone_number"]    = df["normalized_personal_phone_number"].copy().astype(str).replace("nan", "")
	df_personal_new["email_address"]   = df["normalized_personal_email"].copy()
	df_personal_new = df_personal_new.assign(
		insert_datetime=now,
		update_datetime=now
	)
	
	#UPDATEの対象カラム
	update_col_list = [
		"name",
		"company_code",
		"phone_number",
		"email_address",
		"update_datetime",
	]
	
	#m_personの登録・更新
	upsert_m_person(df_personal_new, update_col_list)
	
	#work系テーブル全件削除
	delete_work_table("work_sales_ledger")
	delete_work_table("normalized_sales_ledger")

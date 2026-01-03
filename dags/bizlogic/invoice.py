#請求書関係のビジネスロジック

from common.utils import *
from common.validation import *
from common.name_collection import *
from common.bigquery_access import *
from bizlogic.common import *
from datetime import datetime
import pandas as pd
import logging
import os

CSV_FILE_NAME = "invoice.csv"

#CSVからデータレイクへINSERT
def insert_invoice_from_csv(now):
	#パスを設定
	csv_path = get_csv_filepath(CSV_FILE_NAME)

	#ファイル存在確認、なければ処理中断
	if not os.path.exists(csv_path):
		return

	#CSVを読み込み、登録日付・更新日付を付与
	df = pd.read_csv(
		csv_path,
		parse_dates=["invoice_date", "payment_deadline"]
	)
	df = df.assign(
		insert_datetime=now,
		update_datetime=now
	)

	#単純INSERT
	insert_by_df(df, "lake", "invoice")


#データレイクからworkテーブルへ取込
def insert_work_invoice(now):
	#日付取得
	ds = now.date()
	#INSERT-SELECTの取得元テーブル、登録先テーブル、列名の紐づけ設定値
	insert_select_map = {
		"source": {
			"dataset" : "lake",
			"table" : "invoice",
		},
		"target": {
			"dataset" : "work",
			"table" : "work_invoice",
		},
		"columns": [
			{"src": None,                     "dst": "ROW_NUMBER() OVER (ORDER BY update_datetime) AS seq"},
			{"src": "invoice_issuer",         "dst": "invoice_issuer"},
			{"src": "tel_number",             "dst": "tel_number"},
			{"src": "fax_number",             "dst": "fax_number"},
			{"src": "email",                  "dst": "email"},
			{"src": "address",                "dst": "address"},
			{"src": "invoice_company_number", "dst": "invoice_company_number"},
		]
	}
	insert_select_work(insert_select_map, ds)

#取り込んだデータのバリデーション
def validate_work_invoice(now):
	src_dataset = "work"
	src_table = "work_invoice"
	tgt_dataset = "error"
	tgt_table = "error_invoice"
	
	df = fetch_all_record(src_dataset, src_table)
	error_rows = []

	#メールアドレス不正チェック
	df_email_ng = validate_mail_address(df, "email")
	if not df_email_ng.empty:
		error_rows.append(df_email_ng)

	#電話番号不正チェック
	df_phone_ng = validate_phone_number(df, "tel_number")
	if not df_phone_ng.empty:
		error_rows.append(df_phone_ng)

	#FAX番号不正チェック
	df_phone_ng = validate_fax_number(df, "fax_number")
	if not df_phone_ng.empty:
		error_rows.append(df_phone_ng)

	#都道府県名チェック
	df_pref_ng = validate_prefecture(df, "address")
	if not df_pref_ng.empty:
		error_rows.append(df_pref_ng)

	#適格請求書発行事業者番号チェック
	df_invoice_ng = validate_invoice_company_number(df, "invoice_company_number")
	if not df_invoice_ng.empty:
		error_rows.append(df_invoice_ng)

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


#住所・氏名・電話番号の正規化
# ※請求書には一般的に担当者個人の情報は入ってこないので、m_person登録・更新用の情報正規化は行わない
def normalize_work_invoice():
	src_dataset = "work"
	src_table = "work_invoice"
	tgt_dataset = "work"
	tgt_table = "normalized_invoice"

	df = fetch_columns(src_dataset, src_table, ["seq", "invoice_issuer", "tel_number", "fax_number", "email", "address"])
	df_to_insert = pd.DataFrame(index=df.index)

	#会社名正規化
	df_to_insert["normalized_company_name"] = normalize_company_name(df["invoice_issuer"])

	#電話番号正規化
	df_to_insert["normalized_company_phone_number"] = normalize_phone_number(df["tel_number"])
	df_to_insert["normalized_company_fax_number"] = normalize_phone_number(df["fax_number"])

	#メールアドレス正規化
	df_to_insert["normalized_company_email"] = normalize_email_address(df["email"])

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
# ※請求書には一般的に担当者個人の情報は入ってこないので、m_personへの登録・更新は行わない
def upsert_invoice(now):
	#まずworkから、正規化済み情報をまとめて取得(1レコードに会社レベル・人レベルの情報が混在)
	join_select_map = {
		"from": {
			"dataset": "work",
			"table": "work_invoice",
			"alias": "t1",
		},
		"join": {
			"dataset": "work",
			"table": "normalized_invoice",
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
			{"expr": "t2.normalized_company_fax_number"},
			{"expr": "t2.normalized_company_email"},
			{"expr": "t2.normalized_postal_code"},
			{"expr": "t2.normalized_full_address"},
			{"expr": "t1.invoice_company_number"},
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
	df_company_new["fax_number"]             = df["normalized_company_fax_number"].copy().astype(str).replace("nan", "")
	df_company_new["main_email_address"]     = df["normalized_company_email"].copy()
	df_company_new["invoice_company_number"] = df["invoice_company_number"].copy()
	df_company_new["contact_permission"]     = False
	df_company_new["customer_flag"]          = False
	df_company_new["supplier_flag"]          = False
	df_company_new["payment_flag"]           = True
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
		"fax_number",
		"main_email_address",
		"invoice_company_number",
		"payment_flag",
		"update_datetime",
	]
	
	#m_companyの登録・更新
	upsert_m_company(df_company_new, update_col_list, df)
	
	#work系テーブル全件削除
	delete_work_table("work_invoice")
	delete_work_table("normalized_invoice")

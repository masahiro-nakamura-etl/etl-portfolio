#取引先マスタ集約

from common.utils import *
from datetime import datetime
from bizlogic.common import *
import pandas as pd
import logging
import os

CSV_FILE_NAME = "m_company_aggregation.csv"

#取引先マスタ集約CSVファイルの存在確認、内容検証
def check_aggregation_csv():
	#パスを設定
	csv_path = get_csv_filepath(CSV_FILE_NAME)

	#ファイル存在確認、なければ処理中断
	if not os.path.exists(csv_path):
		logging.error("%s File Not Found", csv_path)
		return False

	#CSVを読み込み
	df = pd.read_csv(
		csv_path,
		header=None,
		dtype=str
	)

	#列数が2でなければ処理中断
	if not df.shape[1] == 2:
		logging.error("%s Not Only 2 Column Exist", csv_path)
		return False

	#▼以下、内容の妥当性チェック
	#既存会社マスタ取得
	df_m_company = fetch_all_record("dwh","m_company")
	company_codes = set(
		df_m_company["company_code"]
		.astype(str)
		.str.strip()
		.dropna()
	)

	#CSV側の1列目（集約先）、2列目（集約元）をそれぞれ抽出
	col0 = df[0].astype(str).str.strip()
	col1 = df[1].astype(str).str.strip()

	#1列目のうち、既存m_companyに存在しない、不正な取引先コードを抽出（重複は排除）
	invalid_col0_list = (col0.loc[~col0.isin(company_codes)].drop_duplicates().tolist())

	#2列目も同上）
	invalid_col1_list = (col1.loc[~col1.isin(company_codes)].drop_duplicates().tolist())

	#CSV上の不正な取引先コードをエラーログ出力
	if invalid_col0_list:
		for code in invalid_col0_list:
			logging.error("%s Column1 invalid company_code: %s", csv_path, code)

	if invalid_col1_list:
		for code in invalid_col1_list:
			logging.error("%s Column2 invalid company_code: %s", csv_path,code)

	#不正な取引先コードが1件でもあれば後続タスク打ち切り
	if invalid_col0_list or invalid_col1_list:
		return False

	return True


#取引先マスタ集約
#集約先と集約元のレコードをマージする形で集約先を更新した後、集約元のレコードを削除する
def merge_m_company(now):
	#パスを設定
	csv_path = get_csv_filepath(CSV_FILE_NAME)

	#CSVを読み込み
	df_csv = pd.read_csv(
		csv_path,
		header=None,
		dtype=str
	)

	#既存会社マスタ取得
	df_m_company = fetch_all_record("dwh","m_company")
	
	#UPDATE用のDFを作成
	df_left = pd.DataFrame(index=df_csv.index)
	df_left["company_code"] = df_csv[0]
	df_left = df_left.merge(df_m_company, on="company_code", how="inner")

	df_right = pd.DataFrame(index=df_csv.index)
	df_right["company_code"] = df_csv[1]
	df_right = df_right.merge(df_m_company, on="company_code", how="inner")

	df_update = pd.DataFrame(index=df_csv.index)
	df_update["company_code"]           = df_left["company_code"]
	df_update["company_name"]           = (df_left["company_name"].replace("", pd.NA).fillna(df_right["company_name"]))
	df_update["postal_code"]            = (df_left["postal_code"].replace("", pd.NA).fillna(df_right["postal_code"]))
	df_update["prefecture_code"]        = (df_left["prefecture_code"].replace("", pd.NA).fillna(df_right["prefecture_code"]))
	df_update["full_address"]           = (df_left["full_address"].replace("", pd.NA).fillna(df_right["full_address"]))
	df_update["employees_volume"]       = (df_left["employees_volume"].replace(0, pd.NA).fillna(df_right["employees_volume"]))
	df_update["annual_revenue"]         = (df_left["annual_revenue"].replace(0, pd.NA).fillna(df_right["annual_revenue"]))
	df_update["main_phone_number"]      = (df_left["main_phone_number"].replace("", pd.NA).fillna(df_right["main_phone_number"]))
	df_update["main_email_address"]     = (df_left["main_email_address"].replace("", pd.NA).fillna(df_right["main_email_address"]))
	df_update["url"]                    = (df_left["url"].replace("", pd.NA).fillna(df_right["url"]))
	df_update["industry"]               = (df_left["industry"].replace("", pd.NA).fillna(df_right["industry"]))
	df_update["contact_permission"]     = (df_left["contact_permission"].replace("", pd.NA).fillna(df_right["contact_permission"]))
	df_update["president_person_id"]    = (df_left["president_person_id"].replace("", pd.NA).fillna(df_right["president_person_id"]))
	df_update["customer_flag"]          = (df_left["customer_flag"].replace("", pd.NA).fillna(df_right["customer_flag"]))
	df_update["supplier_flag"]          = (df_left["supplier_flag"].replace("", pd.NA).fillna(df_right["supplier_flag"]))
	df_update["payment_flag"]           = (df_left["payment_flag"].replace("", pd.NA).fillna(df_right["payment_flag"]))
	df_update["invoice_company_number"] = (df_left["invoice_company_number"].replace("", pd.NA).fillna(df_right["invoice_company_number"]))
	df_update["update_datetime"]        = now

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
		"president_person_id",
		"customer_flag",
		"supplier_flag",
		"payment_flag",
		"invoice_company_number",
	]

	#生き残らせる方のレコードをUPDATE
	update_by_df(df_update, "work", "tmp_m_company_update", "dwh", "m_company", update_col_list, "company_code")

	#不要レコードを削除
	delete_by_str_key(df_right, "dwh", "m_company", "company_code")

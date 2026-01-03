#バリデーション用関数
#共通仕様として、「引数として受け取ったDataFrameから、不正な行を抽出し、エラー事由を付与して返却する」。

from common.utils import *
from common.name_collection import *
from common.bigquery_access import *
import pandas as pd

#メールアドレス不正チェック
#@のない文字列はNG
def validate_mail_address(df: pd.DataFrame, column: str) -> pd.DataFrame:
	df_email_ng = df[df[column].notnull() & (~df[column].str.contains("@", na=False))].copy()
	if not df_email_ng.empty:
		df_email_ng["error_reason"] = "INVALID_MAIL_ADDRESS_FORMAT"
	return df_email_ng


#電話番号不正チェック
#数字部分だけ抜き出して10桁未満ないし11桁超過の場合NG
# ※データ登録前の正規化処理で数字以外除去するので、「奥様：09012345678」などの入力値でも後続処理には問題なし
def validate_phone_number(df: pd.DataFrame, column: str) -> pd.DataFrame:
	phone_numbers = digits_only(df[column])

	valid_mask = phone_numbers.notnull() & (phone_numbers != "")
	df_phone_ng = df[
		valid_mask & ((phone_numbers.str.len() < 10) | (phone_numbers.str.len() > 11))
	].copy()

	if not df_phone_ng.empty:
		df_phone_ng["error_reason"] = "INVALID_PHONE_NUMBER"
	return df_phone_ng


#FAX番号不正チェック
#電話番号不正チェックとロジック共用 エラー文言のみ変える
def validate_fax_number(df: pd.DataFrame, column: str) -> pd.DataFrame:
	df_fax_ng = validate_phone_number(df, column)

	if not df_fax_ng.empty:
		df_fax_ng["error_reason"] = "INVALID_FAX_NUMBER"
	return df_fax_ng


#都道府県名チェック
#存在しない都道府県名はNG
# ※海外企業は現在のところ非対応
def validate_prefecture(df: pd.DataFrame, column: str) -> pd.DataFrame:
	#都道府県マスタより都道府県名を取得
	df_prefecture = fetch_columns("dwh", "m_prefecture", ["prefecture_name"])
	valid_prefectures = set(df_prefecture["prefecture_name"].tolist())

	#突合用Seriesを作成
	prefecture_extracted = df[column].apply(extract_prefecture)

	#突合
	df_pref_ng = df[~prefecture_extracted.isin(valid_prefectures)].copy()
	if not df_pref_ng.empty:
		df_pref_ng["error_reason"] = "INVALID_PREFECTURE"

	return df_pref_ng


#人物レベルの情報を「氏名」しか持っていない行は（仕様上）NGとする
# ⇒メールアドレスが会社代表メールで、かつ電話番号が携帯電話でない場合はNG
#（この場合、人物レベルの情報が氏名だけになる。2項目一致で個人を判定する都合上、氏名だけでは同一人物判定できず、永遠に更新できない孤立データとなってしまうため）
def validate_name_only(df: pd.DataFrame, email_column: str, phone_columns: list) -> pd.DataFrame:

	#会社代表メールアドレス/個人別メールアドレスを判定
	is_company_email = extract_company_email(df[email_column]).astype(bool)
	is_personal_email = ~is_company_email

	#各列ごとに個人電話番号かを判定
	is_personal_phone_list = [
		extract_personal_phone(df[col]).astype(bool) for col in phone_columns
	]

	#「電話番号」項目が複数列あるケースを想定し、OR処理で「個人電話番号の情報がある行」を抽出
	if is_personal_phone_list:
		is_personal_phone = is_personal_phone_list[0]
		for col_mask in is_personal_phone_list[1:]:
			is_personal_phone = is_personal_phone | col_mask
	else:
		is_personal_phone = pd.Series([False]*len(df), index=df.index)

	# NOT「個人メールアドレスの情報がある行」 AND NOT「個人電話番号の情報がある行」で、個人情報なしの行を抽出し返却
	ng_mask = (~is_personal_email) & (~is_personal_phone)
	df_ng = df[ng_mask].copy()
	if not df_ng.empty:
		df_ng["error_reason"] = "PERSONAL_INFORMATION_NAME_ONLY"

	return df_ng


#適格請求書発行事業者番号チェック
#桁数オーバーしたものは上流システムでそもそも通らない前提で、「"T"+数字13桁」であることのみチェック
def validate_invoice_company_number(df: pd.DataFrame, column: str) -> pd.DataFrame:
	df_invoice_ng = df[
		df[column].notnull() &
		(~df[column].str.match(r'^T[0-9]{13}$', na=False))
	].copy()

	if not df_invoice_ng.empty:
		df_invoice_ng["error_reason"] = "INVALID_INVOICE_COMPANY_NUMBER_FORMAT"

	return df_invoice_ng

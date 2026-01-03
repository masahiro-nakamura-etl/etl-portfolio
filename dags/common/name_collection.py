#名寄せロジック

from common.bigquery_access import *
from common.utils import *
import pandas as pd
import numpy as np
import re

KEYWORD_CATEGORY_PERSONAL_PHONE_NUMBER = '1'
KEYWORD_CATEGORY_COMPANY_EMAIL = '2'
KEYWORD_CATEGORY_PRESIDENT = '3'

REPLACE_STRING_CATEGORY_TO_HYPHEN = '1'
REPLACE_STRING_CATEGORY_COMPANY_NAME = '2'
REPLACE_STRING_CATEGORY_NO_JOB_POST = '3'

#電話番号正規化 数字のみ抽出
# ※「2階にお住まいの3男のお子様：09023456789」などのケースは、数字以外除去すると異常値になるが、事前に文字数チェックする前提なので通常問題なし
def normalize_phone_number(phone_numbers: pd.Series) -> pd.Series:
	return digits_only(phone_numbers)


#電話番号のうち、個人電話番号と判定できるものを抽出
#※「個人電話番号とみなす条件一覧」はDBで保持している
def extract_personal_phone(phone_numbers: pd.Series) -> pd.Series:
	is_personal_phone = create_personal_phone_mask(phone_numbers)
	personal_phone_numbers = phone_numbers.where(is_personal_phone, "")
	return personal_phone_numbers


#電話番号のうち、会社電話番号と判定できるものを抽出
def extract_company_phone(phone_numbers: pd.Series) -> pd.Series:
	is_personal_phone = create_personal_phone_mask(phone_numbers)
	company_phone_numbers = phone_numbers.where(~is_personal_phone, "")
	return company_phone_numbers


#電話番号を会社用/個人用と判定するためのマスク作成
def create_personal_phone_mask(phone_numbers: pd.Series) -> pd.Series:
	#キーワードマスタから「個人電話番号とみなす番号(前方マッチング用)」を取得
	keywords = get_keywords(KEYWORD_CATEGORY_PERSONAL_PHONE_NUMBER)

	#キーワードを数字のみに変換（念のため）
	keywords = digits_only(keywords)
	pattern = series_to_prefix_regex(keywords)

	#個人電話番号かどうか判定
	is_personal_phone = (phone_numbers.astype(str).str.match(pattern, na=False))
	return is_personal_phone


#氏名正規化
def normalize_personal_name(names: pd.Series) -> pd.Series:
	#スペース除去
	#入力データの時点で姓と名が1項目にまとまっている場合、姓と名の区切り箇所を機械的に判定するのが困難。そのため"スペース無し"を仕様とする。
	normalized_names = remove_spaces(names)
	#半角カナは全角に変換
	normalized_names = convert_kana_half_to_full(normalized_names)
	return normalized_names


#氏名カナ正規化
def normalize_personal_name_kana(names_kana: pd.Series) -> pd.Series:
	#スペース除去
	normalized_names_kana = remove_spaces(names_kana)
	#半角カナは全角に変換
	normalized_names_kana = convert_kana_half_to_full(normalized_names_kana)
	#ひらがなはカタカナに変換
	normalized_names_kana = convert_kana_hira_to_kata(normalized_names_kana)
	return normalized_names_kana


#会社名正規化
def normalize_company_name(names: pd.Series) -> pd.Series:
	#スペース除去
	normalized_names = remove_spaces(names)
	#全角英数字・記号⇒半角英数字・記号に変換
	normalized_names = convert_ascii_full_to_half(normalized_names)
	#半角カナ⇒全角カナに変換
	normalized_names = convert_kana_half_to_full(normalized_names)
	#ハイフンもどきをハイフンに統一
	df_trans = get_replace_strings(REPLACE_STRING_CATEGORY_TO_HYPHEN)
	normalized_names = replace_string(normalized_names, df_trans)
	#「株式会社」「（株）」「(株)」「㈱」などを表記統一
	df_trans = get_replace_strings(REPLACE_STRING_CATEGORY_COMPANY_NAME)
	normalized_names = replace_string(normalized_names, df_trans)
	return normalized_names


#郵便番号正規化 数字のみ抽出
# ※ポートフォリオとしてダミーデータを扱う都合上チェックをかけていないが、データ精度を考慮するなら全国郵便番号マスタと照合した方が本当は良い
def normalize_postal_code(postal_code: pd.Series) -> pd.Series:
	return digits_only(postal_code)


#住所正規化
def normalize_address(address: pd.Series) -> pd.Series:
	#NaNに対するエラー回避のため空文字に置換
	normalized_address = address.fillna('')
	#スペース除去
	normalized_address = remove_spaces(normalized_address)
	#全角英数字を半角に変換
	normalized_address = convert_ascii_full_to_half(normalized_address)
	#半角カナを全角カナに変換
	normalized_address = convert_kana_half_to_full(normalized_address)
	#ハイフンもどきをハイフンに統一
	df_trans = get_replace_strings(REPLACE_STRING_CATEGORY_TO_HYPHEN)
	normalized_address = replace_string(normalized_address, df_trans)
	#「丁目」「番地」「号室」「号」を半角ハイフンに変換
	normalized_address = normalized_address.str.replace(r'(\d+)(丁目|番地|号室|号)', r'\1-', regex=True)
	#ハイフンの直後が数字でない場合、そのハイフンは削除（「号」と「アパート名」の間などに不自然なハイフンがつくのを回避）
	normalized_address = normalized_address.str.replace(r'-(?=\D|$)', '', regex=True)
	#末尾のハイフン除去（戸建て住宅を想定）
	normalized_address = normalized_address.str.rstrip('-')
	return normalized_address


#メールアドレス正規化
def normalize_email_address(emails: pd.Series) -> pd.Series:
	#NaNに対するエラー回避のため空文字に置換
	normalized_emails = emails.fillna('')
	#スペース除去
	normalized_emails = remove_spaces(normalized_emails)
	#大文字を小文字に変換
	normalized_emails = normalized_emails.str.lower()
	return normalized_emails


#メールアドレスのうち、会社代表アドレスと判定できるものを抽出
#※「会社代表メールアドレスとみなす条件一覧」はDBで保持している
def extract_company_email(emails: pd.Series) -> pd.Series:
	is_company_email = create_company_email_mask(emails)
	company_emails = emails.where(is_company_email, "")
	return company_emails


#メールアドレスのうち、各個人別アドレスと判定できるものを抽出
def extract_personal_email(emails: pd.Series) -> pd.Series:
	is_company_email = create_company_email_mask(emails)
	personal_emails = emails.where(~is_company_email, "")
	return personal_emails


#メールアドレスを会社代表/各個人別と判定するためのマスク作成
def create_company_email_mask(emails: pd.Series) -> pd.Series:
	#キーワードマスタから「個人電話番号とみなす番号(前方マッチング用)」を取得
	keywords = get_keywords(KEYWORD_CATEGORY_COMPANY_EMAIL)

	#突合先メールアドレスに合わせてキーワードも正規化（念のため）
	keywords = normalize_email_address(keywords)
	pattern = series_to_prefix_regex(keywords)

	#会社メールアドレスかどうか判定
	is_company_email = (emails.astype(str).str.match(pattern, na=False))
	return is_company_email


#部署名正規化
#部単位、課単位、支店単位、グループ表記など混在して統一的に処理できないため、表記ブレの統一のみ
def normalize_department(department: pd.Series) -> pd.Series:
	#スペース除去
	normalized_department = remove_spaces(department)
	#全角英数字・記号⇒半角英数字・記号に変換
	normalized_department = convert_ascii_full_to_half(normalized_department)
	#半角カナ⇒全角カナに変換
	normalized_department = convert_kana_half_to_full(normalized_department)
	#ハイフンもどきをハイフンに統一
	df_trans = get_replace_strings(REPLACE_STRING_CATEGORY_TO_HYPHEN)
	normalized_department = replace_string(normalized_department, df_trans)

	return normalized_department


#役職名正規化
def normalize_job_post(job_post: pd.Series) -> pd.Series:
	#スペース除去
	normalized_job_post = remove_spaces(job_post)
	#全角英数字・記号⇒半角英数字・記号に変換
	normalized_job_post = convert_ascii_full_to_half(normalized_job_post)
	#半角カナ⇒全角カナに変換
	normalized_job_post = convert_kana_half_to_full(normalized_job_post)
	#ハイフンもどきをハイフンに統一
	df_trans = get_replace_strings(REPLACE_STRING_CATEGORY_TO_HYPHEN)
	normalized_job_post = replace_string(normalized_job_post, df_trans)
	#null,「-」「役職なし」「平社員」「ヒラ」などを「役職なし」に統一
	normalized_job_post = (normalized_job_post.replace("", pd.NA).fillna("-")) #下準備としてNullと空文字はハイフンに変換
	df_trans = get_replace_strings(REPLACE_STRING_CATEGORY_NO_JOB_POST)
	normalized_job_post = replace_string_exact(normalized_job_post, df_trans)
	
	return normalized_job_post


#最初の非null値を優先して生かす
#  ※仮実装として「無条件に非Nullの先頭行を優先する」としたが、ここは業務用件次第でアレンジすべき箇所
def first_non_null(series) -> any:
	first_val = series.iloc[0]
	if pd.notna(first_val):
		return first_val
	else:
		non_nulls = series[series.notna()]
		return non_nulls.iloc[0] if not non_nulls.empty else np.nan


#郵便番号と住所が混在したSeriesを、2列のDataFrameに変換
def split_postcode_address_series(s: pd.Series) -> pd.DataFrame:
	s = s.fillna("")

	#スペース除去
	s = remove_spaces(s)
	#ハイフンもどきをハイフンに統一
	df_trans = get_replace_strings(REPLACE_STRING_CATEGORY_TO_HYPHEN)
	s = replace_string(s, df_trans)
	#「〒」除去
	s = s.str.lstrip("〒")

	#先頭の郵便番号を抽出（1234567/123-4567）
	postal_code = (
		s.str.extract(r'^(\d{3}-?\d{4})')[0]
		 .str.replace(r'\D', '', regex=True)
		 .fillna("")
	)

	#先頭の郵便番号部分のみ削除して住所を作成
	address = s.str.replace(r'^\d{3}-?\d{4}', '', regex=True)

	return pd.DataFrame({
		"postal_code": postal_code,
		"address": address
	})


#社長属性の役職名に該当する行を抽出
def extract_president(df: pd.DataFrame) -> pd.DataFrame:
	#キーワードマスタから、社長属性の役職名一覧を取得
	keywords = get_keywords(KEYWORD_CATEGORY_PRESIDENT)

	#キーワードが空なら空DataFrameを返す
	#キーワードが0件は運用上考えにくいが、もし発生すると、正規表現で部分一致する都合上、すべての人物が社長と判定されてしまうので
	if keywords.empty:
		return df.iloc[0:0]

	#社長属性の役職名かどうか判定する正規表現パターンを作成
	pattern = "|".join(re.escape(k) for k in keywords.dropna())

	#社長属性に該当する行のTrue/False判定を作成（部分一致判定）
	is_president = (df["job_post"].astype(str).str.contains(pattern, na=False))
	
	#社長属性に該当する列を抽出
	df_president =  df.loc[is_president]
	return df_president


#特定の2カラムが完全一致した行をマージして重複排除
#本ETLにおける名寄せ判定の基本的な考え方は「"氏名（会社名）"、"住所"、"電話番号"、"メールアドレス"のうち2つが完全一致したら同一人物（同一会社）とみなす」なので、その判定・抽出用
def merge_rows(df: pd.DataFrame, combinations) -> pd.DataFrame:
	df_result = df.copy()

	for cols in combinations:
		#両カラムがNULL・ 空文字でない行だけを重複判定対象にする
		valid_mask = pd.Series(True, index=df_result.index)
		for c in cols:
			valid_mask &= df_result[c].notna()
			valid_mask &= df_result[c] != ''

		#有効行だけで duplicated を取る
		dup_mask = df_result[valid_mask].duplicated(subset=cols, keep=False)
		if not dup_mask.any():
			continue

		#index を元に戻してマスクを再構築
		dup_index = df_result[valid_mask].index[dup_mask]

		df_dups = df_result.loc[dup_index]
		df_non_dups = df_result.drop(index=dup_index)

		#マージ実行
		df_merged_dups = (
			df_dups
			.groupby(list(cols), as_index=False)
			.agg(first_non_null)
		)

		df_result = pd.concat(
			[df_merged_dups, df_non_dups],
			ignore_index=True
		)

	return df_result


#既存マスタと新規データを比較し、2項目完全一致したものを更新用dfとして返す
#これも名寄せ判定の考え方はmerge_rowsと同様
def create_update_df(df_master: pd.DataFrame, df_new: pd.DataFrame, combinations: list, pk: str) -> pd.DataFrame:
	if df_new.empty:
		return pd.DataFrame(columns=df_master.columns)

	df_new_indexed = df_new.copy()
	df_new_indexed['_original_idx'] = df_new_indexed.index
	update_info = {}

	#各組み合わせで一致する行を抽出
	for key_columns in combinations:
		#NULL、空文字は除外
		mask_master = df_master[key_columns].notnull().all(axis=1) & (df_master[key_columns] != '').all(axis=1)
		mask_new = df_new_indexed[key_columns].notnull().all(axis=1) & (df_new_indexed[key_columns] != '').all(axis=1)

		df_master_valid = df_master.loc[mask_master]
		df_new_valid = df_new_indexed.loc[mask_new]

		if df_new_valid.empty or df_master_valid.empty:
			continue

		#マージして一致行を抽出
		df_merged = df_master_valid.merge(
			df_new_valid,
			on=key_columns,
			how='right',
			suffixes=('', '_y'),
			indicator=True
		)

		matched = df_merged[df_merged['_merge'] == 'both']

		#original_idxとPK情報を記録
		for _, row in matched.iterrows():
			update_info[row['_original_idx']] = row[pk]

	if not update_info:
		return pd.DataFrame(columns=df_master.columns)

	#df_new_indexedから、UPDATE対象の行を抽出
	df_update = df_new_indexed.loc[df_new_indexed.index.isin(update_info.keys())].copy()

	#PK情報を付与
	df_update[pk] = df_update['_original_idx'].map(update_info)

	#_original_idx列は不要なので削除
	df_update.drop(columns=['_original_idx'], inplace=True)

	return df_update

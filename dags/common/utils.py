#汎用的な文字列変換、データ型変換など

from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from datetime import datetime
import pandas as pd
import pendulum
import re

#全半角変換テーブル
ASCII_ZENKAKU = ''.join(chr(i) for i in range(0xFF01, 0xFF5F))
ASCII_HANKAKU = ''.join(chr(i - 0xFEE0) for i in range(0xFF01, 0xFF5F))
ASCII_TRANSLATION_TABLE = str.maketrans(ASCII_ZENKAKU, ASCII_HANKAKU)
KANA_MAP_FULL_TO_HALF = {
	"。": "｡", "、": "､", "・": "･", "ー": "ｰ",
	"ァ": "ｧ", "ア": "ｱ", "ィ": "ｨ", "イ": "ｲ", "ゥ": "ｩ", "ヴ": "ｳﾞ", "ウ": "ｳ",
	"ェ": "ｪ", "エ": "ｴ", "ォ": "ｫ", "オ": "ｵ",
	"ガ": "ｶﾞ", "カ": "ｶ",
	"ギ": "ｷﾞ", "キ": "ｷ",
	"グ": "ｸﾞ", "ク": "ｸ",
	"ゲ": "ｹﾞ", "ケ": "ｹ",
	"ゴ": "ｺﾞ", "コ": "ｺ",
	"ザ": "ｻﾞ", "サ": "ｻ",
	"ジ": "ｼﾞ", "シ": "ｼ",
	"ズ": "ｽﾞ", "ス": "ｽ",
	"ゼ": "ｾﾞ", "セ": "ｾ",
	"ゾ": "ｿﾞ", "ソ": "ｿ",
	"ダ": "ﾀﾞ", "タ": "ﾀ",
	"ヂ": "ﾁﾞ", "チ": "ﾁ",
	"ッ": "ｯ",
	"ヅ": "ﾂﾞ", "ツ": "ﾂ",
	"デ": "ﾃﾞ", "テ": "ﾃ",
	"ド": "ﾄﾞ", "ト": "ﾄ",
	"ナ": "ﾅ",
	"ニ": "ﾆ",
	"ヌ": "ﾇ",
	"ネ": "ﾈ",
	"ノ": "ﾉ",
	"バ": "ﾊﾞ", "パ": "ﾊﾟ", "ハ": "ﾊ",
	"ビ": "ﾋﾞ", "ピ": "ﾋﾟ", "ヒ": "ﾋ",
	"ブ": "ﾌﾞ", "プ": "ﾌﾟ", "フ": "ﾌ",
	"ベ": "ﾍﾞ", "ペ": "ﾍﾟ", "ヘ": "ﾍ",
	"ボ": "ﾎﾞ", "ポ": "ﾎﾟ", "ホ": "ﾎ",
	"マ": "ﾏ",
	"ミ": "ﾐ",
	"ム": "ﾑ",
	"メ": "ﾒ",
	"モ": "ﾓ",
	"ャ": "ｬ", "ヤ": "ﾔ",
	"ュ": "ｭ", "ユ": "ﾕ",
	"ョ": "ｮ", "ヨ": "ﾖ",
	"ラ": "ﾗ",
	"リ": "ﾘ",
	"ル": "ﾙ",
	"レ": "ﾚ",
	"ロ": "ﾛ",
	"ワ": "ﾜ",
	"ヲ": "ｦ",
	"ン": "ﾝ",
	"ヵ": "ｶ", "ヶ": "ｹ",
}
KANA_MAP_HALF_TO_FULL = {
	"｡": "。", "､": "、", "･": "・", "ｰ": "ー",
	"ｧ": "ァ", "ｱ": "ア", "ｨ": "ィ", "ｲ": "イ", "ｩ": "ゥ", "ｳﾞ": "ヴ", "ｳ": "ウ",
	"ｪ": "ェ", "ｴ": "エ", "ｫ": "ォ", "ｵ": "オ",
	"ｶﾞ": "ガ", "ｶ": "カ",
	"ｷﾞ": "ギ", "ｷ": "キ",
	"ｸﾞ": "グ", "ｸ": "ク",
	"ｹﾞ": "ゲ", "ｹ": "ケ",
	"ｺﾞ": "ゴ", "ｺ": "コ",
	"ｻﾞ": "ザ", "ｻ": "サ",
	"ｼﾞ": "ジ", "ｼ": "シ",
	"ｽﾞ": "ズ", "ｽ": "ス",
	"ｾﾞ": "ゼ", "ｾ": "セ",
	"ｿﾞ": "ゾ", "ｿ": "ソ",
	"ﾀﾞ": "ダ", "ﾀ": "タ",
	"ﾁﾞ": "ヂ", "ﾁ": "チ",
	"ｯﾞ": "ッ", "ﾂ": "ツ",
	"ﾂﾞ": "ヅ", "ﾂ": "ツ",
	"ﾃﾞ": "デ", "ﾃ": "テ",
	"ﾄﾞ": "ド", "ﾄ": "ト",
	"ﾅ": "ナ", "ﾆ": "ニ", "ﾇ": "ヌ", "ﾈ": "ネ", "ﾉ": "ノ",
	"ﾊﾞ": "バ", "ﾊﾟ": "パ", "ﾊ": "ハ",
	"ﾋﾞ": "ビ", "ﾋﾟ": "ピ", "ﾋ": "ヒ",
	"ﾌﾞ": "ブ", "ﾌﾟ": "プ", "ﾌ": "フ",
	"ﾍﾞ": "ベ", "ﾍﾟ": "ペ", "ﾍ": "ヘ",
	"ﾎﾞ": "ボ", "ﾎﾟ": "ポ", "ﾎ": "ホ",
	"ﾏ": "マ", "ﾐ": "ミ", "ﾑ": "ム", "ﾒ": "メ", "ﾓ": "モ",
	"ｬ": "ャ", "ﾔ": "ヤ", "ｭ": "ュ", "ﾕ": "ユ", "ｮ": "ョ", "ﾖ": "ヨ",
	"ﾗ": "ラ", "ﾘ": "リ", "ﾙ": "ル", "ﾚ": "レ", "ﾛ": "ロ",
	"ﾜ": "ワ", "ｦ": "ヲ", "ﾝ": "ン"
}
KANA_TRANSLATION_TABLE = str.maketrans(KANA_MAP_FULL_TO_HALF)

#半角・全角スペース除去
def remove_spaces(s: pd.Series) -> pd.Series:
	s = s.fillna('')
	return s.str.replace(r"[ 　]", "", regex=True)


#数字以外除去
def digits_only(s: pd.Series) -> pd.Series:
	s = s.fillna('')
	return s.str.replace(r'\D', '', regex=True)


#文字列変換(部分一致)
def replace_string(s_before: pd.Series, df_trans: pd.DataFrame) -> pd.Series:
	s_after = s_before.fillna('').copy()
	for _, row in df_trans.iterrows():
		s_after = s_after.str.replace(row["before_string"], row["after_string"], regex=False)
	return s_after


#文字列変換(完全一致)
def replace_string_exact(s_before: pd.Series, df_trans: pd.DataFrame) -> pd.Series:
	s_after = s_before.fillna('').copy()
	for _, row in df_trans.iterrows():
		s_after = s_after.mask(s_after == row["before_string"], row["after_string"])
	return s_after


#全角英数字⇒半角英数字変換
def convert_ascii_full_to_half(s: pd.Series) -> pd.Series:
	return s.str.translate(ASCII_TRANSLATION_TABLE)


#全角⇒半角カナ変換
def convert_kana_full_to_half(s: pd.Series) -> pd.Series:
	return s.str.translate(KANA_TRANSLATION_TABLE)


#半角⇒全角カナ変換 キーが2文字になるのでdetail関数で個別処理
def convert_kana_half_to_full(s: pd.Series) -> pd.Series:
	return s.fillna('').apply(convert_kana_half_to_full_detail)

def convert_kana_half_to_full_detail(s: str) -> str:
	#長い文字列を先に置換
	for half, full in sorted(KANA_MAP_HALF_TO_FULL.items(), key=lambda x: -len(x[0])):
		s = s.replace(half, full)
	#ヵ/ヶの特別対応
	s = s.replace("ヵ", "カ").replace("ヶ", "ケ")
	return s


#ひらがな⇒カタカナ変換
def convert_kana_hira_to_kata(s: pd.Series) -> pd.Series:
	return	s.apply(convert_kana_hira_to_kata_detail)

def convert_kana_hira_to_kata_detail(s: str) -> str:
	return "".join(
		chr(ord(c) + 0x60) if "ぁ" <= c <= "ゖ" else c
		for c in s
	)


#コード値連番作成
def create_new_codes(codes: pd.Series, cnt: int) -> pd.Series:
	#0件のコード値を生成するよう言われたら、空のSeriesを返却
	if cnt == 0:
		return pd.Series(dtype="object")

	#既存コードの最大値を取得
	if codes.empty or codes.dropna().empty:
		start_code = 1
	else:
		#intに変換して最大値を取得
		max_code = codes.dropna().astype(int).max()
		start_code = max_code + 1

	#rangeで連番生成して、左側ゼロ埋め
	s = pd.Series(
		[str(code).zfill(10) for code in range(start_code, start_code + cnt)],
		index=range(cnt),
		dtype="object"
	)

	return s


#都道府県名から都道府県コードを逆引き
def get_pref_code(prefecture_names: pd.Series) -> pd.Series:
	hook = BigQueryHook(gcp_conn_id="google_cloud_default", use_legacy_sql=False, project_id=None)
	df_prefecture = hook.get_pandas_df("SELECT prefecture_code, prefecture_name FROM `dwh.m_prefecture`;")
	pref_map = dict(zip(df_prefecture["prefecture_name"], df_prefecture["prefecture_code"]))
	return prefecture_names.map(pref_map)


#Seriesを正規表現の文字列に置換 「どれか1つでも一致するものがあるか判定」用
def series_to_prefix_regex(prefixes: pd.Series) -> str:
	cleaned = (
		prefixes
		.dropna()
		.astype(str)
		.map(re.escape)   #正規表現メタ文字対策
		.tolist()
	)

	#結果Nullの場合、「何にもマッチしない正規表現」を返却
	if not cleaned:
		return r"$^"

	return r"^(" + "|".join(cleaned) + r")"


#入力値から都道府県名を抽出
def extract_prefecture(value: str) -> str:
	if not isinstance(value, str):
		return ""

	#2〜3文字の文字列 + 都/道/府/県のいずれか が連続している文字列部分を抽出
	match = re.search(r'[一-龥]{2,3}(都|道|府|県)', value)
	if match:
		return match.group()

	return ""


#Proxy/Pendulum/標準datetime ⇒ Pandas.Timestamp変換
def to_pandas_timestamp(dt):
	#Proxy型の場合、_datetime属性を取り出す
	if type(dt).__name__ == "Proxy" and hasattr(dt, "_datetime"):
		dt = dt._datetime

	#Pendulum DateTimeの場合、標準datetimeに変換
	if isinstance(dt, pendulum.DateTime):
		dt = dt.in_timezone("UTC").naive()

	#最終的にPandas.Timestampに変換
	return pd.Timestamp(dt)

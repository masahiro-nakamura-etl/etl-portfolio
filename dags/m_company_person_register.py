#【取引先マスタ・取引先人物連絡先マスタ連携】
#
#各システムに散在している取引先情報、および取引先人物の連絡先情報を、粒度を統一して情報基盤となるマスタテーブルに連携する。
#日次バッチとして実行し、所定のディレクトリ上に格納されている、各システムから前日に連携されたCSVファイルを読み込んで実行。
#
#下流システム側でデータを（単純な整形以外で）加工する事を避けるため、エラーデータの修正・リラン機能はない。
#リランの際は、上流システム側のデータを修正し、翌日の日次処理に混ぜ込んで取り込み直すこと。
#（上流側のデータ修正を促す目的で、エラーリスト画面をBI側で用意している）
#
#----------ポートフォリオとして----------
#本DAGは、処理フローと業務設計をコード上で明示する目的で、実装技術上の再利用性・抽象化よりも、可読性と処理単位の明快さを優先している。

from airflow import DAG
from airflow.operators.python import PythonOperator
from bizlogic.crm import *
from bizlogic.name_card import *
from bizlogic.purchase import *
from bizlogic.sales import *
from bizlogic.invoice import *
from airflow.utils import timezone

#毎日、UTC（グリニッジ標準時刻）13:00 ＝ JST（日本時間）22:00に起動
#BigQueryがJST非対応のため、コード上の時刻表記はUTCで統一する
# ■注意■ UTC15:00以降に設定すると、UTC-JSTの時差が日付を跨いでしまい、「本日登録されたデータを対象に処理」の部分が不具合を起こす恐れがある
with DAG(
	dag_id="m_company_person_register",
    schedule_interval="0 13 * * *", 
    start_date=timezone.datetime(2025, 12, 14, 13, 0, tzinfo=timezone.utc),
	catchup=False,
	max_active_runs=1
) as dag:

	#▼データソース：CRM
	#CSVからデータレイクへ取込
	def insert_crm_lead_from_csv_task(**context):
		now = to_pandas_timestamp(context["execution_date"])
		insert_crm_lead_from_csv(now)
	
	#データレイクからworkテーブルへ取込
	def insert_work_crm_lead_task(**context):
		now = to_pandas_timestamp(context["execution_date"])
		insert_work_crm_lead(now)

	#取り込んだデータのバリデーション
	def validate_work_crm_lead_task(**context):
		now = to_pandas_timestamp(context["execution_date"])
		validate_work_crm_lead(now)
	
	#住所・氏名・電話番号の正規化および会社データ・個人データの切り分け
	def normalize_work_crm_lead_task(**context):
		normalize_work_crm_lead()

	#名寄せしてDWHにINSERT or UPDATE
	def upsert_crm_lead_task(**context):
		now = to_pandas_timestamp(context["execution_date"])
		upsert_crm_lead(now)

	#▼データソース：名刺管理ソフト
	#CSVからデータレイクへ取込
	def insert_name_card_from_csv_task(**context):
		now = to_pandas_timestamp(context["execution_date"])
		insert_name_card_from_csv(now)

	#データレイクからworkテーブルへ取込
	def insert_work_name_card_task(**context):
		now = to_pandas_timestamp(context["execution_date"])
		insert_work_name_card(now)

	#取り込んだデータのバリデーション
	def validate_work_name_card_task(**context):
		now = to_pandas_timestamp(context["execution_date"])
		validate_work_name_card(now)
	
	#住所・氏名・電話番号の正規化および会社データ・個人データの切り分け
	def normalize_work_name_card_task(**context):
		normalize_work_name_card()

	#名寄せしてDWHにINSERT or UPDATE
	def upsert_name_card_task(**context):
		now = to_pandas_timestamp(context["execution_date"])
		upsert_name_card(now)

	#▼データソース：支払帳簿
	#CSVからデータレイクへ取込
	def insert_purchase_from_csv_task(**context):
		now = to_pandas_timestamp(context["execution_date"])
		insert_purchase_from_csv(now)

	#データレイクからworkテーブルへ取込
	def insert_work_purchase_task(**context):
		now = to_pandas_timestamp(context["execution_date"])
		insert_work_purchase(now)

	#取り込んだデータのバリデーション
	def validate_work_purchase_task(**context):
		now = to_pandas_timestamp(context["execution_date"])
		validate_work_purchase(now)
	
	#住所・氏名・電話番号の正規化および会社データ・個人データの切り分け
	def normalize_work_purchase_task(**context):
		normalize_work_purchase()

	#名寄せしてDWHにINSERT or UPDATE
	def upsert_purchase_task(**context):
		now = to_pandas_timestamp(context["execution_date"])
		upsert_purchase(now)

	#▼データソース：売上帳簿
	#CSVからデータレイクへ取込
	def insert_sales_from_csv_task(**context):
		now = to_pandas_timestamp(context["execution_date"])
		insert_sales_from_csv(now)

	#データレイクからworkテーブルへ取込
	def insert_work_sales_task(**context):
		now = to_pandas_timestamp(context["execution_date"])
		insert_work_sales(now)

	#取り込んだデータのバリデーション
	def validate_work_sales_task(**context):
		now = to_pandas_timestamp(context["execution_date"])
		validate_work_sales(now)
	
	#住所・氏名・電話番号の正規化および会社データ・個人データの切り分け
	def normalize_work_sales_task(**context):
		normalize_work_sales()

	#名寄せしてDWHにINSERT or UPDATE
	def upsert_sales_task(**context):
		now = to_pandas_timestamp(context["execution_date"])
		upsert_sales(now)

	#▼データソース：請求書
	#CSVからデータレイクへ取込
	def insert_invoice_from_csv_task(**context):
		now = to_pandas_timestamp(context["execution_date"])
		insert_invoice_from_csv(now)

	#データレイクからworkテーブルへ取込
	def insert_work_invoice_task(**context):
		now = to_pandas_timestamp(context["execution_date"])
		insert_work_invoice(now)

	#取り込んだデータのバリデーション
	def validate_work_invoice_task(**context):
		now = to_pandas_timestamp(context["execution_date"])
		validate_work_invoice(now)
	
	#住所・氏名・電話番号の正規化および会社データ・個人データの切り分け
	def normalize_work_invoice_task(**context):
		normalize_work_invoice()

	#名寄せしてDWHにINSERT or UPDATE
	def upsert_invoice_task(**context):
		now = to_pandas_timestamp(context["execution_date"])
		upsert_invoice(now)


	#▼タスク定義
	crm_task1 = PythonOperator(
		task_id="insert_crm_lead_from_csv",
		python_callable=insert_crm_lead_from_csv_task
	)

	crm_task2 = PythonOperator(
		task_id="insert_work_crm_lead",
		python_callable=insert_work_crm_lead_task
	)

	crm_task3 = PythonOperator(
		task_id="validate_work_crm_lead",
		python_callable=validate_work_crm_lead_task
	)
	
	crm_task4 = PythonOperator(
		task_id="normalize_work_crm_lead",
		python_callable=normalize_work_crm_lead_task
	)

	dwh_task1 = PythonOperator(
		task_id="upsert_crm_lead",
		python_callable=upsert_crm_lead_task
	)

	namecard_task1 = PythonOperator(
		task_id="insert_name_card_from_csv",
		python_callable=insert_name_card_from_csv_task
	)

	namecard_task2 = PythonOperator(
		task_id="insert_work_name_card",
		python_callable=insert_work_name_card_task
	)

	namecard_task3 = PythonOperator(
		task_id="validate_work_name_card",
		python_callable=validate_work_name_card_task
	)
	
	namecard_task4 = PythonOperator(
		task_id="normalize_work_name_card",
		python_callable=normalize_work_name_card_task
	)

	dwh_task2 = PythonOperator(
		task_id="upsert_name_card",
		python_callable=upsert_name_card_task
	)

	purchase_task1 = PythonOperator(
		task_id="insert_purchase_from_csv",
		python_callable=insert_purchase_from_csv_task
	)

	purchase_task2 = PythonOperator(
		task_id="insert_work_purchase",
		python_callable=insert_work_purchase_task
	)

	purchase_task3 = PythonOperator(
		task_id="validate_work_purchase",
		python_callable=validate_work_purchase_task
	)
	
	purchase_task4 = PythonOperator(
		task_id="normalize_work_purchase",
		python_callable=normalize_work_purchase_task
	)

	dwh_task3 = PythonOperator(
		task_id="upsert_purchase",
		python_callable=upsert_purchase_task
	)

	sales_task1 = PythonOperator(
		task_id="insert_sales_from_csv",
		python_callable=insert_sales_from_csv_task
	)

	sales_task2 = PythonOperator(
		task_id="insert_work_sales",
		python_callable=insert_work_sales_task
	)

	sales_task3 = PythonOperator(
		task_id="validate_work_sales",
		python_callable=validate_work_sales_task
	)
	
	sales_task4 = PythonOperator(
		task_id="normalize_work_sales",
		python_callable=normalize_work_sales_task
	)

	dwh_task4 = PythonOperator(
		task_id="upsert_sales",
		python_callable=upsert_sales_task
	)

	invoice_task1 = PythonOperator(
		task_id="insert_invoice_from_csv",
		python_callable=insert_invoice_from_csv_task
	)

	invoice_task2 = PythonOperator(
		task_id="insert_work_invoice",
		python_callable=insert_work_invoice_task
	)

	invoice_task3 = PythonOperator(
		task_id="validate_work_invoice",
		python_callable=validate_work_invoice_task
	)
	
	invoice_task4 = PythonOperator(
		task_id="normalize_work_invoice",
		python_callable=normalize_work_invoice_task
	)

	dwh_task5 = PythonOperator(
		task_id="upsert_invoice",
		python_callable=upsert_invoice_task
	)

	#▼実行順制御
	#各データソースごとに「CSVからデータレイクへ取込」「データレイクからワークテーブルへ項目絞って取込」「バリデーション」「入力値正規化」までは並行処理
	#DWHへのINSERT/UPDATEについては、「既存の会社/人物の登録有無」によって挙動が変わるので、優先度に沿って直列処理
	#優先度は CRM(※見込み客などの不正確な情報を含む) < 名刺管理ソフト < 支払帳簿 < 売上帳簿 < 請求書(※100%正確なはず) としている
	crm_task1 >> crm_task2 >> crm_task3 >> crm_task4
	namecard_task1 >> namecard_task2 >> namecard_task3 >> namecard_task4
	purchase_task1 >> purchase_task2 >> purchase_task3 >> purchase_task4
	sales_task1 >> sales_task2 >> sales_task3 >> sales_task4
	invoice_task1 >> invoice_task2 >> invoice_task3 >> invoice_task4
	[crm_task4, namecard_task4, purchase_task4, sales_task4, invoice_task4] >> dwh_task1 >> dwh_task2 >> dwh_task3 >> dwh_task4 >> dwh_task5

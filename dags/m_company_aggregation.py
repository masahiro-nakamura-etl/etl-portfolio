#【取引先マスタ集約】
#
#【取引先マスタ・取引先人物連絡先マスタ連携】での機械的な名寄せで集約されなかった取引先を、人間系で集約する処理。
#集約先・集約元を記載したCSVを入力データとし、DWH上の情報をマージ、不要なレコードを削除する。
#
#原則、BI側で公開している「取引先マスタ集約候補一覧」画面を参照し、
#業務的な経緯を知っている担当者の判断により同一取引先かどうかを判断の上で入力用CSVを作る想定。
#
#※なお、取引先人物連絡先マスタの集約機能は存在しない（2025.12.30現在）。
#　取引先人物連絡先マスタは「氏名」「電話番号」「メールアドレス」のうち2項目完全一致で同一人物判定しており、
#　誤った名寄せや、逆に同一人物が名寄せされないという事が起こるケースが稀なため。

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from bizlogic.aggregation import *
from airflow.utils import timezone

with DAG(
	dag_id="m_company_aggregation",
	schedule=None,
	start_date=timezone.datetime(2025, 1, 1),
	catchup=False
) as dag:

	#▼データソース：CRM
	#CSVからデータレイクへ取込
	def check_aggregation_csv_task(**context):
		return check_aggregation_csv()

	def merge_m_company_task(**context):
		now = to_pandas_timestamp(context["execution_date"])
		merge_m_company(now)

	#▼タスク定義
	check_task = ShortCircuitOperator(
		task_id="check_aggregation_csv",
		python_callable=check_aggregation_csv_task
	)

	merge_task = PythonOperator(
		task_id="merge_m_company",
		python_callable=merge_m_company_task
	)

	#▼実行順制御
	check_task >> merge_task

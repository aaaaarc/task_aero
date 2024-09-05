import pendulum
from airflow.decorators import task, dag
import requests
import psycopg2
import logging
import pandas as pd

logger = logging.getLogger(__name__)

handler = logging.FileHandler(f"{__name__}.log", mode='a') # название файла для записи логов и режим записи - записи из всех сеансов
formatter = logging.Formatter("%(name)s %(asctime)s %(levelname)s %(message)s") # формат записи - имя, время, уровень важности сообщения, сообщение

handler.setFormatter(formatter) # все логи, обрабатываемые handler будут отформатированы formatter
logger.addHandler(handler) # логи логгера отправляются handler

@dag(
    'dag_test_api', # название дага
    schedule_interval='0 0 */12 * * *', # расписание запуска (каждые 12 часов??   в 0 сек, 0 мин и в 0 или 12 часов в любой (*) день и год)
    tags=['api'], # тег для поиска по дагам в airflow
    start_date=pendulum.datetime(2024, 9, 1), # с какой даты начать запуск дага
    catchup=False, # нужно ли прогружать исторические данные
    default_args={
        'owner': 'admin'
    }
)

def dag_test_api():
    # настроить импорт данных с api
    @task
    def get_data():
        logger.info("Запрос данных с api")
        try:
            response = requests.get('https://random-data-api.com/api/cannabis/random_cannabis?size=10') # получили инфу с сайта
            logger.info("Запрос успешно выполнен")

            try:
                data = response.json()
                logger.info("Данные были преобразованы в json")
            except ValueError as e:
                logger.error(f"Ошибка преобразования данных в json: {e}")
                data = None

        except requests.ConnectionError as e:
            logger.error(f"Ошибка подключения: {e}")
            data = None
        except requests.Timeout as e:
            logger.error(f"Ошибка тайм-аута: {e}")
            data = None
        except requests.RequestException as e:
            logger.error(f"Ошибка запроса: {e}")
            data = None

        return data

    @task
    def insert_data(data):
        try:
            connection = psycopg2.connect(
                dbname="aero_testtask",
                user="admin",
                password="admin",
                host="localhost",
                port="5432"
            )
            logger.info("Соединение с бд установлено")

            try:
                cursor = connection.cursor()
                query = "insert into cannabis values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                for i in range(len(data)):
                    id = data[i]['id']
                    uid = data[i]['uid']
                    strain = data[i]['strain']
                    cannabinoid_abbreviation = data[i]['cannabinoid_abbreviation']
                    cannabinoid = data[i]['cannabinoid']
                    terpene = data[i]['terpene']
                    medical_use = data[i]['medical_use']
                    health_benefit = data[i]['health_benefit']
                    category = data[i]['category']
                    type = data[i]['type']
                    buzzword = data[i]['buzzword']
                    brand = data[i]['brand']
                    cursor.execute(query, (
                    id, uid, strain, cannabinoid_abbreviation, cannabinoid, terpene, medical_use, health_benefit, category,
                    type, buzzword, brand))
                    logger.info(f"Строка с id {id} занесена в таблицу")
                connection.commit()
                cursor.close()
                logger.info("Изменения в бд сохранены")
            except psycopg2.DatabaseError as e:
                logger.error(f"Ошибка с выполнением запроса к бд: {e}")
                connection.rollback()
            finally:
                connection.close()
                logger.info("Соединение завершено")

        except psycopg2.OperationalError as e:
            logger.error(f"Ошибка с уcтановкой соединения с бд: {e}")

    @task
    def metrics(data):
        try:
            df = pd.DataFrame(data)
            logger.info("Данные были преобразованы датафрейм")

            completeness = df.notnull().mean()  # доля непустых знач (завершенность)
            logger.info(f"Завершенность (доля непустых значений): {completeness}")

            uniqueness = df.nunique() / len(df)  # доля уникальных значений (Достоверность данных — Validity (уникальность))
            logger.info(f"Достоверность данных (доля уникальных значений): {uniqueness}")

            data_types = df.dtypes  # ограничения типа данных (тип данных в каждом столбце) (Достоверность данных — Validity)
            logger.info(f"Достоверность данных (тип данных для каждого столбца): {data_types}")

            validity = df.isnull().mean()  # доля пропущенных значений в каждом столбце (Достоверность данных — Validity)
            logger.info(f"Достоверность данных (доля пустых значений): {validity}")

            duplicates = df.duplicated
            logger.info(f"Дупликация (дуплирующиеся строки): {duplicates}")

        except ValueError as e:
            logger.error(f"Ошибка преобразования данных в датафрейм: {e}")

    data = get_data()
    insert_data(data)
    metrics(data)

    # get_data() >> insert_data()


mydag = dag_test_api()








































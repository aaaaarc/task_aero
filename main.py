import pendulum
from airflow.decorators import task, dag
import requests
import psycopg2

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
        response = requests.get('https://random-data-api.com/api/cannabis/random_cannabis?size=10') # получили инфу с сайта
        data = response.json() # перевели в формат json
        return data

    @task
    def insert_data(data):
        connection = psycopg2.connect(
            dbname="aero_testTask",
            user="admin",
            password="admin",
            host="localhost",
            port="5432"
        )
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
        connection.commit()
        cursor.close()
        connection.close()

    data = get_data()
    insert_data(data)

    # get_data() >> insert_data()


mydag = dag_test_api()








































from sqlalchemy import create_engine
import pymysql
import pandas as pd
import json
import datetime
from json import JSONEncoder
from io import StringIO
from pandas.io.json import json_normalize


class DateTimeEncoder(JSONEncoder):
    """
    Перезаписываем формат даты
    """
    def default(self, obj):
        if isinstance(obj, (datetime.date, datetime.datetime)):
            return obj.isoformat()


""" Соединяемся с базой """
sqlEngine = create_engine('mysql+pymysql://root:root@127.0.0.1/dump_origin', pool_recycle=3600)
dbConnection = sqlEngine.connect()
""" Делаем запрос к таблице """
data_from_bd_wordpress = pd.read_sql("SELECT * FROM view_products", dbConnection)
pd.set_option('display.expand_frame_repr', False)

df = pd.DataFrame(data_from_bd_wordpress)
""" Без force_ascii следующая строка encode и decode не работает """
string_result_json_data = df.to_json(orient='table', index=False, force_ascii=False)
""" Кириллица в базе была закодирована """
result_read_string_json = json.loads(string_result_json_data.encode('cp866').decode('utf8'))

""" Читаем наш json-файл с раскодированными данными """
with open("myjsonp.json", "w", encoding='utf-8') as file:
    json.dump(result_read_string_json, file, ensure_ascii=False, cls=DateTimeEncoder)
""" Производим нормализацию JSON, так как в нем есть лишние данные """
json_data_normalize = json_normalize(result_read_string_json['data'])
""" Таблица, в которую переносим нормализированные данные """
tableName = "wp_posts_new"

try:
    """ Если таблица существует, то перезаписываем данные в ней """
    frame = json_data_normalize.to_sql(tableName, dbConnection, if_exists='replace')
except ValueError as vx:
    print(vx)
except Exception as ex:
    print(ex)
else:
    print("Таблица успешно создана: " % tableName)
finally:
    dbConnection.close()

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


sqlEngine = create_engine('mysql+pymysql://root:root@127.0.0.1/dump_origin', pool_recycle=3600)
dbConnection = sqlEngine.connect()

frame = pd.read_sql("SELECT * FROM view_products", dbConnection)


pd.set_option('display.expand_frame_repr', False)

df = pd.DataFrame(frame)
result = df.to_json(orient='table', index=False, force_ascii=False)

parser = json.loads(result.encode('cp866').decode('utf8'))


with open("myjsonp.json", "w", encoding='utf-8') as file:
    json.dump(parser, file, ensure_ascii=False, cls=DateTimeEncoder)


dp = json_normalize(parser['data'])
print(dp)

# yyy = pd.DataFrame(kkk)
tableName = "wp_posts_new"

try:
    frame = dp.to_sql(tableName, dbConnection, if_exists='replace')

except ValueError as vx:

    print(vx)

except Exception as ex:

    print(ex)

else:

    print("Table %s created successfully." % tableName)

finally:
    dbConnection.close()

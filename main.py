import os
import pandas as pd
from pandas import json_normalize
import json
from datetime import datetime
import schedule 
import time


from pymongo import MongoClient

from dotenv import load_dotenv
load_dotenv()

from api import Api

router = Api(address=os.environ["ip_routerboard"],
             user=os.environ["user_router_os"],
             password=os.environ["pw_router_os"],
             port=os.environ["port_routerboard"])

client = MongoClient(os.environ["uri_banco"])
database = client[os.environ["Database_Name"]]

lista_interfaces = ['ether1']


def atualiza_lista_interfaces():
    global lista_interfaces
    collection = database['rb_interface']

    # Passo 1: Executar a primeira consulta para obter os valores distintos
    distinct_values = collection.distinct("name")

    # Passo 2: Usar o resultado da primeira consulta na segunda consulta
    subquery_result = collection.find({'name': {'$in': distinct_values}})

    df = pd.DataFrame(subquery_result)
    lista_interfaces = pd.unique(df['name'])


# -----------------------------------------------------------------------------
#
# -----------------------------------------------------------------------------
def monitor_traffic():
    
    collection = database['rb_monitor_traffic']
    global lista_interfaces

    lista_separada = ','.join(lista_interfaces).split(',')
    lista_string = str(lista_separada).replace('[', '').replace(']', '').replace("'", '')

    interface_str = '=interface='+ lista_string
    try:
        message = [('/interface/monitor-traffic', interface_str, '=once=true')]
        response = router.talk(message)
        data_dict = response[0]
        
        df = pd.DataFrame(data_dict)
        df["data"] = datetime.now().strftime("%Y-%m-%d")
        df["hora"] = datetime.now().strftime("%H:%M:%S")
        df["data-hora"] = datetime.now().isoformat()

        # Converter o data frame em um formato compatível com JSON
        df_json = df.to_dict(orient='records')
        collection.insert_many(df_json)
        print("traffic test finished")
        

    except router.LoginError(Exception) as e:
        print(f"Ocorreu um erro na requisição: {e}")


# -----------------------------------------------------------------------------
#
# -----------------------------------------------------------------------------

def interfaces():

    collection = database['rb_interface']

    try:
        response = router.talk('/interface/print')
        response = [response]
        data_dict = response[0]

        # Converter a lista em um data frame
        df = pd.DataFrame(data_dict)
        df["data"] = datetime.now().strftime("%Y-%m-%d")
        df["hora"] = datetime.now().strftime("%H:%M")
        df["data-hora"] = datetime.now().isoformat()

        # Converter o data frame em um formato compatível com JSON
        df_json = df.to_dict(orient='records')
        collection.insert_many(df_json)
        print("Teste interface finalizado")

    except router.LoginError(Exception) as e:
        print(f"Ocorreu um erro na requisição: {e}")

# -----------------------------------------------------------------------------
#
# -----------------------------------------------------------------------------

def ping():
    message = [('/ping', '=address=1.1.1.1','=count=5')]
    collection = database['rb_ping']

    try:
        response = router.talk(message)
        data_dict = response[0]

        # Converter a lista em um data frame
        df = pd.DataFrame(data_dict)
        df["data"] = datetime.now().strftime("%Y-%m-%d")
        df["hora"] = datetime.now().strftime("%H:%M")
        df["data-hora"] = datetime.now().isoformat()

        # Converter o data frame em um formato compatível com JSON
        df_json = df.to_dict(orient='records')
        
        print (df_json)
        collection.insert_many(df_json)


        print("bkp Finalizado")

    except router.LoginError(Exception) as e:
        print(f"Ocorreu um erro na requisição: {e}")


# -----------------------------------------------------------------------------
#
# -----------------------------------------------------------------------------
def Insert_alert(severity, alert_name, object):
    collection = database['rb_alerts']


    df = pd.DataFrame()
    df["severity"] = severity
    df["alert_name"] = alert_name
    df["object"] = object
    df["data"] = datetime.now().strftime("%Y-%m-%d")
    df["hora"] = datetime.now().strftime("%H:%M")
    df["data-hora"] = datetime.now().isoformat()
    # Converter o data frame em um formato compatível com JSON
    df_json = df.to_dict(orient='records')
        
    print (df_json)
    collection.insert_many(df_json)

    return 




def neighbor():
    message = [('/ip/neighbor','print')]
    collection = database['rb_neighbors']

    try:
        response = router.talk('/ip/neighbor/print')
        # Converter a lista em um data frame
        df = pd.DataFrame(response)
        df["data"] = datetime.now().strftime("%Y-%m-%d")
        df["hora"] = datetime.now().strftime("%H:%M")
        df["data-hora"] = datetime.now().isoformat()

        # Converter o data frame em um formato compatível com JSON
        df_json = df.to_dict(orient='records')
        
        print (df_json)
        collection.insert_many(df_json)


        print("bkp Finalizado")

    except router.LoginError(Exception) as e:
        print(f"Ocorreu um erro na requisição: {e}")

# -----------------------------------------------------------------------------
#
# -----------------------------------------------------------------------------

def route_test():
    collection = database['rb_routes']

    try:
        response = router.talk('/ip/route/print')
        #print (response)
        response = [response]
        data_dict = response[0]
        df = pd.DataFrame(data_dict)
        df["data"] = datetime.now().strftime("%Y-%m-%d")
        df["hora"] = datetime.now().strftime("%H:%M")
        df["data-hora"] = datetime.now().isoformat()

        # Converter o data frame em um formato compatível com JSON
        df_json = df.to_dict(orient='records')
        collection.insert_many(df_json)


        print("Teste rotas Finalizado")

    except router.LoginError(Exception) as e:
        print(f"Ocorreu um erro na requisição: {e}")
# -----------------------------------------------------------------------------
#
# -----------------------------------------------------------------------------
def health():
    message = [('/system/health/print')]

    try:
        response = router.talk(message)
        print(response)
        return response[0]
    except router.LoginError(Exception) as e:
        print(f"Ocorreu um erro na requisição: {e}")


# -----------------------------------------------------------------------------
#
# -----------------------------------------------------------------------------
def sytem_status():
    collection = database['rb_sytem']

    try:
        response = router.talk('/system/resource/print')

        temperarura = health()
        data=response[0]

        data["cpu-temperature"] = (temperarura[0]['value'])
        data["temperature-scale"] = (temperarura[0]['type'])
        data["data"] = datetime.now().strftime("%Y-%m-%d")
        data["hora"] = datetime.now().strftime("%H:%M:%S") 
        data["data-hora"] = datetime.now().isoformat()

        collection.insert_one(data)


    except router.LoginError(Exception) as e:
        print(f"Ocorreu um erro na requisição: {e}")



def monitor():
    
    
    Insert_alert
    return

schedule.every(2).seconds.do(sytem_status) 
schedule.every(1).seconds.do(monitor_traffic)
schedule.every(120).seconds.do(interfaces)
schedule.every(30).seconds.do(neighbor)
schedule.every(60).seconds.do(ping)
schedule.every(120).seconds.do(route_test)


if __name__ == '__main__':
    route_test()
    interfaces()
    atualiza_lista_interfaces()
    monitor_traffic()
    sytem_status()
    ping()

    while True:
       schedule.run_pending()
       time.sleep(1)

    
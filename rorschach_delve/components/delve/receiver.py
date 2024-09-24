from nite_howl import NiteHowl, minute
from vtsync.database.engine.database import Database
from utils.utils import Utils
from os import getenv, path
from sqlalchemy import text
import pandas as pd
import time 
import toml

class Receiver:
    def __init__(self):
        self.broker = getenv('BROKER')
        self.topic = getenv('TOPIC')
        self.group = getenv('GROUP')
        self.env =  getenv('ENV_PATH')
        self.howler = NiteHowl(self.broker, self.group, str(self.topic).split(","), "delve")
        
    def catch(self):
        radar_generator = self.howler.radar()
        primary = None
        secondary = None
        headers_primary = None
        while True:
            try:
                minute.register("info", f"Searching topics for transform...")
                table, topic, key, headers, type = next(radar_generator)
                if key == "delve":
                    if headers and "type" in headers.keys() and headers["type"] == "crm":
                        secondary = table
                    else:
                        primary = table
                        self.generate(key, headers)
                
                    if isinstance(primary, pd.DataFrame) and isinstance(secondary, pd.DataFrame) and not primary.empty and not secondary.empty:
                        self.join(primary, secondary, key)
                        primary = None
                        secondary = None
                else:
                    minute.register("info", f"That larva '{key}' won't need me.")
            except StopIteration:
                # Si radar_generator se agota, crea una nueva instancia
                radar_generator = self.howler.radar()
            # Pausa breve para no saturar el bucle
            time.sleep(0.1)
    
    def generate(self, key, headers):
        file_path = path.join(f'{self.env}/database.toml')
        env_config = toml.load(file_path)
        
        database = env_config["database"][1]

        user = database["user"]
        password = database["password"]
        host = database["host"]
        port = database["port"]
        type = database["type"]
        conn = database["conn"]
        database_name = database["database"]

        client = Database(database_name,user,password,host,port,type,conn)
        minute.register("info", f"Connect to {database_name}-{user}-{password}-{host}-{port}-{type}-{conn}")
        utils = Utils()
        query = f"""
            SELECT a.*
            FROM vtigercrm_2022.vtiger_salesordercf AS a
                INNER JOIN vtigercrm_2022.vtiger_salesorder AS b ON a.salesorderid = b.salesorderid
                JOIN vtigercrm_2022.vtiger_contactscf AS c ON b.contactid = c.contactid
                JOIN vtigercrm_2022.vtiger_crmentity AS d ON b.salesorderid = d.crmid
            WHERE d.deleted = '0'
                AND a.cf_2141 <> 'Cancelación'
                AND a.cf_2193 >= '{utils.today.strftime("%Y-%m-%d")}'
                {f"AND a.cf_2069 = '{key.capitalize()}'" if key != "healthsherpa" else ""}
                {f"AND a.cf_2067 = '{'Beatriz Sierra' if headers["subregistry"] == 'BS' else ('Ana Daniella Corrales' if headers["subregistry"] == 'ADC' else 'Juan Ramirez')}'" if headers["subregistry"] else "AND a.cf_2067 <> 'Otro Broker'"}
        """
        df = pd.read_sql_query(text(query), client.engine)
        df = df.drop_duplicates()
        self.howler.send("mask", msg=df, key="vtigercrm", headers={"type": "crm"})
        
    def join(self, primary, secondary, key):       
        self.howler.send("scrutinise", msg=primary, key=key, headers={"side": "left"})
        self.howler.send("scrutinise", msg=secondary, key=key, headers={"side": "right"})
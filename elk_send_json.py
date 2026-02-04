"""
Modulo per simulare e inviare log a Elasticsearch
"""
import json
import random
import time
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from typing import Dict, List
from pprint import pprint



def main():
    """Funzione principale per demo"""
    # Configurazione connessione (modifica questi parametri)
    PROTOCOL = "http"
    HOST = "20.105.91.235"
    PORT = 9200
    
    # Opzione 1: Autenticazione con Username/Password
    USERNAME = None  # Imposta se necessario
    PASSWORD = None  # Imposta se necessario
    
    # Opzione 2: Autenticazione con API Key (alternativa a username/password)
    # Formato: "id:api_key" o "base64_encoded_key"
    # API_KEY = None  # Es: "VnVhQ2ZHY0JDZGJrUW0tZTVhT3g6dWkybHAyYXhUTm1zeWFrdzl0dk5udw=="
    API_KEY = "WFJINUlwd0JaSG8xX01GRm9XenE6cGo2RHN4eFJ1YUw0bXdMTHBnLUtfdw=="
    
    elastic = None
    
    if API_KEY:
        elastic = Elasticsearch(
            [f"{PROTOCOL}://{HOST}:{PORT}"],
            api_key=API_KEY
        )
        print(f"🔑 Autenticazione tramite API Key")
    elif USERNAME and PASSWORD:
        elastic = Elasticsearch(
            [f"{PROTOCOL}://{HOST}:{PORT}"],
            basic_auth=(USERNAME, PASSWORD)
        )
        print(f"🔐 Autenticazione tramite Username/Password")
    else:
        elastic = Elasticsearch([f"{PROTOCOL}://{HOST}:{PORT}"])
        print(f"⚠️  Connessione senza autenticazione")
        
    # Verifica connessione
    if elastic.ping():
        print(f"✓ Connessione a Elasticsearch stabilita su {HOST}:{PORT}")
    else:
        print(f"✗ Impossibile connettersi a Elasticsearch su {HOST}:{PORT}")
    
    
    log = {
        "timestamp": (datetime.now() - timedelta(hours=1)).isoformat(),
        "service": "service",
        "status": "ok",
        "response_time_ms": random.randint(10, 5000),
        "request_id": f"req-{random.randint(100000, 999999)}",
        "environment": random.choice(["production", "staging", "development"])
    }
    
    logs = []
    
    index_name = "services-log-2024-06"
    isBulk = False
    if isBulk:
        actions = [
            {
                "_index": index_name,
                "_source": log
            }
            for log in  logs
        ]
        
        try:
            success, failed = bulk(elastic, actions, stats_only=True)
            print(f"\n✓ Bulk insert completato: {success} successi, {failed} fallimenti")
            return {"success": success, "failed": failed}
        except Exception as e:
            print(f"✗ Errore nel bulk insert: {e}")
            return {"success": 0, "failed": len(logs)}
        
        # success, failed = bulk(elastic, actions, stats_only=True)
        #print(f"\n✓ Bulk insert completato: {success} successi, {failed} fallimenti")
    else:
        response = elastic.index(index=index_name, document=log)
        print(f"✓ Log inviato: {log['service']} - {log['status']} (ID: {response['_id']})")
    
    
   


if __name__ == "__main__":
    main()

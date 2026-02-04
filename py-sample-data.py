from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from pprint import pprint

def main():
    print("Test ELK !")
    
    PROTOCOL = "http"
    HOST = "20.105.91.235"
    PORT = 9200
    
    USERNAME = "elastic"
    PASSWORD = "GsYXF4kl"
    API_KEY = "WFJINUlwd0JaSG8xX01GRm9XenE6cGo2RHN4eFJ1YUw0bXdMTHBnLUtfdw=="
    
    elastic = None
    
    ## Connessione a Elasticsearch
    if API_KEY:
        elastic = Elasticsearch(
            hosts=[f"{PROTOCOL}://{HOST}:{PORT}"],
            api_key=API_KEY,
        )
        print(f"Connected to Elasticsearch using API Key at {HOST}:{PORT}")
    elif USERNAME and PASSWORD:
        elastic = Elasticsearch(
            hosts=[f"{PROTOCOL}://{HOST}:{PORT}"],
            http_auth=(USERNAME, PASSWORD),
        )
        print(f"Connected to Elasticsearch using Username/Password at {HOST}:{PORT}")
    else:
        elastic = Elasticsearch(
            hosts=[f"{PROTOCOL}://{HOST}:{PORT}"]
        )
        print(f"Connected to Elasticsearch without authentication at {HOST}:{PORT}")
        return    
    
    # Verifica della connessione
    if elastic.ping():
        print("Elasticsearch cluster is up!")
    else:
        print("Elasticsearch cluster is down!")
        return          

    log = {  
    }

    query =  """
    FROM sample_data
    | STATS median_duration = MEDIAN(event_duration) by client_ip
    """
    
    
    response = None
    
    try:
        response = elastic.esql.query(query=query)
      
    except Exception as e:
        print(f"Errore durante l'esecuzione della query: {e}")
        
    
    print("Response:")
    pprint(response)
    

if __name__ == "__main__":
    main()
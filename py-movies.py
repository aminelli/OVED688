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

    query = {
        "size": 1,
        "query": {
            "bool": {
                "should": [
                    { "match": { "actor_1_name": "Vin Diesel" }},
                    { "match": { "actor_2_name": "Vin Diesel" }},
                    { "match": { "actor_3_name": "Vin Diesel" }}
                ],
                "minimum_should_match": 1
            }
        },
        "aggs": {
            "by_country": {
                "terms": {
                    "field": "country",
                    "size": 50
                }
            }
        }
    }
     
     
    # index_name = "test-index"
    index_name = "movie_idx"
    
    # response = elastic.index(index=index_name, document=log)
    response = None
    
    try:
        response = elastic.search(index=index_name, body=query)
      
    except Exception as e:
        print(f"Errore durante l'esecuzione della query: {e}")
        
    
    print("Response:")
    pprint(response)
    

if __name__ == "__main__":
    main()
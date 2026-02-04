"""
Modulo per interrogare Elasticsearch e aggregare i film per attore.
"""

from elasticsearch import Elasticsearch
from typing import List, Dict, Any
import json


class MovieElasticsearchClient:
    """Client per interrogare l'indice movie_idx su Elasticsearch."""
    
    def __init__(self, host: str = "localhost", port: int = 9200):
        """
        Inizializza il client Elasticsearch.
        
        Args:
            host: Host di Elasticsearch (default: localhost)
            port: Porta di Elasticsearch (default: 9200)
        """
        self.es = Elasticsearch([{'host': host, 'port': port}])
        self.index_name = "movie_idx"
    
    def verify_connection(self) -> bool:
        """
        Verifica la connessione a Elasticsearch.
        
        Returns:
            True se la connessione è attiva, False altrimenti
        """
        try:
            return self.es.ping()
        except Exception as e:
            print(f"Errore nella connessione a Elasticsearch: {e}")
            return False
    
    def aggregate_movies_by_actor(self) -> Dict[str, Any]:
        """
        Aggrega i film per attore usando i campi actor_1_name, actor_2_name, actor_3_name.
        
        Returns:
            Dizionario con i risultati dell'aggregazione
        """
        # Query di aggregazione che combina i tre campi attore
        query = {
            "size": 0,
            "aggs": {
                "actor_1_aggregation": {
                    "terms": {
                        "field": "actor_1_name.keyword",
                        "size": 10000
                    },
                    "aggs": {
                        "movies": {
                            "top_hits": {
                                "size": 100,
                                "_source": ["movie_title", "actor_1_name"]
                            }
                        }
                    }
                },
                "actor_2_aggregation": {
                    "terms": {
                        "field": "actor_2_name.keyword",
                        "size": 10000
                    },
                    "aggs": {
                        "movies": {
                            "top_hits": {
                                "size": 100,
                                "_source": ["movie_title", "actor_2_name"]
                            }
                        }
                    }
                },
                "actor_3_aggregation": {
                    "terms": {
                        "field": "actor_3_name.keyword",
                        "size": 10000
                    },
                    "aggs": {
                        "movies": {
                            "top_hits": {
                                "size": 100,
                                "_source": ["movie_title", "actor_3_name"]
                            }
                        }
                    }
                }
            }
        }
        
        try:
            response = self.es.search(index=self.index_name, body=query)
            return response
        except Exception as e:
            print(f"Errore durante l'esecuzione della query: {e}")
            return {}
    
    def get_actor_film_list(self) -> List[Dict[str, Any]]:
        """
        Restituisce la lista di tutti i film aggregati per attore con il totale.
        
        Returns:
            Lista di dizionari con attore, film e totale
        """
        results = self.aggregate_movies_by_actor()
        
        if not results:
            return []
        
        actor_films = {}
        
        # Processa le aggregazioni per tutti e tre i campi attore
        for agg_name in ["actor_1_aggregation", "actor_2_aggregation", "actor_3_aggregation"]:
            if agg_name in results.get("aggregations", {}):
                buckets = results["aggregations"][agg_name]["buckets"]
                
                for bucket in buckets:
                    actor_name = bucket["key"]
                    film_count = bucket["doc_count"]
                    
                    # Estrai i titoli dei film
                    movies = []
                    if "movies" in bucket:
                        hits = bucket["movies"]["hits"]["hits"]
                        for hit in hits:
                            movie_title = hit["_source"].get("movie_title", "N/A")
                            if movie_title not in movies:
                                movies.append(movie_title)
                    
                    # Combina i dati per lo stesso attore se appare in più campi
                    if actor_name in actor_films:
                        actor_films[actor_name]["total_films"] += film_count
                        # Aggiungi nuovi film evitando duplicati
                        for movie in movies:
                            if movie not in actor_films[actor_name]["films"]:
                                actor_films[actor_name]["films"].append(movie)
                    else:
                        actor_films[actor_name] = {
                            "actor_name": actor_name,
                            "total_films": film_count,
                            "films": movies
                        }
        
        # Converti in lista e ordina per numero di film (decrescente)
        result_list = list(actor_films.values())
        result_list.sort(key=lambda x: x["total_films"], reverse=True)
        
        return result_list
    
    def print_results(self, limit: int = None):
        """
        Stampa i risultati dell'aggregazione.
        
        Args:
            limit: Numero massimo di risultati da mostrare (None = tutti)
        """
        actor_films = self.get_actor_film_list()
        
        if not actor_films:
            print("Nessun risultato trovato.")
            return
        
        print(f"\n{'=' * 80}")
        print(f"AGGREGAZIONE FILM PER ATTORE - Indice: {self.index_name}")
        print(f"{'=' * 80}\n")
        
        display_list = actor_films[:limit] if limit else actor_films
        
        for idx, actor_data in enumerate(display_list, 1):
            print(f"{idx}. Attore: {actor_data['actor_name']}")
            print(f"   Totale film: {actor_data['total_films']}")
            print(f"   Film:")
            for film in actor_data['films']:
                print(f"      - {film}")
            print()
        
        if limit and len(actor_films) > limit:
            print(f"... e altri {len(actor_films) - limit} attori")
    
    def export_to_json(self, filename: str = "actor_films_aggregation.json"):
        """
        Esporta i risultati in un file JSON.
        
        Args:
            filename: Nome del file di output
        """
        actor_films = self.get_actor_film_list()
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(actor_films, f, ensure_ascii=False, indent=2)
        
        print(f"Risultati esportati in: {filename}")


def main():
    """Funzione principale di esempio."""
    # Inizializza il client (modifica host e porta se necessario)
    client = MovieElasticsearchClient(host="localhost", port=9200)
    
    # Verifica la connessione
    if not client.verify_connection():
        print("Impossibile connettersi a Elasticsearch. Verifica che il servizio sia attivo.")
        return
    
    print("Connessione a Elasticsearch stabilita con successo!")
    
    # Esegui l'aggregazione e stampa i risultati (mostra i primi 20 attori)
    client.print_results(limit=20)
    
    # Esporta i risultati completi in JSON
    client.export_to_json("json/actor_films_aggregation.json")


if __name__ == "__main__":
    main()

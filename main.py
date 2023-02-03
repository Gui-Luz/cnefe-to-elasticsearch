from datetime import datetime
from src.models.elastic_connector import ElasticConnector
from elasticsearch import helpers

if __name__ == '__main__':
    print(f'[+] {datetime.now()} STARTING PROGRAM')
    host = "http://localhost:9200"
    index = 'cnefe_novo'
    filename = 'data/Cnefe2217All.csv'

    ec = ElasticConnector(host, index, filename)
    ec.create_mapping()
    helpers.bulk(ec.create_client(), ec.generate_docs(), raise_on_error=False, raise_on_exception=False, chunk_size=1000)
    print(f'[+] {datetime.now()} DONE!')




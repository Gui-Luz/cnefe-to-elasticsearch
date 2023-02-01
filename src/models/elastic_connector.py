import csv
from datetime import datetime
from elasticsearch import Elasticsearch
import requests
from src.helpers.elastic_mapping import mapping


class ElasticConnector:

    def __init__(self, host, index, filename):
        self._host = host
        self._index = index
        self._filename = filename

    def create_client(self):
        print(f'[+] {datetime.now()} CREATING ELASTICSEARCH CLIENT')
        es_client = Elasticsearch(self._host)
        return es_client

    def create_mapping(self):
        print(f'[+] {datetime.now()} CREATING {self._index} INDEX MAPPING')
        r = requests.put(f'{self._host}/{self._index}', json=mapping)
        print(f'[+] STATUS CODE: {r.status_code}')
        print(f'[+] TEXT: {r.text}')

    def generate_docs(self):
        print(f'[+] {datetime.now()} LOADING DOCS INTO ELASTIC SEARCH')
        with open(self._filename, 'r', encoding="utf-8") as csvfile:
            datareader = csv.reader(csvfile)

            for row in datareader:
                doc = {
                    "_index": "cnefe",
                    "_id": row[0],
                    "_source": {
                        'id_cnefe': row[0],
                        'cnefe_ano': row[1],
                        'id_uf': row[2],
                        'sg_uf': row[3],
                        'id_municipio': row[4],
                        'nm_municipio': row[5],
                        'id_distrito': row[6],
                        'id_subdistrito': row[7],
                        'id_setor': row[8],
                        'situacao_setor': row[9],
                        'codigo_quadra': row[10],
                        'codigo_face': row[11],
                        'codigo_geo': row[12],
                        'id_unico_endereco_ibge': row[13],
                        'especie': row[14],
                        'indicador_endereco': row[15],
                        'logradouro': row[16],
                        'tipo_logradouro': row[17],
                        'titulo_logradouro': row[18],
                        'nome_logradouro': row[19],
                        'numero_imovel': row[20],
                        'tipo_modificador': row[21],
                        'nome_primeiro_complemento': row[22],
                        'valor_primeiro_complemento': row[23],
                        'nome_segundo_complemento': row[24],
                        'valor_segundo_complemento': row[25],
                        'nome_terceiro_complemento': row[26],
                        'valor_terceiro_complemento': row[27],
                        'nome_quarto_complemento': row[28],
                        'valor_quarto_complemento': row[29],
                        'nome_quinto_complemento': row[30],
                        'valor_quinto_complemento': row[31],
                        'dsc_ponto_referencia': row[32],
                        'nome_localidade': row[33],
                        'codigo_cep': row[34],
                        'id_estabelecimento': row[35],
                        'nome_estabelecimento': row[36],
                        'cnefe_lat': row[37],
                        'cnefe_lng': row[38],
                        'altitude': row[39],
                        'face_lat': row[40],
                        'face_lng': row[41],
                        'face_ano': row[42],
                        'face_dist_max_estimada': row[43],
                        'bgeo_lat': row[44],
                        'bgeo_lng': row[45],
                        'bgeo_dist_max_est': row[46],
                        'bgeo_font': row[47],
                        'id_localidade': row[48],
                        'id_logradouro': row[49],
                        'id_localidade_logradouro': row[50],
                        'id_logradouro_numero': row[51],
                        'id_localidade_logradouro_numero': row[52],
                        'coordenadas': f"{row[44]}, {row[45]}",
                        'endereco': f'{row[16]}, {row[20]}, {row[33]}, {row[34]}, {row[5]}, {row[3]}',
                        'endereco_completo': f'{row[16]} {row[20]} {row[22]} {row[23]} {row[25]} {row[26]} {row[27]} {row[28]} {row[29]} {row[30]} {row[31]} {row[33]} {row[34]} {row[5]} {row[3]}'
                    }
                }
                yield doc
    print(datetime.now())
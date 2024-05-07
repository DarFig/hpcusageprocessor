# -*- coding: utf-8 -*-
from elasticsearch import Elasticsearch
from hpcprocessor.controlhpc.loadconfig import get_hostcache
# Configuración de Elasticsearch
es = Elasticsearch(["http://"+get_hostcache()+":9200"])



def send(document, index_name):
    document_id = f"{document['group']}_{document['date']}"
    #print(document)
    es.index(index=index_name, id=document_id, document=document)


    

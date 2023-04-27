from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import base64

from config.configs import config

host = config.host
headers = {
    "Content-Type": "application/json",
    "Authorization": "Basic %s"
    % base64.b64encode(f"{config.username}:{config.password}".encode()).decode(),
}

class ElasticsearchLoader:
    def __init__(
        self, index_name, hosts=None, http_auth=(config.username, config.password)
    ):
        self.index_name = index_name
        self.hosts = hosts
        self.es = Elasticsearch(hosts=self.hosts, headers=headers)

    def load_data(self, df):
        actions = []
        for i, row in df.iterrows():
            actions.append({"_index": self.index_name, "_source": row.to_dict()})

        try:
            self.es.indices.create(index=self.index_name, ignore=400)
            bulk(self.es, actions)
            print("Data successfully loaded into Elasticsearch")
        except Exception as e:
            print("Error loading data into Elasticsearch")
            print(e)

    def query_elasticsearch(self):
        # Search for all documents in an index
        res = self.es.search(
            index="test_index", body={"query": {"match_all": {}}, "size": 100}
        )

        # Print the documents
        for hit in res["hits"]["hits"]:
            print(hit["_source"])

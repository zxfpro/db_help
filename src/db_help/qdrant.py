from qdrant_client import QdrantClient, models

def create_collection(collection_name = "diglife",
                      vector_dimension = 2560,
                      distance_metric = models.Distance.COSINE):
    try:
        client = QdrantClient(
            host="localhost",
            port=6333,
        )
        client.recreate_collection(
            collection_name=collection_name,
            vectors_config=models.VectorParams(
                size=vector_dimension, distance=distance_metric
            ),
        )
    except Exception as e:
        raise Exception("创建collection失败") from e
    finally:
        client.close()

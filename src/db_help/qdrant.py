
import importlib
import yaml
import qdrant_client
from qdrant_client import QdrantClient, models
from llama_index.core.postprocessor import SimilarityPostprocessor
from llama_index.vector_stores.qdrant import QdrantVectorStore
from llama_index.core import VectorStoreIndex
from llama_index.core import Document
from diglife.embedding_model import VolcanoEmbedding
from diglife.log import Log
from llama_index.core import PromptTemplate
import json
from llama_index.core.vector_stores import (
    MetadataFilter,
    MetadataFilters,
    FilterOperator,
)

logger = Log.logger
def load_config():
    """ load config """
    with importlib.resources.open_text('diglife', 'config.yaml') as f:
        return yaml.safe_load(f)

class EmbeddingPool():
    def __init__(self):
        self.reload()

    def create_collection(self,collection_name:str = "diglife",vector_dimension:str = 2560):
        distance_metric = models.Distance.COSINE # 使用余弦相似度

        # 2. 定义 Collection 参数
        config = load_config()
        
        client = qdrant_client.QdrantClient(
            host=config.get("host","localhost"),
            port=config.get("port",6333),
        )

        # 3. 创建 Collection (推荐使用 recreate_collection)
        try:
            client.recreate_collection(
                collection_name=collection_name,
                vectors_config=models.VectorParams(size=vector_dimension, distance=distance_metric),
            )
        except Exception as e:
            raise Exception("创建collection失败") from e
        finally:
            client.close()

    def reload(self):
        logger.info('reload')
        # 默认仓库要被创建的
        config = load_config()
        self.postprocess = SimilarityPostprocessor(similarity_cutoff=config.get("similarity_cutoff",0.5))
        client = qdrant_client.QdrantClient(
            host=config.get("host","localhost"),
            port=config.get("port",6333),
        )
        vector_store = QdrantVectorStore(client=client, collection_name=config.get("collection_name","diglife"))
        self.embed_model = VolcanoEmbedding(model_name = config.get("model_name","doubao-embedding-text-240715"),
                                            api_key =config.get("api_key",''))
        self.index = VectorStoreIndex.from_vector_store(vector_store,embed_model=self.embed_model)
        """
            # TODO add more operators
            EQ = "=="  # default operator (string, int, float)
            GT = ">"  # greater than (int, float)
            LT = "<"  # less than (int, float)
            NE = "!="  # not equal to (string, int, float)
            GTE = ">="  # greater than or equal to (int, float)
            LTE = "<="  # less than or equal to (int, float)
            IN = "in"  # In array (string or number)
            NIN = "nin"  # Not in array (string or number)
            ANY = "any"  # Contains any (array of strings)
            ALL = "all"  # Contains all (array of strings)
            TEXT_MATCH = "text_match"  # full text match (allows you to search for a specific substring, token or phrase within the text field)
            TEXT_MATCH_INSENSITIVE = (
                "text_match_insensitive"  # full text match (case insensitive)
            )
            CONTAINS = "contains"  # metadata array contains value (string or number)
            IS_EMPTY = "is_empty"  # the field is not exist or empty (null or empty array)

        """
        filters_bac = MetadataFilters(
            filters=[
                MetadataFilter(
                    key="type", value=1, operator=FilterOperator.LTE
                )  
            ]
        )
        
        self.retriver_bac = self.index.as_retriever(filters = filters_bac,
                                                    similarity_top_k=config.get("similarity_top_k",10))

        filters_figure_person = MetadataFilters(
            filters=[
                MetadataFilter(
                    key="type", value=2, operator=FilterOperator.EQ
                )  
            ]
        )

        self.retriver_figure_person = self.index.as_retriever(filters = filters_figure_person,
                                                              similarity_top_k=config.get("similarity_top_k",10))

        logger.debug("== reload start ==")
        logger.debug(self.postprocess)


    def update(self,text:str,id:str,type:int)->str:
        doc = Document(text = text,
                       id_=id,
                        metadata={"type":type,
                                  "id":id},
                        excluded_embed_metadata_keys=['type',"id"],
                    )
        self.index.update(document=doc)

    def delete(self,id:str):
        logger.info('delete')
        # 2025-09-18 10:37:32,238 - 警告 - llama_index.core.indices.base - base：310 - delete() 方法现已弃用，请使用 delete_ref_doc() 删除已导入的文档和节点，或使用 delete_nodes 删除节点列表。如需异步实现，请使用 delete_ref_docs() 。
        self.index.delete(doc_id = id)

    def search_bac(self,query:str)->str:
        results = self.retriver_bac.retrieve(query)
        output = []
        for i,result in enumerate(results):
            dct = result.node.metadata
            dct.update({"order":i})
            output.append(dct)

        return output

    def search_figure_person(self,query:str)->str:
        results = self.retriver_figure_person.retrieve(query)
        output = []
        for i,result in enumerate(results):
            dct = result.node.metadata
            dct.update({"order":i})
            output.append(dct)

        return output

import os
from openai import OpenAI


class Desensitization():
    def __init__(self):
        self.config = load_config()
        self.client = OpenAI(
            base_url='https://ark.cn-beijing.volces.com/api/v3',
            api_key=self.config.get("api_key",''),
        )
        self.model_name = self.config.get("chat_model_name",'')

    def _prompt_chat(self,prompt:str):
        response = self.client.chat.completions.create(
            model= self.model_name,
            messages=[
                        {
                            "role": "system",
                            "content": [
                                {"type": "text", "text": prompt },
                            ],
                        }
                    ],
        )
        result = response.choices[0].message.content
        return result
    
    def _postprocess(self,eva_result:str):
        if 'true' in eva_result:
            return True
        else:
            return False

    def desensitization(self,text):
        desensitization_prompt_ = self.config.get("Desensitization_prompt","")
        evaluation_prompt_ = self.config.get("Evaluation_prompt","")
        desensitization_prompt = PromptTemplate(desensitization_prompt_)
        evaluation_prompt = PromptTemplate(evaluation_prompt_)

        advice = ""
        for i in range(self.config.get("roll_time",3)):
            logger.info(i)
            des_result = self._prompt_chat(desensitization_prompt.format(text = text,advice = advice))
            print(des_result,'des_result')

            eva_result = self._prompt_chat(evaluation_prompt.format(des_result = des_result))
            print(eva_result,'eva_result')
            eva_result_json =  json.loads(eva_result)
            status = eva_result_json.get('status')
            review = eva_result_json.get("review")
            if status == "0":
                return "success", des_result
            elif status == "1":
                return "failed", "脱敏失败 " + review
            elif status == "2":
                advice = review
                text = des_result

        return "failed", "脱敏失败 执行异常"


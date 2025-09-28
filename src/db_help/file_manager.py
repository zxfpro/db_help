from datetime import datetime
from volcenginesdkarkruntime import Ark

import os
from dotenv import load_dotenv, find_dotenv
dotenv_path = find_dotenv()
load_dotenv(dotenv_path, override=True)

from db_help.mysql import MySQLManager
from db_help.qdrant import QdrantManager

def embedding_inputs(inputs:list[str],model_name = None):
    model_name = model_name or os.getenv("Ark_model_name")
    ark_client = Ark(api_key=os.getenv("Ark_api_key"))

    resp = ark_client.embeddings.create(
                model=model_name,
                input=inputs,
                encoding_format="float",
            )
    return [i.embedding for i in resp.data]

import zlib

def get_adler32_hash(s):
    return zlib.adler32(s.encode('utf-8'))


class ContentManager():
    def __init__(self):
        """
        """
        self.mysql = MySQLManager(
            host = os.environ.get("MySQL_DB_HOST"),
            user = os.environ.get("MySQL_DB_USER"),
            password = os.environ.get("MySQL_DB_PASSWORD"),
            database =  os.environ.get("MySQL_DB_NAME"),
        )
        self.qdrant = QdrantManager(host = "localhost")
        self.neo = None


    def update(self,text:str,name:str, version:str)->str:

        # 1 存入到数据库中
        table_name = "content"
        new_id = get_adler32_hash(f"{name}_{version}")

        vector_list = embedding_inputs([text])
        print(new_id,'new_id')
        self.mysql.insert(table_name, 
                                     {'new_id':new_id,
                                      'name': name, 
                                      'version': version, 
                                      'timestamp': datetime.now(),
                                      "content":text})

        # 2 存入到qdrant中
        self.qdrant.insert(table_name,
                           {"id": new_id,
                            "vector":vector_list[0],
                            "payload":{
                                "name": name, 
                                'version': version,
                                'timestamp': datetime.now(),
                                "content": text}
                            })
        # 考虑使用知识图 + 增加因果联系


    def search_by_id(self,target_name, target_version):
        """
        获取指定 prompt_id 和特定版本的数据。

        Args:
            target_name (str): 目标提示词的唯一标识符。
            target_version (int): 目标提示词的版本号。
            table_name (str): 存储提示词数据的数据库表名。
            db_manager (DBManager): 数据库管理器的实例，用于执行查询。

        Returns:
            dict or None: 如果找到，返回包含 id, prompt_id, version, timestamp, prompt 字段的字典；
                        否则返回 None。
        """

        table_name = "content"
        query = f"""
            SELECT new_id, name, version, timestamp, content
            FROM {table_name}
            WHERE prompt_id = %s AND version = %s
            LIMIT 1 -- 理论上 prompt_id 和 version 组合应该是唯一的，所以只取一个
        """
        
        # 组合参数，顺序与 query 中的 %s 对应
        params = (target_name, target_version)
        result = self.db_manager.execute_query(query, params=params, fetch_one=True)
        if result:
            print(f"找到 prompt_id '{target_name}', 版本 '{target_version}' 的提示词数据。")
        else:
            print(f"未找到 prompt_id '{target_name}', 版本 '{target_version}' 的提示词数据。")
        return result
    
    def search_by_name():
        #TODO 增加search_by_name
        pass

    def search_by_time():
        #TODO 增加ssearch_by_time
        pass

    def search_by_similarity():
        pass



    def search(self,query:str)->str:
        results = self.retriver.retrieve(query)
        results = self.postprocess.postprocess_nodes(results)

        for result in results:
            super_print(result.text,"result")
        return "success"

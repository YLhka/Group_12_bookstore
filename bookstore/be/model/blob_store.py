import logging
from pymongo import MongoClient
from pymongo.errors import PyMongoError
import os

class BlobStore:
    def __init__(self):
        self.client = None
        self.col = None
        try:
            # 默认连接本地 MongoDB，实际生产环境应从配置读取
            self.client = MongoClient(os.environ.get("MONGO_URL", "mongodb://localhost:27017"), serverSelectionTimeoutMS=2000)
            self.db = self.client["bookstore_blob"]
            self.col = self.db["book_content"]
        except Exception as e:
            logging.error(f"Failed to connect to Blob Store (MongoDB): {e}")

    def put_book_blob(self, book_id: str, content: str, book_intro: str, author_intro: str):
        """
        保存书籍的大文本数据到 MongoDB。如果失败，仅记录日志，不阻断主流程。
        """
        if self.col is None:
            return
        try:
            doc = {
                "book_id": book_id,
                "content": content,
                "book_intro": book_intro,
                "author_intro": author_intro
            }
            # 使用 upsert，如果已存在则更新
            self.col.update_one({"book_id": book_id}, {"$set": doc}, upsert=True)
        except PyMongoError as e:
            logging.error(f"Blob Store Put Error: {e}")

    def get_book_blob(self, book_id: str):
        """
        获取书籍的大文本数据。如果失败，返回空对象。
        """
        default_res = {"content": "", "book_intro": "", "author_intro": ""}
        if self.col is None:
            return default_res
            
        try:
            doc = self.col.find_one({"book_id": book_id}, {"_id": 0})
            if doc:
                return doc
            return default_res
        except PyMongoError as e:
            logging.error(f"Blob Store Get Error: {e}")
            return default_res

    def search_in_blob(self, keyword: str):
        """
        (Optional) 在 Blob 中搜索关键字，返回匹配的 book_id 列表
        """
        if self.col is None:
            return []
            
        try:
            # 建立索引（如果不存在），这对性能至关重要
            self.col.create_index([
                ("content", "text"), 
                ("book_intro", "text"), 
                ("author_intro", "text")
            ])
            
            cursor = self.col.find({"$text": {"$search": keyword}}, {"book_id": 1})
            return [doc["book_id"] for doc in cursor]
        except Exception as e:
            logging.error(f"Mongo Search Error: {e}")
            return []

blob_store_instance = BlobStore()

def get_blob_store():
    return blob_store_instance

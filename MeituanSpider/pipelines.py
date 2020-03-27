# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
from pymongo import MongoClient
from .items import MeishiCommentItem, MeishiInfoItem


class MeishiPipeline(object):
    """保存美食商家及评论信息"""

    def __init__(self, MONGODB_HOST, MONGODB_PORT, MONGODB_DB):
        self.client = MongoClient(MONGODB_HOST, MONGODB_PORT)
        self.db = self.client[MONGODB_DB]

    @classmethod
    def from_settings(cls, settings):
        MONGODB_HOST = settings['MONGODB_HOST']
        MONGODB_PORT = settings['MONGODB_PORT']
        MONGODB_DB = settings['MONGODB_DB']

        return cls(MONGODB_HOST, MONGODB_PORT, MONGODB_DB)

    def process_item(self, item, spider):
        print('进入管道文件')
        if isinstance(item, MeishiInfoItem):
            collection = self.db['meishi_info']
            if not collection.find_one({'poiId': item['poiId']}):  # 数据去重
                collection.insert_one(item)
                return item

        if isinstance(item, MeishiCommentItem):
            collection = self.db['meishi_comment']
            if not collection.find_one({'poiId': item['poiId'], 'publishDate': item['publishDate'], 'comment': item['comment']}):
                collection.insert_one(item)
                return item

    def close_spider(self, spider):
        print('关闭爬虫')
        self.client.close()




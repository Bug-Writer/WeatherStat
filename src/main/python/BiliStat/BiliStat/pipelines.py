# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from kafka import KafkaProducer
import json
import pymongo

class KafkaPipeline(Object):
    def open_spider(self, spider):
        self.producer = KafkaProducer(
            bootstrap_servers = ['localhost:9092'],
            value_serializer = lambda m:value.encode('utf-8')
        )

    def close_spider(self, spider):
        self.producer.close()

    def process_item(self, item, spider):
        message = json.dumps(dict(item))
        self.producer.send('videoInfo', message)
        return item

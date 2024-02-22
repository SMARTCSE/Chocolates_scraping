# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import pymongo
from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem


class ChocolatescrapperPipeline:
    def process_item(self, item, spider):
        return item

class PriceToRsPipeline:
    gbpToRsRate = 104.67

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        if adapter.get('price'):
            floatPrice = float(adapter['price'])

            adapter['price'] = floatPrice * self.gbpToRsRate
            return item
        else:
            raise DropItem(f"Missing price in {item}")

class DuplicatesPipeline:

    def __init__(self):
        self.name_Seen = set()

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        if adapter['name'] in self.name_Seen:
            raise DropItem(f"Duplicate name found: {item!r}")
        else:
            self.name_Seen.add(adapter['name'])
            return item
        
class ChocolatesPipeline(object):
    def __init__(self):
        self.conn = pymongo.MongoClient(
            'localhost',
            27017
        )
        db = self.conn['chocos']
        self.collection = db['mychoco_tb']

    def process_item(self, item, spider):
        self.collection.insert(dict(item))
        return item

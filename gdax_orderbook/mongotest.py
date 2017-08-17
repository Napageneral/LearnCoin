import pymongo
from order_book import OrderBook
import datetime, time
import json
from decimal import Decimal
import threading

class MongoOrderBook(OrderBook):
      
      def __init__(self, client, product_id='BTC-USD'):
         super(MongoOrderBook, self).__init__(product_id=product_id)
         self.dbname = "gdax"
         self.collection_name = ''.join(product_id.split('-'))#"BTCUSD" #datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
         self.mydb = client[self.dbname]
         self.collection = self.mydb[self.collection_name]
         print("init Mongoorderbook")
         
         
      def on_message(self, message):
         super(MongoOrderBook, self).on_message(message)         
            
      def remove(self, order):
         super(MongoOrderBook, self).remove(order)
         
         if order['reason'] == 'canceled':
               self.collection.insert_one({"type":"cancel",
                                       "price": float(order['price']),
                                       "size": -float(order.get('size') or order['remaining_size']),
                                       "side": order['side'],
                                       "time": order['time'],
                                    })
         
      def match(self, order):
         super(MongoOrderBook, self).match(order)
         self.collection.insert_one({"type":"match",
                                    "price": float(order['price']),
                                    "size": -float(order.get('size') or order['remaining_size']),
                                    "side": order['side'],
                                    "time": order['time'],
                               })
   
      def add(self, order):
         super(MongoOrderBook, self).add(order)
         
         if not 'time' in order:
            self.collection.insert_one({"type":"message",
                                    "price": float(order['price']),
                                    "size": float(order.get('size') or order['remaining_size']),
                                    "side": order['side'],
                                 })
         else:
            self.collection.insert_one({"type":"order",
                                       "price": float(order['price']),
                                       "size": float(order.get('size') or order['remaining_size']),
                                       "side": order['side'],
                                       "time": order['time'],
                                    })
    
def main():

   client = pymongo.MongoClient("mongodb://127.0.0.1:27017")	
   things = ['ETH-USD', 'BTC-USD', 'LTC-USD', 'LTC-EUR', 'BTC-EUR', 'ETH-EUR', 'BTC-GBP', 'ETH-BTC', 'LTC-BTC']
   threads = []
   for thing in things:
      ob = MongoOrderBook(client, product_id=thing)
      obt = threading.Thread(target=ob.start)
      threads.append(obt)
      obt.start()
   #time.sleep(100)
   #ob.close()

if __name__ == '__main__':
   main()
  
   
'''
{'sequence': 3610668509, 'price': '2041.91000000', 'time': '2017-07-17T11:13:25.977000Z', 'remaining_size': '0.50000000', 'side': 'buy', 'product_id': 'BTC-USD', 'type': 'open', 'order_id': 'ba8366c1-fc6c-41a4-9389-ab9672f130c3'}
'''
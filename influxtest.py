import influxdb
from order_book import OrderBook
import datetime, time
import json
from decimal import Decimal
      
def main():
   mydb = influxdb.InfluxDBClient()
   
   class InfluxOrderBook(OrderBook):
      
      def __init__(self, product_id=None):
         super(InfluxOrderBook, self).__init__(product_id=product_id)
         self.dbname = "gdax."+datetime.datetime.now().strftime('%Y-%m-%d %H-%M-%S')
         mydb.create_database(self.dbname)
         print("init influxorderbook")
         
         
      def on_message(self, message):
         super(InfluxOrderBook, self).on_message(message)         
            
      def remove(self, order):
         super(InfluxOrderBook, self).remove(order)
         
         if order['reason'] == 'canceled':
               mydb.write_points([{"measurement":"message",
                                   "fields":{
                                       "price": float(order['price']),
                                       "size": -float(order.get('size') or order['remaining_size']),
                                       "side": order['side'],
                                       "time": order['time']
                                    },
                                    }], database=self.dbname)
         
      def match(self, order):
         super(InfluxOrderBook, self).match(order)
         mydb.write_points([{"measurement":"message",
                                 "fields":{
                                    "price": float(order['price']),
                                    "size": -float(order.get('size') or order['remaining_size']),
                                    "side": order['side'],
                                    "time": order['time']
                                 },
                               }], database=self.dbname)
   
      def add(self, order):
         super(InfluxOrderBook, self).add(order)
         
         if not 'time' in order:
            mydb.write_points([{"measurement":"message",
                                 "fields":{
                                    "price": float(order['price']),
                                    "size": float(order.get('size') or order['remaining_size']),
                                    "side": order['side'],
                                    },
                                 }], database=self.dbname)
         else:
            mydb.write_points([{"measurement":"message",
                                    "fields":{
                                       "price": float(order['price']),
                                       "size": float(order.get('size') or order['remaining_size']),
                                       "side": order['side'],
                                       "time": order['time']
                                       },
                                    }], database=self.dbname)
         '''
         fields = {
            'side': order['side'],
            'size': float(order.get('size') or order['remaining_size']),
            'price': float(order['price'])
         }
         
         print("add: side:"+str(order['side']) + " size: " + str(float(order.get('size') or order['remaining_size'])) + " price: " + str(float(order['price'])))
         mydb.write_points([{"measurement":"add", "fields":fields}])
         '''
   ob = InfluxOrderBook()
   
   ob.start()
   #time.sleep(100)
   #ob.close()

if __name__ == '__main__':
   main()
  
   
'''
{'sequence': 3610668509, 'price': '2041.91000000', 'time': '2017-07-17T11:13:25.977000Z', 'remaining_size': '0.50000000', 'side': 'buy', 'product_id': 'BTC-USD', 'type': 'open', 'order_id': 'ba8366c1-fc6c-41a4-9389-ab9672f130c3'}
'''
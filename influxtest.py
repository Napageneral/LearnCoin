import influxdb
from order_book import OrderBook
import time
import json
from decimal import Decimal

'''
class MySeriesHelper(influxdb.SeriesHelper):
   class Meta:
      db = mydb
      series_name = 'events'
'''    
      
      
def main():
   mydb = influxdb.InfluxDBClient(database='new')
   
   class InfluxOrderBook(OrderBook):
      
      def __init__(self, product_id=None):
         super(InfluxOrderBook, self).__init__(product_id=product_id)
         
         print("init influxorderbook")

      def match(self, order):
         super(InfluxOrderBook, self).match(order)
         
         size = float(order['size'])
         price = float(order['price'])
         print("match: " + "size: " + str(size) + " price: " + str(price))
         mydb.write_points([{"measurement":"match","fields":{"size":size,"price":price}}])
   
      def add(self, order):
         super(InfluxOrderBook, self).add(order)
         
         fields = {
            'side': order['side'],
            'size': float(order.get('size') or order['remaining_size']),
            'price': float(order['price'])
         }
         
         print("add: side:"+str(order['side']) + " size: " + str(float(order.get('size') or order['remaining_size'])) + " price: " + str(float(order['price'])))
         mydb.write_points([{"measurement":"add", "fields":fields}])
      
   ob = InfluxOrderBook()
   
   ob.start()
   time.sleep(100)
   ob.close()

if __name__ == '__main__':
   main()
  
   
'''
{'sequence': 3610668509, 'price': '2041.91000000', 'time': '2017-07-17T11:13:25.977000Z', 'remaining_size': '0.50000000', 'side': 'buy', 'product_id': 'BTC-USD', 'type': 'open', 'order_id': 'ba8366c1-fc6c-41a4-9389-ab9672f130c3'}
'''
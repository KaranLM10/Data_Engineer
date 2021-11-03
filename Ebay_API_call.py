import os
import ebaysdk
import datetime
from ebaysdk.exception import ConnectionError
from ebaysdk.finding import Connection
from dotenv import load_dotenv
load_dotenv()
API_KEY = os.getenv('api_key')

class Ebay(object):
    def __init__(self,API_KEY):
        self.api_key = API_KEY

    def fetch(self):
        try:
            api = Connection(appid=self.api_key, config_file=None)
            response = api.execute('findItemsAdvanced', {'keywords': 'legos'})
            print(response.reply)
            assert (response.reply.ack == 'Success')
            assert (type(response.reply.timestamp) == datetime.datetime)
            assert (type(response.reply.searchResult.item) == list)

            item = response.reply.searchResult.item[0]
            assert (type(item.listingInfo.endTime) == datetime.datetime)
            assert (type(response.dict()) == dict)

        except ConnectionError as e:
            print(e)
            print(e.response.dict())
        pass

    def parse(self):
        pass


if __name__=='__main__':
    e = Ebay(API_KEY)
    e.fetch()
    e.parse()
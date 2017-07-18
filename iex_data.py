"""
This api will provide a python wrapper over IEX's native api for retrieving its data.

This api will allow you to:
- get latest quote and trade data
- get trade data only

"""

import pandas as pd
from urllib.request import Request, urlopen
import json
from pandas.io.json import json_normalize


class API(object):

    """
    This API class allows users to get different type of data from IEX via its methods.
    API class can be used for:
    - requesting valid securities
    - getting latest quote and trade data
    - getting latest trade data

    Examples:
        m = API()
        print(m.get_latest_trade_data(['AAPL', 'IBM']))
        print(m.get_latest_quote_and_trade_data(['AAPL', 'IBM']))
        print(m.return_valid_securities(['AAPL', 'IBM']))
    """

    def __init__(self):
        self._end_point_prefix = r'https://api.iextrading.com/1.0/'

    def return_valid_securities(self, securities):

        """
        Return a list of valid securities
        :param securities: list of securities
        :return: list of valid securities
        """

        suffix = r'ref-data/symbols'
        valid_securities = self._url_to_dataframe(self._end_point_prefix+suffix)['symbol']

        return [x for x in securities if x in set(valid_securities)]

    def _url_to_dataframe(self, url):

        """
        Takes a url and returns the response in a pandas dataframe
        :param url: str url
        :return: pandas dataframe containing data from url
        """

        request = Request(url)
        response = urlopen(request)
        elevations = response.read()
        data = json.loads(elevations)

        return pd.DataFrame(json_normalize(data))

    def get_latest_quote_and_trade(self, securities):

        """
        Gets latest quote and trade data
        :param securities: list of securities
        :return: pandas dataframe containing data for valid securities
        """

        securities = self.return_valid_securities(securities)

        if securities:
            suffix = r'tops?symbols='
            symbols = ','.join(securities)
            df = self._url_to_dataframe(self._end_point_prefix + suffix + symbols)
            df['lastSaleTime'] = pd.to_datetime(df['lastSaleTime'], unit='ms')
            df['lastUpdated'] = pd.to_datetime(df['lastUpdated'], unit='ms')
            df.set_index(['symbol'], inplace=True)
            return df
        else:
            print('These stock(s) are invalid!')

    def get_latest_trade(self, securities):

        """
        Gets latest trade data
        :param securities: list of securities
        :return: pandas dataframe containing data for valid securities
        """

        securities = self.return_valid_securities(securities)

        if securities:
            suffix = r'tops/last?symbols='
            symbols = ','.join(securities)
            df = self._url_to_dataframe(self._end_point_prefix + suffix + symbols)
            df['time'] = pd.to_datetime(df['time'], unit='ms')
            df.set_index(['symbol'], inplace=True)
            return df
        else:
            print('These stock(s) are invalid!')
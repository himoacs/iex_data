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

    def _url_to_dataframe(self, url, nest=None):

        """
        Takes a url and returns the response in a pandas dataframe
        :param url: str url
        :param nest: column with nested data
        :return: pandas dataframe containing data from url
        """

        request = Request(url)
        response = urlopen(request)
        elevations = response.read()
        data = json.loads(elevations)

        if nest:
            data = json_normalize(data[nest])
        else:
            data = json_normalize(data)

        return pd.DataFrame(data)

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

    def get_latest_news(self, securities, count=1):

        """
        Get latest news for securities. By default, gets one news item per security.

        :param securities: list of securities
        :param count: how many news items to return
        :return: dataframe
        """

        securities = self.return_valid_securities(securities)

        final_df = pd.DataFrame({})

        # Get news for each security and then append the results together
        if securities:
            for symbol in securities:
                suffix = r'stock/{symbol}/news/last/{count}'.format(symbol=symbol,
                                                                    count=count)
                df = self._url_to_dataframe(self._end_point_prefix + suffix)
                df['time'] = pd.to_datetime(df['datetime'])
                del df['datetime']
                df['symbol'] = symbol
                df = df[['symbol', 'time', 'headline', 'summary', 'source', 'url', 'related']]
                final_df = final_df.append(df, ignore_index=True)
            return final_df
        else:
            print('These stock(s) are invalid!')

    def get_financials(self, securities):

        """
        Get latest financials of securities. By default, gets one news item per security.
        :param securities: list of symbols
        :return: dataframe
        """

        securities = self.return_valid_securities(securities)

        final_df = pd.DataFrame({})

        # Get financials of each security and then append the results together
        if securities:
            for symbol in securities:
                suffix = r'stock/{symbol}/financials'.format(symbol=symbol)
                df = self._url_to_dataframe(self._end_point_prefix + suffix, 'financials')
                df['symbol'] = symbol
                final_df = final_df.append(df, ignore_index=True)
            return final_df
        else:
            print('These stock(s) are invalid!')

    def get_earnings(self, securities):

        """
        Get latest earnings for securities.
        :param securities: list of symbols
        :return: dataframe
        """

        securities = self.return_valid_securities(securities)

        final_df = pd.DataFrame({})

        # Get earnings data for each security and then append the results together
        if securities:
            for symbol in securities:
                suffix = r'stock/{symbol}/earnings'.format(symbol=symbol)
                df = self._url_to_dataframe(self._end_point_prefix + suffix, 'earnings')
                df['symbol'] = symbol
                final_df = final_df.append(df, ignore_index=True)
            return final_df
        else:
            print('These stock(s) are invalid!')

    def get_trade_bars_data(self, securities, bucket='1m'):

        """
        Get bucketed trade/volume data. Supported buckets are: 1m, 3m, 6m, 1y, ytd, 2y, 5y

        :param securities: list of securities
        :param bucket:
        :return: dataframe
        """

        securities = self.return_valid_securities(securities)

        final_df = pd.DataFrame({})

        # Get earnings data for each security and then append the results together
        if securities:
            for symbol in securities:
                suffix = r'stock/{symbol}/chart/{bucket}'.format(symbol=symbol,
                                                                   bucket=bucket)
                df = self._url_to_dataframe(self._end_point_prefix + suffix)
                df['symbol'] = symbol
                final_df = final_df.append(df, ignore_index=True)
            return final_df
        else:
            print('These stock(s) are invalid!')
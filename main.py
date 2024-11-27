import os
import pandas as pd
from zoneinfo import ZoneInfo
from typing import Final
from scripts.CleanData import *

# Config Constants
DATADIR: Final[str] = os.path.join('.','data')
RAWDATADIR: Final[str] = os.path.join(DATADIR, 'raw')
INTERMEDIATEDATADIR: Final[str] = os.path.join(DATADIR, 'intermediate')
PROCESSEDDATADIR: Final[str] = os.path.join(DATADIR, 'processed')

EXTRACTNAME: Final[str] = '20517797 16NOV2024.csv'
TIMEZONE: Final[ZoneInfo] = ZoneInfo('Pacific/Auckland')

COLNAMES: Final[pd.Series] = pd.Series(['EventDT', 'Type', 'OrderId', 'TradeId', \
            'RelOrderId', 'Product', 'Units/Amt', 'Price', \
            'BoundaryPrice', 'StopLoss', 'TakeProfit', \
            'ConversionRate', 'ValueNZD', 'AmountNZD', \
            'BalanceNZD', 'AmountExcludingFee', 'Fee', \
            'HoldingCostAmt', 'HoldingRate', 'HoldingCostNZD', \
            'HoldingCostTotalNZD', 'Premium', 'RefundPercentage'],
            dtype=pd.StringDtype())

COLDATATYPES: Final[dict] = {
     'EventDT' : pd.DatetimeTZDtype(tz=TIMEZONE),
     'Type' : 'string',
     'OrderId' : 'string',
     'TradeId' : 'string',
     'RelOrderId' : 'string',
     'Product' : 'string',
     'Units/Amt' : 'string',
     'Price' : pd.Float64Dtype(),
     'BoundaryPrice' : 'string',
     'StopLoss' : 'string',
     'TakeProfit' : 'string',
     'ConversionRate' : 'string',
     'ValueNZD' : 'string',
     'AmountNZD' : 'string',
     'BalanceNZD' : 'string',
     'AmountExcludingFee' : 'string',
     'Fee' : 'string',
     'HoldingCostAmt' : 'string',
     'HoldingRate' : 'string',
     'HoldingCostNZD' : 'string',
     'HoldingCostTotalNZD' : 'string',
     'Premium' : 'string',
     'RefundPercentage' : 'string'
}


RAWDATA: Final[pd.DataFrame] = pd.read_csv(os.path.join(RAWDATADIR,EXTRACTNAME))

# Code Begins Here
CMCCleanedData: pd.DataFrame = RAWDATA

CMCCleanedData = CMCCleanedData \
     .replace('-', pd.NA) \
     .pipe(RemoveAllEmptyCols) \
     .astype(pd.StringDtype())

CMCCleanedData.columns = COLNAMES

TransactionGroupingDf = GetRelatedTransactions(CMCCleanedData, \
                                               'OrderId', 'RelOrderId', 'TradeId')

CMCCleanedData = pd.merge(TransactionGroupingDf, CMCCleanedData, how='left')

CMCCleanedData['Contains (B) Tag'] = CMCCleanedData['Price'].str.contains(r'^\(B\)')
CMCCleanedData['Price'] = CMCCleanedData['Price'].replace('(B) -', pd.NA)
CMCCleanedData['Price'] = CMCCleanedData['Price'].str.replace('(B)', '')
CMCCleanedData['Price'] = CMCCleanedData['Price'].str.replace(',', '')
CMCCleanedData['Price'] = CMCCleanedData['Price'].str.strip()

CMCCleanedData = CMCCleanedData.astype(COLDATATYPES)
 
for TransactionId in CMCCleanedData['TransactionId'].unique():
    WriteTransactionsToCsv(os.path.join(INTERMEDIATEDATADIR, 'Transactions'), CMCCleanedData, TransactionId)
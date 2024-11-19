import os
import pandas as pd
from zoneinfo import ZoneInfo
from typing import Final
from scripts.StageData import *

# Config Constants
DATADIR: Final[str] = os.path.join('.','data')
RAWDATADIR: Final[str] = os.path.join(DATADIR, 'raw')
PROCESSEDDATADIR: Final[str] = os.path.join(DATADIR, 'processed')
CLEANEDDATADIR: Final[str] = os.path.join(DATADIR, 'cleaned')

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
     'Price' : 'string',
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
CMCStagedData: pd.DataFrame = RAWDATA

CMCStagedData = CMCStagedData.replace('-', pd.NA)
CMCStagedData = RemoveAllEmptyCols(CMCStagedData)

CMCStagedData.columns = COLNAMES
CMCStagedData = CMCStagedData.astype(pd.StringDtype())

# TO DO: Create a grouping for related orders
# TransactionGroupingDf = GetRelatedTransactions(CMCStagedData)

CMCStagedData.SetDataTypes(COLDATATYPES)
 
# for TransactionId in TransactionGroupingDf['TransactionId']:
#     WriteTransactionsToCsv(TransactionId)
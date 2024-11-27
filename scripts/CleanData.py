import os
import pandas as pd
from collections import deque

def RemoveAllEmptyCols(Df) -> pd.DataFrame:
   ColsToRemove = []

   for i in range(Df.shape[1]):
      IsAllNA = sum(map(lambda x: not x, pd.isna(Df.iloc[:, i]))) == 0
      if IsAllNA:
         ColsToRemove += [Df.columns[i]]

   Df = Df.drop(ColsToRemove, axis=1)

   return Df

def GetRelatedTransactions(Df: pd.DataFrame, OrderId: str, \
                           RelOrderId: str, TradeId: str) -> pd.DataFrame:
   GroupedDf = Df.loc[:, [OrderId, RelOrderId, TradeId]] \
      .drop_duplicates(subset=[OrderId, RelOrderId, TradeId]) \
      .reset_index(drop=True)
   
   GroupedDf['TransactionId'] = GroupedDf.groupby([OrderId]).ngroup(ascending=False)
   GroupedDf['TransactionId'] = GroupedDf['TransactionId'].astype(pd.StringDtype())
   GroupedDf['TransactionId'] = pd.Series(map(lambda x: 'T' + x.zfill(7), GroupedDf['TransactionId']))

   GroupedDf = GroupedDf.loc[:, ['TransactionId', OrderId, RelOrderId, TradeId]]

   return GroupedDf

   
def WriteTransactionsToCsv(OutputDir: str, Df: pd.DataFrame, TransactionId: str):
   Df.loc[TransactionId==Df['TransactionId'], :].to_csv(os.path.join(OutputDir, 'Transactions', TransactionId + '.csv'))
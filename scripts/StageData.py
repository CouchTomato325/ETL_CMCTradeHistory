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

def GetRelatedTransactions(Df: pd.DataFrame) -> pd.DataFrame:
   # TO DO: Write Code to logically group together transactions
   pass

   
def WriteTransactionsToCsv(Df: pd.DataFrame, TransactionId: str):
   # TO DO: Write Code to write out those grouped transactions (in their respective groups) to analyse further
   pass
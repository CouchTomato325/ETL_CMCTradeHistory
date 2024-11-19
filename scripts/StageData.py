import os
import pandas as pd
from collections import deque

def RemoveAllEmptyCols(Df) -> pd.Series:
   ColsToRemove = []

   for i in range(Df.shape[1]):
      if sum(pd.isna(Df.iloc[:, i])) == Df.shape[0]:
         ColsToRemove += [Df.columns[i]]

   Df = Df.drop(ColsToRemove, axis=1)

   return pd.Series(ColsToRemove, dtype=pd.StringDtype())

def GetRelatedTransactions(Df: pd.DataFrame) -> pd.DataFrame:
   # TO DO: Write Code to logically group together transactions
   pass

   
def WriteTransactionsToCsv(Df: pd.DataFrame, TransactionId: str):
   # TO DO: Write Code to write out those grouped transactions (in their respective groups) to analyse further
   pass
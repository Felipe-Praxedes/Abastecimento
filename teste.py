from ntpath import join
import pandas as pd
import numpy as np

df1 = pd.DataFrame([['a', 1], ['b', 2]],
                   columns=['letter', 'number'])

df2 = pd.DataFrame([['b', 3], ['d', 4]],
                   columns=['letter', 'number_s'])

df3 = pd.merge(df1, df2, how='cross')
df4 = pd.merge(df1, df2, how='inner', on='letter')

print(df4)


import pandas as pd
import numpy as np

data = {'Name': ['Ankit', 'Amit',
                 'Aishwarya', 'Priyanka'],
        'Stream': ['Math', 'Commerce',
                   'Arts', 'Biology'],
        'Percentage': [88, 92, 95, 70]}
  
# Convert the dictionary into DataFrame
df = pd.DataFrame(data, columns=['Name', 'Stream', 'Percentage'])

print(df)
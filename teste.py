import pandas as pd
from datetime import date, datetime

raw_data = {'name': ['Willard Morris', 'Al Jennings', 'Omar Mullins', 'Spencer McDaniel'],
'favorite_color': ['blue', 'red', 'yellow', "green"],
'grade': [88, 92, 95, 70],
'data': ['01.08.2022', '09.08.2022', '10.08.2022', '24.08.2022']}
df = pd.DataFrame(raw_data, index = ['Willard Morris', 'Al Jennings', 'Omar Mullins', 'Spencer McDaniel'])

df['dias'] =  (pd.to_datetime(df['data'], format="%d.%m.%Y") - pd.to_datetime(date.today())).dt.days

print(df.dtypes, df)


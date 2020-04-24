import pandas as pd
data = pd.read.csv("D:\Drivers\github\Big-Data-Programming\Module2\ICP3-pyspark\survey.csv")
data.dropna(inplace = True)
df2 = df2.str.split("t", n = 1,)
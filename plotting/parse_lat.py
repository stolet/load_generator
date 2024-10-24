import pandas as pd

df = pd.read_csv('output.dat', delimiter='\t', engine='python')

median = df.iloc[:,0].quantile(0.5)
tail_99 = df.iloc[:,0].quantile(0.99)
tail_999 = df.iloc[:,0].quantile(0.999)

print("50p={}".format(median))
print("99p={}".format(tail_99))
print("99.9p={}".format(tail_999))

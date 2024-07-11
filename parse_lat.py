import pandas as pd

df = pd.read_csv('output.dat', delimiter='\t', engine='python')

median = df.iloc[:,0].quantile(0.5)
tail = df.iloc[:,0].quantile(0.999)

print("50p={}".format(median))
print("99.9p={}".format(tail))

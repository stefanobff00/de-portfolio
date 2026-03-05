import pandas as pd

df = pd.read_csv("data/2019.csv")

print(f"Shape: {df.shape}")
print(f"\nDtypes:\n{df.dtypes}")
print(f"\nFirst 5 lines:\n{df.head }")
print(f"\nNull values:\n{df.isnull().sum()}")


print(f"\nMy test output:\n{df.abs }")
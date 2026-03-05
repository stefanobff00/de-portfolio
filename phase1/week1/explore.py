import pandas as pd

#import data
df = pd.read_csv("data/2019.csv")
"""
# Basic exploration
print(f"Shape: {df.shape}")
print(f"\nDtypes:\n{df.dtypes}")
print(f"\nNull values:\n{df.isnull().sum()}")

# Top 10 happiest countries
print("\nTop 10 happiest countries:")
print(df.nlargest(10, "Score")[["Country or region", "Score"]])

# GDP vs happiness correlation (Pearson)
print("\nGDP vs Score correlation:")
print(df[["GDP per capita", "Score"]].corr())

# Top 20 vs Bottom 20 comparison
top20 = df.head(20)
bottom20 = df.tail(20)
cols = ["GDP per capita", "Social support", "Healthy life expectancy"]

comparison = pd.DataFrame({
    "top20": top20[cols].mean(),
    "bottom20": bottom20[cols].mean()
})
comparison["differenza"] = comparison["top20"] - comparison["bottom20"]
print("\nTop 20 vs Bottom 20:")
print(comparison.round(3))
"""
print(f"My print test \n {(df[["Score"]].sum()).round()}")
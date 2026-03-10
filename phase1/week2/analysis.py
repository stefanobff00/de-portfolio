import pandas as pd

df = pd.read_csv("data/2019.csv")

# 1. Rename columns

df.columns = [
    "rank", "country", "score", "gdp",
    "social", "health", "freedom", "generosity", "corruption"
]


# 2. define function to calculate score 

def happiness_tier(input_score):
    if input_score >= 7:
        return pd.Series(["high", 1], index=["tier", "tierId"])
    elif input_score >= 5:
        return pd.Series(["medium", 2], index=["tier", "tierId"])
    else:
        return pd.Series(["low", 3], index=["tier", "tierId"])


# 3. Apply function (add 2 columns) 

df[["tier", "tierId"]] = df["score"].apply(happiness_tier)


# 4. Count how many countries in each tier
'''
print("Country per tier:")
print(df.groupby(["tier", "tierId"])["country"].count().reset_index().sort_values("tierId"))
'''

# Create new Dataframe (All the arrays of the same lenght)

tier_info= pd.DataFrame({
    "tierId":[1, 2, 3],
    "description": ["Very happy", "Moderately happy", "Unhappy"],
    "target_gdp": [1.2, 0.8, 0.4]
})

df_merged = df.merge(tier_info, on= "tierId", how="left") 
print(df_merged[["country", "score", "tier", "tierId", "description"]].head(10))

'''
# Merge con il DataFrame principale
df_merged = df.merge(tier_info, on="tier", how="left")
print(df_merged[["country", "score", "tier", "description"]].head(10))

'''

import duckdb

# Create temporary database 

conn = duckdb.connect()

# Load data
conn.execute('''
    CREATE TABLE countries AS 
    SELECT * FROM read_csv_auto('phase1/week1/data/2019.csv')
''')

# 1. ROW_NUMBER — rank countries within each tier
result = conn.execute("""
    SELECT 
        "Country or region",
        Score,
        "GDP per capita",
        ROW_NUMBER() OVER (ORDER BY Score DESC) as global_rank,
        CASE 
            WHEN Score >= 7 THEN 'high'
            WHEN Score >= 5 THEN 'medium'
            ELSE 'low'
        END as tier
    FROM countries
    ORDER BY Score DESC
    LIMIT 20
""").df()

#print("ROW_NUMBER example:")
#print(result)


result2 = conn.execute("""
    SELECT
        "Country or region",
        Score,
        CASE 
            WHEN Score >= 7 THEN 'high'
            WHEN Score >= 5 THEN 'medium'
            ELSE 'low'
        END as tier,
        ROW_NUMBER() OVER (ORDER BY Score DESC) as global_rank,
        ROW_NUMBER() OVER (PARTITION BY 
            CASE 
                WHEN Score >= 7 THEN 'high'
                WHEN Score >= 5 THEN 'medium'
                ELSE 'low'
            END 
        ORDER BY Score DESC) as rank_within_tier
    FROM countries
    ORDER BY tier, rank_within_tier
LIMIT 20
                       """).df()

#print(result2)


result3 = conn.execute("""
    SELECT 
        "Country or region",
        Score,
        ROW_NUMBER() OVER (ORDER BY Score DESC) as rank,
        LAG(Score) OVER (ORDER BY Score DESC) as prev_score,
        Score - LAG(Score) OVER (ORDER BY Score DESC) as gap_from_prev
    FROM countries
    ORDER BY Score DESC
    LIMIT 20
""").df()

#print(result3)

result4 = conn.execute("""
    SELECT 
        "Country or region",
        Score,
        ROW_NUMBER() OVER (ORDER BY Score DESC) as rank,
        LEAD(Score) OVER (ORDER BY Score DESC) as next_score,
        Score - LEAD(Score) OVER (ORDER BY Score DESC) as gap_from_next
    FROM countries
    ORDER BY Score DESC
    LIMIT 20
""").df()

#print(result4)

result5 = conn.execute("""
    SELECT 
        "Country or region",
        Score,
        CASE 
            WHEN Score >= 7 THEN 'high'
            WHEN Score >= 5 THEN 'medium'
            ELSE 'low'
        END as tier,
        ROUND(AVG(Score) OVER (
            PARTITION BY CASE 
                WHEN Score >= 7 THEN 'high'
                WHEN Score >= 5 THEN 'medium'
                ELSE 'low'
            END
        ), 3) as avg_score_in_tier,
        ROUND(Score - AVG(Score) OVER (
            PARTITION BY CASE 
                WHEN Score >= 7 THEN 'high'
                WHEN Score >= 5 THEN 'medium'
                ELSE 'low'
            END
        ), 3) as diff_from_tier_avg
    FROM countries
    ORDER BY Score DESC
    LIMIT 20
""").df()

#print(result5)


result6 = conn.execute("""
    SELECT 
        CASE 
            WHEN Score >= 7 THEN 'high'
            WHEN Score >= 5 THEN 'medium'
            ELSE 'low'
        END as tier,
        ROUND(AVG(Score), 3) as avg_score,
        ROUND(MIN(Score), 3) as min_score,
        ROUND(MAX(Score), 3) as max_score,
        ROUND(MAX(Score) - MIN(Score), 3) as range_score,
        ROUND(STDDEV(Score), 3) as std_score
    FROM countries
    GROUP BY tier
    ORDER BY avg_score DESC
""").df()

#print(result6)

# CTE, temporary subquery, increase readability

result7 = conn.execute("""
    WITH tier_stats AS (
        SELECT 
            CASE 
                WHEN Score >= 7 THEN 'high'
                WHEN Score >= 5 THEN 'medium'
                ELSE 'low'
            END as tier,
            ROUND(AVG(Score), 3) as avg_score,
            ROUND(STDDEV(Score), 3) as std_score
        FROM countries
        GROUP BY tier
    ),
    ranked_countries AS (
        SELECT 
            "Country or region",
            Score,
            CASE 
                WHEN Score >= 7 THEN 'high'
                WHEN Score >= 5 THEN 'medium'
                ELSE 'low'
            END as tier,
            ROW_NUMBER() OVER (
                PARTITION BY CASE 
                    WHEN Score >= 7 THEN 'high'
                    WHEN Score >= 5 THEN 'medium'
                    ELSE 'low'
                END
                ORDER BY Score DESC
            ) as rank_in_tier
        FROM countries
    )
    SELECT 
        r."Country or region",
        r.Score,
        r.tier,
        r.rank_in_tier,
        t.avg_score as tier_avg,
        t.std_score as tier_std
    FROM ranked_countries r
    JOIN tier_stats t ON r.tier = t.tier
    ORDER BY r.tier, r.rank_in_tier
    LIMIT 20
""").df()

#print(result7)

#query optimization
# EXPLAIN — see how DuckDB executes the query
plan = conn.execute("""
    EXPLAIN 
    SELECT 
        "Country or region",
        Score
    FROM countries
    WHERE Score > 6
    ORDER BY Score DESC
""").df()

#print("Query plan:")
#print(plan["explain_value"].iloc[0])

#We can encounter performance issue in the first step of the query: SEQ-SCAN, to find only x records which satisfies where condition
# Create index on Score
conn.execute("CREATE INDEX idx_score ON countries(Score)")

# Run EXPLAIN again
plan2 = conn.execute("""
    EXPLAIN 
    SELECT "Country or region", Score
    FROM countries
    WHERE Score > 6
    ORDER BY Score DESC
""").df()

print(plan2["explain_value"].iloc[0])

#dataset too small to notice improvements using INDEX (only 150 records approx.)
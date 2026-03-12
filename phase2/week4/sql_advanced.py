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

print(result6)
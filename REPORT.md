# Hands-on L9: Report

**Name: Arham Hussain Inamdar**

**Student ID: 801404110**

**Email: ainamda3@charlotte.edu**

---

## Seed and commands

Seed used for `datagen.py`: 801404110

The commands you ran, in order. If you deviated from the steps in the README, say where and
why.

```bash
python3 datagen.py 801404110

docker compose -f docker-compose.codespaces.yml up -d

docker cp datagen.py spark-master:/opt/spark/work-dir/shared/
docker exec spark-master python3 /opt/spark/work-dir/shared/datagen.py 801404110 \
  /opt/spark/work-dir/shared/input

docker cp main.py spark-master:/opt/spark/work-dir/

docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/work-dir/main.py \
  /opt/spark/work-dir/shared/input \
  /opt/spark/work-dir/shared/output

docker compose down
```

---

## Results

For each task, your output and one or two sentences on what it says about your data.

### Task 1: hashtag trends (DataFrame API)

```
hashtag,count
"#mood",1403
"#social",1355
"#update",1294
"#bug",890
"#fail",877
"#darkmode",868
"#ux",864
"#tech",824
"#cleanui",540
"#design",515
```

How many distinct spellings did your most frequent hashtag have before `lower` and `trim`?
The most frequent hashtag is `#mood`, with 1,403 uses after normalization. It appeared in 2 distinct spellings before normalization: `#mood` (1,196) and `#MOOD` (207).

### Task 2: engagement by age group (Spark SQL)

```
age_group,posts,avg_likes,avg_retweets
Adult,1206,56.7,14.37
Teen,1827,55.48,13.9
Senior,1967,54.38,13.61
```

Adults had the highest average engagement, with 56.70 likes and 14.37 retweets per post. Teens contributed the most posts (1,827), while Seniors had the lowest average likes and retweets.

### Task 3: sentiment versus engagement (Spark SQL)

```
sentiment,posts,avg_likes,avg_retweets
Negative,1005,24.62,6.61
Neutral,2270,50.53,12.85
Positive,1725,79.58,19.53
```

Does sentiment pay on your data? Say it with the numbers.

Yes, sentiment is associated with higher engagement in this dataset: positive posts averaged 79.58 likes and 19.53 retweets, compared with 50.53 likes and 12.85 retweets for neutral posts, and 24.62 likes and 6.61 retweets for negative posts.

### Task 4: top verified users (DataFrame API)

```
username,total_likes,total_retweets,reach
@night_coder48,20167,4858,25025
@pixel_pusher1,19544,4826,24370
@techie17,18455,4866,23321
@designer49,18491,4692,23183
@calm_mind41,16109,4245,20354
```

Among these verified users, `@night_coder48` had the greatest reach at 25,025, from 20,167 likes and 4,858 retweets.

---

## Two languages, one plan

Paste the two `explain()` outputs of task 2, the SQL version and the DataFrame version:

SQL version:

```
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
 +- Sort [avg_likes#54 DESC NULLS LAST], true, 0
   +- Exchange rangepartitioning(avg_likes#54 DESC NULLS LAST, 200), ENSURE_REQUIREMENTS, [plan_id=743]
     +- HashAggregate(keys=[age_group#10], functions=[count(1), avg(likes#4), avg(retweets#5)])
       +- Exchange hashpartitioning(age_group#10, 200), ENSURE_REQUIREMENTS, [plan_id=740]
         +- HashAggregate(keys=[age_group#10], functions=[partial_count(1), partial_avg(likes#4), partial_avg(retweets#5)])
           +- Project [likes#4, retweets#5, age_group#10]
             +- BroadcastHashJoin [user_id#1], [user_id#8], Inner, BuildRight, false, false
               :- Filter isnotnull(user_id#1)
               :  +- FileScan csv [user_id#1,likes#4,retweets#5] Batched: false, DataFilters: [isnotnull(user_id#1)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/opt/spark/work-dir/shared/input/posts.csv], PartitionFilters: [], PushedFilters: [IsNotNull(user_id)], ReadSchema: struct<user_id:string,likes:int,retweets:int>
               +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, string, false]),false), [plan_id=735]
                 +- Filter isnotnull(user_id#8)
                   +- FileScan csv [user_id#8,age_group#10] Batched: false, DataFilters: [isnotnull(user_id#8)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/opt/spark/work-dir/shared/input/users.csv], PartitionFilters: [], PushedFilters: [IsNotNull(user_id)], ReadSchema: struct<user_id:string,age_group:string>
```

DataFrame version:

```
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
 +- Sort [avg_likes#134 DESC NULLS LAST], true, 0
   +- Exchange rangepartitioning(avg_likes#134 DESC NULLS LAST, 200), ENSURE_REQUIREMENTS, [plan_id=793]
     +- HashAggregate(keys=[age_group#10], functions=[count(1), avg(likes#4), avg(retweets#5)])
       +- Exchange hashpartitioning(age_group#10, 200), ENSURE_REQUIREMENTS, [plan_id=790]
         +- HashAggregate(keys=[age_group#10], functions=[partial_count(1), partial_avg(likes#4), partial_avg(retweets#5)])
           +- Project [likes#4, retweets#5, age_group#10]
             +- BroadcastHashJoin [user_id#1], [user_id#8], Inner, BuildRight, false, false
               :- Filter isnotnull(user_id#1)
               :  +- FileScan csv [user_id#1,likes#4,retweets#5] Batched: false, DataFilters: [isnotnull(user_id#1)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/opt/spark/work-dir/shared/input/posts.csv], PartitionFilters: [], PushedFilters: [IsNotNull(user_id)], ReadSchema: struct<user_id:string,likes:int,retweets:int>
               +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, string, false]),false), [plan_id=785]
                 +- Filter isnotnull(user_id#8)
                   +- FileScan csv [user_id#8,age_group#10] Batched: false, DataFilters: [isnotnull(user_id#8)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/opt/spark/work-dir/shared/input/users.csv], PartitionFilters: [], PushedFilters: [IsNotNull(user_id)], ReadSchema: struct<user_id:string,age_group:string>
```

Your comparison: are the two plans the same? Which operator is the join and which kind of
join did Spark choose, where are the `Exchange` operators and why are they needed, and what
does this tell you about choosing between SQL and the DataFrame API?


The plans have the same structure; only generated expression and plan IDs differ. Spark chose an inner `BroadcastHashJoin`, building the right side by broadcasting the smaller `users` table. The `Exchange hashpartitioning(age_group, 200)` redistributes rows so Spark can aggregate each age group together. The `Exchange rangepartitioning(avg_likes DESC NULLS LAST, 200)` supports the requested sort; the `BroadcastExchange` distributes the `users` side for the join. SQL and the DataFrame API produce equivalent physical plans for this query, so either API can express the same computation.



---

## Views

What did `createOrReplaceTempView("posts")` do, and what did it not do? Was any data copied?
What happens to the views when the program ends, and how would you keep a table for the
next session?

`createOrReplaceTempView("posts")` registered the DataFrame's query plan under the name `posts` for SQL queries in the current Spark session. It did not copy the data. The temporary view disappears when the Spark session ends. To keep a table for later sessions, I could save it as a persistent table with `saveAsTable` in a configured catalog/metastore.



---

## Problems and fixes

Anything that went wrong and what resolved it. Paste the actual error message. If nothing
went wrong, say so.

No errors occurred

## AI Use Statement
I used ChatGPT to help me refine my responses for the report
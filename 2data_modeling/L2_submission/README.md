# 1. Discuss the purpose of this database in the context of the startup, Sparkify, and their analytical goals.

- This Star Schema will primarily help Sparkify analyze activity related to their song playback data. They will be able to quickly get basic stats about their playback events, as well as answer more in depth analytical questions by joining and grouping by supplemental dimension tables (artists, songs, time, users).

# 2. State and justify your database schema design and ETL pipeline.

- The Star Schema proposed provides Sparkify with good read performance to a valuable asset in their company - i.e. playback events. The data model follows a traditional fact-dimension table approach where all tables are easily joinable via respective ID columns. This provides a good blend/trade-off of human readable tables for fast ad hoc queries related to playback events and reasonable database efficiency (in terms of write performance and storage requirements). 
- The ETL process converts raw .json files into the proper form to be inserted into the relevant tables. This process is optimized for data integrity over speed/clock performance, to minimize potential ad hoc querying/analysis issues. All entries must conform to the specified schema with its inherent data types and contraints.

# 3. [Optional] Provide example queries and results for song play analysis.

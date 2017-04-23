## PySparkPageRank

This application requires Antlr4, Spark, the MariaDB JDBC connector, Python and an assortment of other libraries and compilers. `sqlInsert.[py|g4]` is an Antlr4 based context-free grammar, parser, and processor to generate a sparse matrix of the internal pagelinks in Wikipedia from the raw SQL dump and pull page id's from a MariaDB instance containing the `pages` table. `SparkPageRank.py` is a Spark implementation of PageRank which takes the output of the above processor and generates weights for each article page in Wikipedia.

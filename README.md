# Wikipedia PageRank
## Albert Giegerich, Bryan Prather-Huff, Wyatt Bettis for Large Data Analysis @ the University of Iowa under the supervision of Dr. Suely Oliveira

This project contains the source for a Spark based implementation of the Google PageRank algorithm (PySparkPageRank). It also includes a Java implementation of the same algorithm (JavaPageRank) taken from [Project Nayuki](https://www.nayuki.io/page/computing-wikipedias-internal-pageranks) and modified for the modern schema of the [Wikipedia database dumps](https://dumps.wikimedia.org/). The required databases are the `pages` and `pagelinks` tables from the aforementioned database dump.

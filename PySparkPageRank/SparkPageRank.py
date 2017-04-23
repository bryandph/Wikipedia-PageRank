from pyspark import *
from operator import add

def parse_int(id):
    try:
        id = int(id)
    except:
        id = -1
    return id


def remove_invalid_links(links):
    new_links = []
    for i in links:
        if i >= 0:
            new_links.append(i)
    return new_links

class PageRank:
    def __init__(self, data_file, output_file):
        self.data_file = data_file
        self.output_file = output_file



    def runPageRank(self, iterations):

        print("Reading data...")
        damp_param = 0.85
        raw_data = sc.textFile(self.data_file)
        # Organize raw data to run_length encoding
        target_to_source = raw_data.map(lambda l: l.split(",")).map(lambda p:(int(p[0]), map(lambda e: parse_int(''.join([c for c in e if c in '1234567890'])), p[2:]))).reduceByKey(lambda x,y: x+y).filter(lambda (id, links): id != -1).map(lambda (id, links): (id, remove_invalid_links(links))).flatMap(lambda (id, links): [(id, (x, len(links))) for x in links])

        source_to_target = target_to_source.flatMap(lambda x: [(src,[x[0]]) for src in x[1]]).reduceByKey(lambda x,y: x+y).filter(lambda (id, links): id != -1).map(lambda (id, links): (id, remove_invalid_links(links))).flatMap(lambda (id, links): [(id, (x, len(links))) for x in links])

        source_to_target_no_count = source_to_target.map(lambda (src, (tar, num_tar)): (src, tar))

        print("Data read in!")

        # Get number of distinct articles
        no_out = source_to_target.filter(lambda (id, links): len(links) == 0).map(lambda (id, link): id).distinct()


        active_links = sc.union([target_to_source.map(lambda (target, source): target), source_to_target.map(lambda (source, target): source)]).distinct()
        num_active = active_links.count()
        print("post distincts")
        init_weight = 1.0/num_active
        pr_scores = active_links.map(lambda x: (x, init_weight))

        print("starting pagerank")
        for i in range(iterations):
            # (source, (target, total_targets)) JOIN (link, weight) = (source, ((target, total_targets), source_weight))
            new_pr_scores = source_to_target.join(pr_scores).map(lambda (src, (tar_total, weight)): (src, weight/tar_total[1]))
            pr_scores = new_pr_scores.fullOuterJoin(pr_scores).map(lambda (id, (new, old)): (id, new if new is not None else old))

            # (id, 1) JOIN (link, weight) = (id, (1, weight))
            no_out_bias = 0 if no_out.count() == 0 else no_out.map(lambda id: (id, 1)).join(pr_scores).map(lambda (id, (one, weight)): weight).reduce(add)

            # (src, tar) JOIN (src, weight) Z= (src, (tar, src_weight))
            pr_scores = source_to_target_no_count.join(pr_scores).map(lambda (src, (tar, src_weight)): (tar, src_weight)).reduceByKey(add).map(lambda (id, weight): (id, weight * damp_param + no_out_bias * damp_param + (1 - damp_param) / num_active))

        pr_scores = pr_scores.map(lambda (x,y): (y,x)).sortByKey(True, 1).map(lambda (x,y): (y,x))
        with open(self.output_file, "w") as f:
            for page_id, page_rank  in pr_scores.collect():
                f.write("{}, {}\n".format(page_id, page_rank))

        # sql_con = sqlContext.read.format("jdbc").option("url", "jdbc:mariadb://blackwin.personal.bryanph.com/wikipedia").option("driver", "org.mariadb.jdbc.Driver").option("dbtable", "pagerank").option("user", "bryan").option("password", "supereasypassword").load()

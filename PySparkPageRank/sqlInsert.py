import antlr4
from sqlInsertLexer import sqlInsertLexer
from sqlInsertListener import sqlInsertListener
from sqlInsertParser import sqlInsertParser


class sqlInsertPrintListener(sqlInsertListener):

    def __init__(self, *args, **kwargs):
        super(sqlInsertPrintListener, self).__init__(*args, **kwargs)
        self.collector = []

    def enterEntry(self, ctx):
        self.collector.append((int(ctx.pid().getText()), int(
            ctx.from_ns().getText()), ctx.ptxt().getText(), int(ctx.to_ns().getText())))


class SqlInsertParser:

    def __init__(self):
        self.parser = sqlInsertParser
        self.stream = antlr4.CommonTokenStream
        self.lexer = sqlInsertLexer
        self.walker = antlr4.ParseTreeWalker

    def parse_line(self, line):
        lexer = self.lexer(antlr4.InputStream(line))
        stream = self.stream(lexer)
        parser = self.parser(stream)
        tree = parser.s()
        printer = sqlInsertPrintListener()
        walker = self.walker()
        walker.walk(printer, tree)
        return(printer.collector)


if __name__ == '__main__':
    test_file = "/home/bryan/SQLInsert/test_string.txt"
    test_file2 = "A:/provisions for LDA server/enwiki-latest-pagelinks.sql/enwiki-latest-pagelinks-sample.sql"
    test_string = u"INSERT INTO `pagelinks` VALUES (22175644,0,'(I_Just_Want_It)_To_Be_Over',0),(22743534,0,'(I_Just_Want_It)_To_Be_Over',0),(22776783,0,'(I_Just_Want_It)_To_Be_Over',0),(22851158,0,'(I_Just_Want_It)_To_Be_Over',0),(23895327,0,'(I_Just_Want_It)_To_Be_Over',0),(24693358,0,'(I_Just_Want_It)_To_Be_Over',0),(29211757,0,'(I_Just_Want_It)_To_Be_Over',0),(29763249,0,'(I_Just_Want_It)_To_Be_Over',0),(29876102,0,'(I_Just_Want_It)_To_Be_Over',0),(30593670,0,'(I_Just_Want_It)_To_Be_Over',0),(33435021,0,'(I_Just_Want_It)_To_Be_Over',0);\n"
    p = SqlInsertParser()
    tuples = p.parse_line(test_string)

    out_file = 'A:/provisions for LDA server/pagelinks_raw.csv'
    current_title = ''
    with open(test_file, 'r') as f:
       with open('./sparse_matrix.txt', 'w') as o:
           for line in f:
               if line[:6] == 'INSERT':
                   tuples = p.parse_line(line.decode('utf8'))
                   for i in tuples:
                       print(i)
                       o.write('{}\n'.format(str(i)))

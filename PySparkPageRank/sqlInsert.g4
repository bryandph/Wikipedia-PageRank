grammar sqlInsert;

INSERT : 'INSERT';
INTO : 'INTO';
VALUES : 'VALUES';

L_PARENTHS: '(';
R_PARENTHS: ')';
COMMA: ',';

INT : DIGIT+ ;
fragment DIGIT : [0-9] ;

QUOTED_STRING :   QUOTE ( ESCAPED_QUOTE | ~('\n'|'\r') )*? QUOTE;
fragment ESCAPED_QUOTE : '\\' QUOTE;
fragment QUOTE : ('\''|'"'|'`');

WS: ( ' ' | '\t')+ -> skip;

NL: '\r'? '\n' | '\r';
ENDLINE: ';' NL;

file: (s | comment)* EOF;
comment: ~INSERT (~NL)*? NL;
s: INSERT INTO db_table VALUES (entry ','?)+ ENDLINE;
db_table: QUOTED_STRING;
entry: (L_PARENTHS pid COMMA from_ns COMMA ptxt COMMA to_ns R_PARENTHS);
pid: INT;
from_ns: INT;
to_ns: INT;
ptxt: QUOTED_STRING;

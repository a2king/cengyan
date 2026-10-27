grammar Gql;

// 关键字不区分大小写，标识符区分大小写。标识符只允许英文、数字和下划线。

script: statement ';'? EOF;

statement
    : CREATE GRAPH name=IDENT                                 #createGraph
    | USE name=IDENT                                          #useGraph
    | CREATE ALIAS alias=IDENT FOR GRAPH target=IDENT         #createAlias
    | DROP ALIAS name=IDENT                                   #dropAlias
    | DROP GRAPH name=IDENT                                   #dropGraph
    | DROP INDEX name=IDENT                                   #dropIndex
    | SHOW GRAPHS                                             #showGraphs
    | SHOW ALIASES                                            #showAliases
    | SHOW NODE TYPES                                         #showNodeTypes
    | SHOW REL TYPES                                          #showRelTypes
    | SHOW INDEXES                                            #showIndexes
    | DESCRIBE NODE TYPE name=IDENT                           #describeNode
    | DESCRIBE REL TYPE name=IDENT                            #describeRel
    | CREATE NODE TYPE name=IDENT '(' fields? ')'             #createNodeType
    | CREATE REL TYPE name=IDENT '(' fields? ')'              #createRelType
    | CREATE UNIQUE? INDEX name=IDENT ON NODE TYPE typeName=IDENT '(' indexCols ')' #createNodeIndex
    | CREATE UNIQUE? INDEX name=IDENT ON REL TYPE typeName=IDENT '(' indexCols ')' #createRelIndex
    | ALTER NODE TYPE typeName=IDENT ADD prop=IDENT valueType NULL #alterNode
    | ALTER REL TYPE typeName=IDENT ADD prop=IDENT valueType NULL #alterRel
    | INSERT node (edge node)?                                #insertStmt
    | MATCH node (edge node)? whereClause? SET assignment (',' assignment)* #setStmt
    | MATCH node (edge node)? whereClause? DELETE IDENT (',' IDENT)* #deleteStmt
    | MATCH node edge node whereClause? returnClause orderClause? skipClause? limitClause? #matchStmt
    | CALL namespace=IDENT '.' name=IDENT '(' '{' callArgs? '}' ')' YIELD yieldItem (',' yieldItem)* #callStmt
    ;

fields: field (',' field)*;
field: name=IDENT valueType NULL?;
indexCols: IDENT (',' IDENT)*;
valueType: BOOL | BOOLEAN | INT | INTEGER | INT64 | DOUBLE | STRING | DATETIME;

node: '(' variable=IDENT? (':' typeName=IDENT)? props? ')';
edge
    : '-' '[' edgeBody ']' '->' quantifier?   #edgeOut
    | '<-' '[' edgeBody ']' '-' quantifier?   #edgeIn
    | '-' '[' edgeBody ']' '-' quantifier?    #edgeAny
    ;
edgeBody: variable=IDENT? ':' typeName=IDENT ('@' rank=INTEGER_LIT)? props?;
// 星号表示所有路径。手写解析器会拒绝，并提示改用最短路或加上边界。
quantifier: '{' min=INTEGER_LIT (',' max=INTEGER_LIT)? '}' | '*';
callArgs: callArg (',' callArg)*;
callArg: name=IDENT ':' expr;
yieldItem: IDENT;
props: '{' (prop (',' prop)*)? '}';
prop: name=IDENT ':' expr;

whereClause: WHERE expr;
returnClause: RETURN DISTINCT? item (',' item)*;
assignment: variable=IDENT '.' field=IDENT '=' expr;
item: expr (AS alias=IDENT)?;
orderClause: ORDER BY sortItem (',' sortItem)*;
sortItem: expr (ASC | DESC)?;
skipClause: SKIP INTEGER_LIT;
limitClause: LIMIT INTEGER_LIT;

expr: orExpr;
orExpr: andExpr (OR andExpr)*;
andExpr: notExpr (AND notExpr)*;
notExpr: NOT notExpr | predicate;
predicate
    : addExpr cmpOp addExpr                              #compareExpr
    | addExpr not=NOT? IN '(' expr (',' expr)* ')'       #inExpr
    | addExpr IS not=NOT? NULL                           #isNullExpr
    | addExpr                                            #valueExpr
    ;
cmpOp: '=' | '<>' | '<=' | '>=' | '<' | '>';
addExpr: mulExpr (('+' | '-') mulExpr)*;
mulExpr: unary (('*' | '/') unary)*;
unary: '-' unary | primary;
primary
    : literal                    #litPrimary
    | parameter                  #paramPrimary
    | call                       #callPrimary
    | variable=IDENT '.' field=IDENT #propPrimary
    | name=IDENT                 #varPrimary
    | '(' expr ')'               #parenPrimary
    ;
call: name=IDENT '(' ('*' | expr (',' expr)*)? ')';
parameter: '$' name=IDENT;
literal: INTEGER_LIT | REAL_LIT | STRING_LIT | TRUE | FALSE | NULL | DATETIME STRING_LIT;

BOOL: B O O L;
BOOLEAN: B O O L E A N;
INT64: I N T '64';
INT: I N T;
INTEGER: I N T E G E R;
DOUBLE: D O U B L E;
STRING: S T R I N G;
DATETIME: D A T E T I M E;
CREATE: C R E A T E;
GRAPH: G R A P H;
GRAPHS: G R A P H S;
USE: U S E;
ALIAS: A L I A S;
ALIASES: A L I A S E S;
FOR: F O R;
DROP: D R O P;
SHOW: S H O W;
NODE: N O D E;
REL: R E L;
TYPES: T Y P E S;
TYPE: T Y P E;
DESCRIBE: D E S C R I B E;
ALTER: A L T E R;
ADD: A D D;
NULL: N U L L;
INSERT: I N S E R T;
MATCH: M A T C H;
CALL: C A L L;
YIELD: Y I E L D;
SET: S E T;
DELETE: D E L E T E;
DISTINCT: D I S T I N C T;
WHERE: W H E R E;
RETURN: R E T U R N;
ORDER: O R D E R;
BY: B Y;
LIMIT: L I M I T;
SKIP: S K I P;
AND: A N D;
OR: O R;
NOT: N O T;
IS: I S;
IN: I N;
AS: A S;
ASC: A S C;
DESC: D E S C;
TRUE: T R U E;
FALSE: F A L S E;
INDEXES: I N D E X E S;
INDEX: I N D E X;
UNIQUE: U N I Q U E;
ON: O N;

fragment A: [Aa];
fragment B: [Bb];
fragment C: [Cc];
fragment D: [Dd];
fragment E: [Ee];
fragment F: [Ff];
fragment G: [Gg];
fragment H: [Hh];
fragment I: [Ii];
fragment J: [Jj];
fragment K: [Kk];
fragment L: [Ll];
fragment M: [Mm];
fragment N: [Nn];
fragment O: [Oo];
fragment P: [Pp];
fragment Q: [Qq];
fragment R: [Rr];
fragment S: [Ss];
fragment T: [Tt];
fragment U: [Uu];
fragment V: [Vv];
fragment W: [Ww];
fragment X: [Xx];
fragment Y: [Yy];
fragment Z: [Zz];

INTEGER_LIT: [0-9]+;
REAL_LIT: [0-9]+ '.' [0-9]* | '.' [0-9]+;
STRING_LIT: '\'' (~['\r\n] | '\'\'')* '\'' | '"' (~["\r\n] | '""')* '"';
IDENT: [A-Za-z] [A-Za-z0-9_]*;

WS: [ \t\r\n]+ -> skip;
LINE_COMMENT: '//' ~[\r\n]* -> skip;
BLOCK_COMMENT: '/*' .*? '*/' -> skip;

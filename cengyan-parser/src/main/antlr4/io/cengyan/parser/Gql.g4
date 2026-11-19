grammar Gql;

// 层岩 GQL 参考文法。对照 ISO/IEC 39075:2024，标准写法都要能解析、含义和标准一致；
// 标注「扩展」的是层岩自有写法。手写解析器在 GqlReader，本文件只作对照，构建时不生成。
// 关键字不区分大小写，标识符区分大小写。除 AND OR XOR NOT TRUE FALSE NULL UNKNOWN AS IN IS 外，
// 关键字都可以当名字用。

script: statement (';' statement)* ';'? EOF;

statement
    : EXPLAIN statement                                                       // 扩展
    | USE graphName block                                                      // 只对这一条查询换图
    | USE graphName                                                            // 切换当前图（扩展写法）
    | SESSION SET PROPERTY? GRAPH graphName                                    // 切换当前图
    | createGraph | createType | createIndex | createAlias                     // DDL，后三种是扩展
    | DROP PROPERTY? GRAPH ifExists? graphName ifExists?
    | DROP (ALIAS | INDEX) name ifExists?                                      // 扩展
    | DROP (NODE | REL | EDGE) TYPE name ifExists?                             // 扩展
    | SHOW (GRAPHS | ALIASES | INDEXES | NODE TYPES | REL TYPES)               // 扩展
    | DESCRIBE (NODE | REL | EDGE) TYPE name                                   // 扩展
    | ALTER (NODE | REL | EDGE) TYPE name alterAction                          // 扩展
    | block
    ;
// START TRANSACTION / COMMIT / ROLLBACK、SESSION 的其他命令、CREATE GRAPH TYPE、CREATE OR REPLACE、
// AS COPY OF、KEEP、SELECT 不支持，解析时给出明确报错。

graphName: name;   // 没有目录路径

// ---------------------------------------------------------------- 组合查询

block: composite (NEXT (YIELD item (',' item)*)? composite)*;   // NEXT 比集合运算结合得松
composite: linear (setOp linear)*;
setOp: (UNION | EXCEPT | INTERSECT) (ALL | DISTINCT)? | OTHERWISE;

linear
    : queryClause* resultTail aggregateFilter?
    | queryClause* INSERT insertElements resultTail?
    | queryClause+ mutation+ resultTail?
    | callNamed                                                    // 单独的过程调用，可以不写 RETURN
    ;
aggregateFilter: FILTER expr;                                      // 扩展：整条查询结果再过滤

queryClause
    : MATCH matchBody
    | OPTIONAL MATCH matchBody
    | OPTIONAL ('{' (OPTIONAL? MATCH matchBody)+ '}' | '(' (OPTIONAL? MATCH matchBody)+ ')')
    | FILTER WHERE? expr
    | LET name '=' expr (',' name '=' expr)*
    | FOR name IN expr (WITH (ORDINALITY | OFFSET) name)?
    | orderBy? (OFFSET | SKIP)? limit?
    | OPTIONAL? CALL callTarget
    ;
callTarget: callNamed | ('(' (name (',' name)*)? ')')? '{' block '}';   // 内联过程只能读
callNamed: CALL? name ('.' name)* '(' callArgs? ')' (YIELD name (AS name)? (',' name (AS name)?)*)?;
callArgs: '{' (name ':' expr (',' name ':' expr)*)? '}' | expr (',' expr)*;   // {名字: 值} 是扩展

mutation
    : SET setItem (',' setItem)*
    | REMOVE removeItem (',' removeItem)*
    | (DETACH | NODETACH)? DELETE name (',' name)*
    ;
setItem: name '.' name '=' expr | name '=' expr | name (':' | IS) name;
removeItem: name '.' name | name (':' | IS) name;

resultTail
    : FINISH
    | RETURN NO BINDINGS
    | RETURN (DISTINCT | ALL)? ('*' | item (',' item)*)
        (GROUP BY ('(' ')' | expr (',' expr)*))?
        (FILTER expr)?                                             // 扩展：分组后过滤
        orderBy? ((OFFSET | SKIP) expr)? (LIMIT expr)?
    ;
item: expr (AS name)?;
orderBy: ORDER BY expr (ASC | DESC)? (',' expr (ASC | DESC)?)*;
limit: LIMIT expr;

// ---------------------------------------------------------------- 图模式

matchBody: matchMode? pathPattern (',' pathPattern)* (WHERE expr)?;
matchMode: REPEATABLE (ELEMENT BINDINGS? | ELEMENTS) | DIFFERENT (EDGE BINDINGS? | EDGES);
pathPattern: (name '=')? pathPrefix? pathTerm (('|' | '|+|') pathTerm)*;
pathPrefix
    : ALL SHORTEST? pathMode? (PATH | PATHS)?
    | ANY (SHORTEST | INTEGER_LIT)? pathMode? (PATH | PATHS)?
    | SHORTEST INTEGER_LIT? pathMode? (PATH | PATHS)? (GROUP | GROUPS)?
    | pathMode (PATH | PATHS)?
    ;
pathMode: WALK | TRAIL | SIMPLE | ACYCLIC;
pathTerm: element (edge element | group | node)*;   // 量化路径可以和点直接相邻
element: node | group;
group: '(' node (edge node)* (WHERE expr)? ')' quantifier;   // 目前组里只能有一条边

node: '(' name? ((':' | IS) labelExpr)? props? (WHERE expr)? ')';
edge
    : '-[' edgeBody ']->' | '<-[' edgeBody ']-' | '-[' edgeBody ']-' | '<-[' edgeBody ']->'
    | '<~[' edgeBody ']~' | '~[' edgeBody ']~>'                    // 层岩只有有向边，~[ ]~ 报错
    | '->' | '<-' | '-' | '<->' | '<~' | '~>'
    ;   // 后面可以接 quantifier
edgeBody: name? ((':' | IS) labelExpr)? ('@' INTEGER_LIT)? props? (WHERE expr)?;   // @rank 是扩展
quantifier: '*' | '+' | '?' | '{' INTEGER_LIT '}' | '{' INTEGER_LIT? ',' INTEGER_LIT? '}';
labelExpr: labelAnd ('|' ':'? labelAnd)*;
labelAnd: labelNot (('&' | AND) labelNot)*;   // 模式里的 AND 是扩展；表达式里的 n:A AND ... 是逻辑与
labelNot: ('!' | NOT) labelNot | name | '%' | '(' labelExpr ')';
props: '{' (name ':' expr (',' name ':' expr)*)? '}';

insertElements: node ((edge node) | (',' node))*;

// ---------------------------------------------------------------- DDL

createGraph
    : CREATE PROPERTY? GRAPH ifNotExists? graphName ifNotExists?
      ( PARTS INTEGER_LIT | REPLICAS INTEGER_LIT | ID (INT64 | INT | INTEGER | STRING '(' INTEGER_LIT ')')   // 扩展
      | COMMENT STRING_LIT                                                                               // 扩展
      | (TYPED | '::')? '{' (elementType (',' elementType)*)? '}'
      | ANY PROPERTY? GRAPH?
      | LIKE graphName )*
    ;
elementType
    : '(' name? ((':' | IS | LABEL) name)? typeProps? ')'                                  // 点类型
    | '(' name? ((':' | IS) name)? ')' ('-[' | '<-[') name? ((':' | IS | LABEL) name)? typeProps? (']->' | ']-')
      '(' name? ((':' | IS) name)? ')'                                                   // 边类型
    | NODE TYPE? name? ((LABEL | LABELS | ':' | IS) name)? typeProps?
    | DIRECTED? EDGE TYPE? name? ((LABEL | LABELS | ':' | IS) name)? typeProps? CONNECTING '(' name ('->' | TO) name ')'
    ;   // 类型名就是标签，一个类型只有一个标签
typeProps: '{' (field (',' field)*)? '}';

createType: CREATE (NODE | REL | EDGE) TYPE name ifNotExists? commentClause? (ID (STRING '(' INTEGER_LIT ')' | INT | INT64 | INTEGER))?
            ('(' (field (',' field)*)? ')' | typeProps);
field: name (TYPED | '::')? valueType (NOT NULL | NULL)? commentClause?;   // 默认可空
alterAction: ADD name (TYPED | '::')? valueType NULL? commentClause? | DROP name | RENAME name TO name;
createIndex: CREATE UNIQUE? INDEX ifNotExists? name ON (NODE | REL | EDGE) TYPE name '(' name (',' name)* ')';
createAlias: CREATE ALIAS name FOR GRAPH name;
commentClause: COMMENT STRING_LIT;
ifNotExists: IF NOT EXISTS;
ifExists: IF EXISTS;

// 存成 BOOL / INT64 / DOUBLE / STRING / DATETIME。DATE、TIME、DURATION、LIST、RECORD、BYTES、DECIMAL、
// 定宽整数、FLOAT32、带长度的字符串报错。
valueType
    : BOOL | BOOLEAN
    | INT | INTEGER | INT64 | INTEGER64 | BIGINT | SIGNED (INTEGER64 | BIGINT | INTEGER | INT64)
    | DOUBLE PRECISION? | FLOAT | FLOAT64
    | STRING | VARCHAR | CHAR
    | DATETIME | ZONED DATETIME | TIMESTAMP (WITH TIME ZONE)?
    ;

// ---------------------------------------------------------------- 表达式

expr: orExpr;
orExpr: andExpr ((OR | XOR) andExpr)*;   // 标准里 OR 和 XOR 同一级
andExpr: notExpr (AND notExpr)*;
notExpr: NOT notExpr | predicate;
predicate
    : addExpr (cmpOp addExpr)?
    | addExpr NOT? IN ('(' expr (',' expr)* ')' | '[' (expr (',' expr)*)? ']' | addExpr)   // 扩展：标准没有列表成员谓词
    | name ':' labelExpr
    | addExpr IS NOT? (NULL | TRUE | FALSE | UNKNOWN | DIRECTED)
    | addExpr IS NOT? (TYPED | '::') valueType
    | addExpr IS NOT? (LABELED | ':') labelExpr
    | addExpr IS NOT? (SOURCE | DESTINATION) OF addExpr
    ;
cmpOp: '=' | '<>' | '<=' | '>=' | '<' | '>';
addExpr: mulExpr (('+' | '-' | '||') mulExpr)*;
mulExpr: unary (('*' | '/') unary)*;
unary: ('-' | '+') unary | postfix;
postfix: primary ('[' expr (':' expr?)? ']')*;
primary
    : literal
    | '$' name
    | '(' expr ')'
    | CASE expr? (WHEN expr THEN expr)+ (ELSE expr)? END
    | CAST '(' expr AS valueType ')'
    | EXISTS ('{' (matchBody | queryClause+ resultTail?) '}' | '(' (matchBody | queryClause+ resultTail?) ')')
    | VALUE '{' block '}'
    | LET name '=' expr (',' name '=' expr)* IN expr END
    | (LIST | ARRAY)? '[' (expr (',' expr)*)? ']'
    | RECORD? '{' (name ':' expr (',' name ':' expr)*)? '}'
    | TRIM '(' ((BOTH | LEADING | TRAILING)? expr? FROM)? expr ')'
    | (SAME | ALL_DIFFERENT) '(' expr (',' expr)+ ')'
    | PROPERTY_EXISTS '(' expr ',' name ')'
    | REDUCE '(' name '=' expr ',' name IN expr '|' expr ')'      // 扩展
    | name '(' (DISTINCT expr | ALL? '*' | expr (',' expr)*)? ')'
    | CURRENT_TIMESTAMP | LOCAL_TIMESTAMP | LOCAL_DATETIME          // 无参函数可以不写括号
    | name '.' name
    | name
    ;
literal
    : INTEGER_LIT | REAL_LIT | STRING_LIT | TRUE | FALSE | NULL | UNKNOWN
    | (DATETIME | TIMESTAMP | ZONED DATETIME) STRING_LIT
    ;
name: IDENT | DELIMITED_IDENT;

// ---------------------------------------------------------------- 词法

INTEGER_LIT: DIGITS [mM]? | '0' [xX] [0-9a-fA-F_]+ | '0' [oO] [0-7_]+ | '0' [bB] [01_]+;
REAL_LIT: (DIGITS ('.' [0-9_]*)? | '.' DIGITS) ([eE] [+-]? DIGITS)? [dDfF]?;
fragment DIGITS: [0-9] [0-9_]*;
STRING_LIT: '\'' (~['\\\r\n] | '\\' . | '\'\'')* '\'' | '"' (~["\\\r\n] | '\\' . | '""')* '"';
DELIMITED_IDENT: '`' (~[`\r\n] | '``')+ '`';
IDENT: [\p{L}_] [\p{L}\p{N}_\p{Mn}\p{Mc}]*;   // 标准的正则标识符，可以是中文

WS: [ \t\r\n]+ -> skip;
LINE_COMMENT: ('//' | '--') ~[\r\n]* -> skip;
BLOCK_COMMENT: '/*' .*? '*/' -> skip;

// 关键字词法规则从略：每个关键字不区分大小写，见 GqlReader.KEYWORDS。

import { loader } from '@monaco-editor/react';
import * as monaco from 'monaco-editor/esm/vs/editor/editor.api';
import 'monaco-editor/esm/vs/basic-languages/sql/sql.contribution';
import editorWorker from 'monaco-editor/esm/vs/editor/editor.worker?worker';

window.MonacoEnvironment = {
  getWorker() {
    return new editorWorker();
  },
};

loader.config({ monaco });

monaco.languages.register({ id: 'gql' });
monaco.languages.setMonarchTokensProvider('gql', {
  ignoreCase: true,
  keywords: [
    'MATCH', 'OPTIONAL', 'RETURN', 'FILTER', 'LET', 'FOR', 'WHERE', 'WITH',
    'ORDER', 'BY', 'OFFSET', 'SKIP', 'LIMIT', 'GROUP', 'DISTINCT', 'AS',
    'UNION', 'EXCEPT', 'INTERSECT', 'ALL', 'ANY', 'NEXT', 'INSERT', 'SET', 'REMOVE',
    'DELETE', 'DETACH', 'NODETACH', 'CALL', 'YIELD', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END',
    'AND', 'OR', 'XOR', 'NOT', 'IN', 'IS', 'NULL', 'TRUE', 'FALSE', 'EXISTS', 'CAST',
    'CREATE', 'DROP', 'ALTER', 'USE', 'SHOW', 'DESCRIBE', 'GRAPH', 'NODE', 'REL', 'TYPE',
    'INDEX', 'UNIQUE', 'IF', 'EXPLAIN', 'WALK', 'TRAIL', 'SIMPLE', 'ACYCLIC', 'SHORTEST',
  ],
  tokenizer: {
    root: [
      [/\/\/.*$/, 'comment'],
      [/\/\*/, 'comment', '@comment'],
      [/'([^'\\]|\\.)*'/, 'string'],
      [/"([^"\\]|\\.)*"/, 'string'],
      [/`[^`]*`/, 'string'],
      [/\d+(\.\d+)?([eE][+-]?\d+)?/, 'number'],
      [/[A-Za-z_][\w]*/, { cases: { '@keywords': 'keyword', '@default': 'identifier' } }],
    ],
    comment: [
      [/[^\/*]+/, 'comment'],
      [/\*\//, 'comment', '@pop'],
      [/[\/*]/, 'comment'],
    ],
  },
});

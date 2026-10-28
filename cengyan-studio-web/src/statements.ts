/** 去掉注释后，按不在字符串里的分号拆成多条 GQL。 */
export function splitGqlScript(source: string): string[] {
  const statements: string[] = [];
  let current = '';
  let index = 0;
  while (index < source.length) {
    const currentChar = source[index];
    const nextChar = source[index + 1] ?? '';
    if (currentChar === "'" || currentChar === '"') {
      const text = readString(source, index);
      current += text;
      index += text.length;
      continue;
    }
    if ((currentChar === '-' && nextChar === '-') || currentChar === '#' || (currentChar === '/' && nextChar === '/')) {
      while (index < source.length && source[index] !== '\n') index++;
      continue;
    }
    if (currentChar === '/' && nextChar === '*') {
      const end = source.indexOf('*/', index + 2);
      if (end < 0) throw new Error('注释没有结束');
      current += ' ';
      index = end + 2;
      continue;
    }
    if (currentChar === ';') {
      pushStatement(statements, current);
      current = '';
      index++;
      continue;
    }
    current += currentChar;
    index++;
  }
  pushStatement(statements, current);
  return statements;
}

function readString(source: string, start: number): string {
  const quote = source[start];
  let index = start + 1;
  let body = quote;
  while (index < source.length) {
    const current = source[index];
    if (current === '\n' || current === '\r') throw new Error('字符串没有结束');
    if (current === quote) {
      if (source[index + 1] === quote) {
        body += quote + quote;
        index += 2;
        continue;
      }
      return body + quote;
    }
    body += current;
    index++;
  }
  throw new Error('字符串没有结束');
}

function pushStatement(statements: string[], text: string) {
  const trimmed = text.trim();
  if (trimmed) statements.push(trimmed);
}

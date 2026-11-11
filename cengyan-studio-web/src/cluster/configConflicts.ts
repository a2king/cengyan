import type { Role } from '../clusterApi';

/** 参数值能保存，但运行时会被其他参数卡住、不能按填写值生效。 */
export interface ConfigConflict {
  summary: string;
  rules: string[];
}

type Field = { key: string; label: string; value: string };

/** 按当前表单里的生效值找出被关联规则卡住的参数。键是被卡住的参数。 */
export function configConflicts(role: Role, rows: Field[]): Record<string, ConfigConflict> {
  if (role !== 'COMPUTE') return {};
  const byKey = new Map(rows.map((row) => [row.key, row]));
  const slots = integer(byKey.get('query_slots')?.value);
  const queryMemory = integer(byKey.get('query_memory_bytes')?.value);
  const nodeMemory = integer(byKey.get('node_memory_bytes')?.value);
  const queryTemp = integer(byKey.get('query_temp_bytes')?.value);
  const nodeTemp = integer(byKey.get('node_temp_bytes')?.value);
  if (slots == null || queryMemory == null || nodeMemory == null || queryTemp == null || nodeTemp == null) return {};

  // 与 ExecutionSlots.reserve 一致：每个执行名额预留满额单查询内存和临时盘。
  const byMemory = Math.floor(nodeMemory / queryMemory);
  const byDisk = Math.floor(nodeTemp / queryTemp);
  const rules: string[] = [];
  if (byMemory < slots) {
    rules.push(`${name(byKey, 'node_memory_bytes')} ${bytesText(nodeMemory)} ÷ ${name(byKey, 'query_memory_bytes')} ${bytesText(queryMemory)} = ${byMemory}`);
  }
  if (byDisk < slots) {
    rules.push(`${name(byKey, 'node_temp_bytes')} ${bytesText(nodeTemp)} ÷ ${name(byKey, 'query_temp_bytes')} ${bytesText(queryTemp)} = ${byDisk}`);
  }
  if (rules.length === 0) return {};
  const effective = Math.min(byMemory, byDisk);
  return {
    query_slots: {
      summary: `${name(byKey, 'query_slots')} ${slots} 不会全部生效，这个节点同时执行的上限是 ${effective}。每个查询都会占满一份单查询内存和单查询临时盘，多出来的请求会排队。`,
      rules,
    },
  };
}

function name(byKey: Map<string, Field>, key: string) {
  return byKey.get(key)?.label || key;
}

function integer(value: string | undefined) {
  if (value == null || !/^[1-9]\d*$/.test(value.trim())) return null;
  const parsed = Number(value);
  return Number.isSafeInteger(parsed) ? parsed : null;
}

function bytesText(value: number) {
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  let index = 0;
  let shown = value;
  while (shown >= 1024 && index < units.length - 1) {
    shown /= 1024;
    index++;
  }
  return `${Number.isInteger(shown) ? shown : shown.toFixed(1)} ${units[index]}`;
}

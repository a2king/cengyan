import { Tag } from 'antd';

export function bytesText(value?: unknown) {
  const bytes = Number(value);
  if (!Number.isFinite(bytes) || bytes < 0) return '—';
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  let index = 0;
  let amount = bytes;
  while (amount >= 1024 && index < units.length - 1) { amount /= 1024; index += 1; }
  return `${amount.toFixed(index ? 1 : 0)} ${units[index]}`;
}

export function percentText(value?: unknown) {
  const number = Number(value);
  return Number.isFinite(number) && number >= 0 ? `${(number * 100).toFixed(1)}%` : '—';
}

export function rateText(value?: unknown) {
  const number = Number(value);
  return Number.isFinite(number) && number >= 0 ? `${bytesText(number)}/s` : '—';
}

export function identityText(value: unknown) {
  const text = typeof value === 'string' ? value.trim() : value == null ? '' : String(value).trim();
  return text || '—';
}

export function timeText(value: unknown) {
  const time = Number(value);
  if (!Number.isFinite(time) || time <= 0) return '—';
  return new Date(time).toLocaleString();
}

export function numberValue(value: unknown): number | undefined {
  const number = Number(value);
  return value !== null && value !== undefined && value !== '' && Number.isFinite(number) && number >= 0 ? number : undefined;
}

export function recordValue(value: unknown): Record<string, unknown> | undefined {
  return value && typeof value === 'object' ? value as Record<string, unknown> : undefined;
}

export function opsLabel(value: unknown) {
  switch (String(value || '')) {
    case 'GOOD': return '良好';
    case 'HEALTHY': return '正常';
    case 'REACHABLE': return '可达';
    case 'CONCERNING': return '关注';
    case 'DEGRADED': return '缺副本';
    case 'BAD': return '故障';
    case 'UNAVAILABLE': return '不可达';
    case 'INFO': return '信息';
    case 'WARN': return '警告';
    case 'ERROR': return '故障';
    default: return '等待采集';
  }
}

export function opsTag(value: unknown) {
  const text = String(value || '');
  const color = text === 'GOOD' || text === 'HEALTHY' || text === 'REACHABLE' || text === 'INFO' ? 'green'
    : text === 'CONCERNING' || text === 'DEGRADED' || text === 'WARN' ? 'gold'
      : text === 'BAD' || text === 'UNAVAILABLE' || text === 'ERROR' ? 'red' : 'default';
  return <Tag color={color}>{opsLabel(text)}</Tag>;
}

export function escapeHtml(text: string) {
  return text.replace(/[&<>"']/g, (char) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[char] as string));
}

// 集群管理接口 /api/cluster/*。管理员令牌放在 sessionStorage，随请求头 X-Cengyan-Admin 发送。
export type Role = 'STORAGE' | 'COMPUTE';
export type ServerStatus = 'PROVISIONING' | 'READY' | 'FAILED';
export type NodeStatus = 'INSTALLING' | 'RUNNING' | 'FAILED' | 'REMOVING';
export type TaskState = 'RUNNING' | 'PAUSED' | 'INTERRUPTED' | 'DONE' | 'ABORTED';
export type StepState = 'PENDING' | 'RUNNING' | 'DONE' | 'FAILED' | 'SKIPPED';

export interface ServerRow {
  id: string; host: string; sshPort: number; note: string; status: ServerStatus; message: string;
  hostKey: string; arch: string; os: string; hostname: string; releaseId: string; nodes: number;
}
export interface NodeRow {
  id: string; role: Role; serverId: string; host: string; raftPort: number; rpcPort: number; clientPort: number; dataDir: string;
  status: NodeStatus; message: string; joined: boolean; releaseId: string; pendingRestart: boolean; upgradeAvailable: boolean; overrides: number;
}
export interface StepView { title: string; state: StepState; startedAt: number; endedAt: number; log: string[] }
export interface TaskView {
  id: string; type: string; title: string; params: Record<string, string>; state: TaskState; current: number;
  error: string; createdAt: number; updatedAt: number; steps: StepView[];
}
export interface Overview {
  clusterId: string; release: string; version: string; bundleError: string; rootPasswordSet: boolean; configVersion: number;
  servers: ServerRow[]; nodes: NodeRow[]; pendingRestart: number; upgradeAvailable: number; activeTask?: TaskView;
}
export interface MaintenanceStatus {
  active: boolean; id: string; since: number; metadataLeader: string;
  partitions: { total: number; ready: number; writing: number; frozen: number; lost: number };
  nodes: Array<{ nodeId: string; reachable: boolean; sealed: boolean; resumed: boolean; untrustedReplicas: number }>;
}
export interface Runtime { maintenance?: MaintenanceStatus; maintenanceError?: string; computeError?: string; nodes: Record<string, string> }
export interface CatalogEntry {
  key: string; target: string; label: string; type: 'int' | 'bytes' | 'label' | 'jvm'; defaultValue: string;
  min?: number; max?: number; description: string;
}
export type Catalog = Record<Role, CatalogEntry[]>;
export interface ConfigVersion {
  version: number; savedAt: number; note: string;
  roleDefaults: Partial<Record<Role, Record<string, string>>>; nodeOverrides: Record<string, Record<string, string>>;
}
export interface ConfigState { current: ConfigVersion; effective: Record<string, Record<string, string>> }
export interface Change { scope: string; key: string; before: string; after: string }
export interface Suggestion { id: string; raftPort: number; rpcPort: number; clientPort: number; dataDir: string }

export class ClusterError extends Error {
  constructor(readonly code: string, message: string, readonly details: Record<string, string> = {}) { super(message); }
}

const TOKEN_KEY = 'cengyan.studio.admin';

export class ClusterApi {
  onUnauthorized: () => void = () => { };

  get token() { return sessionStorage.getItem(TOKEN_KEY) || ''; }
  set token(value: string) { if (value) sessionStorage.setItem(TOKEN_KEY, value); else sessionStorage.removeItem(TOKEN_KEY); }

  private async call<T>(method: 'GET' | 'POST', path: string, body?: unknown): Promise<T> {
    const response = await fetch(`/api/cluster/${path}`, {
      method,
      headers: { 'Content-Type': 'application/json', ...(this.token ? { 'X-Cengyan-Admin': this.token } : {}) },
      body: method === 'POST' ? JSON.stringify(body ?? {}) : undefined,
    });
    let data: Record<string, unknown>;
    try { data = await response.json() as Record<string, unknown>; }
    catch { throw new ClusterError('INVALID_RESPONSE', `Studio 返回了无效响应（HTTP ${response.status}）`); }
    if (response.status === 401 && !path.startsWith('admin/')) { this.token = ''; this.onUnauthorized(); }
    if (!response.ok || data.code !== 'OK') {
      throw new ClusterError(String(data.code || 'FAILED'), String(data.message || '请求失败'), (data.details as Record<string, string>) || {});
    }
    return ('data' in data ? data.data : data) as T;
  }

  status() { return this.call<{ initialized: boolean }>('GET', 'admin/status'); }
  async setup(password: string) { this.token = (await this.call<{ token: string }>('POST', 'admin/setup', { password })).token; }
  async login(password: string) { this.token = (await this.call<{ token: string }>('POST', 'admin/login', { password })).token; }
  async logout() { try { await this.call('POST', 'admin/logout'); } finally { this.token = ''; } }
  changePassword(current: string, password: string) { return this.call('POST', 'admin/password', { current, password }); }

  overview() { return this.call<Overview>('GET', 'overview'); }
  runtime() { return this.call<Runtime>('GET', 'runtime'); }
  catalog() { return this.call<Catalog>('GET', 'catalog'); }

  addServer(host: string, sshPort: number, password: string, note: string) { return this.call<TaskView>('POST', 'servers', { host, sshPort, password, note }); }
  removeServer(id: string) { return this.call<TaskView>('POST', 'servers/remove', { id }); }

  suggest(role: Role, server: string) { return this.call<Suggestion>('GET', `nodes/suggest?role=${role}&server=${encodeURIComponent(server)}`); }
  addNode(node: { role: Role; serverId: string; id: string; raftPort: number; rpcPort: number; clientPort: number; dataDir: string }) {
    return this.call<TaskView>('POST', 'nodes', node);
  }
  removeNode(id: string, deleteData: boolean, confirm: string) { return this.call<TaskView>('POST', 'nodes/remove', { id, deleteData, confirm }); }
  logs(id: string, lines = 300) { return this.call<{ text: string }>('GET', `nodes/logs?id=${encodeURIComponent(id)}&lines=${lines}`); }
  restart(nodes: string[]) { return this.call<TaskView>('POST', 'restart', { nodes }); }
  upgrade() { return this.call<TaskView>('POST', 'upgrade'); }

  config() { return this.call<ConfigState>('GET', 'config'); }
  saveConfig(roleDefaults: ConfigVersion['roleDefaults'], nodeOverrides: ConfigVersion['nodeOverrides'], note: string) {
    return this.call<ConfigVersion>('POST', 'config', { roleDefaults, nodeOverrides, note });
  }
  history() { return this.call<ConfigVersion[]>('GET', 'config/history'); }
  diff(from: number, to: number) { return this.call<Change[]>('GET', `config/diff?from=${from}&to=${to}`); }
  rollback(version: number) { return this.call<ConfigVersion>('POST', 'config/rollback', { version }); }
  setRootPassword(password: string) { return this.call('POST', 'root-password', { password }); }

  tasks(limit = 20) { return this.call<TaskView[]>('GET', `tasks?limit=${limit}`); }
  task(id: string) { return this.call<TaskView>('GET', `tasks/detail?id=${encodeURIComponent(id)}`); }
  act(id: string, action: 'retry' | 'skip' | 'abort', options: { password?: string; exitMaintenance?: boolean } = {}) {
    return this.call<TaskView>('POST', 'tasks/action', { id, action, ...options });
  }
  exitMaintenance() { return this.call('POST', 'maintenance/exit'); }
}

export const STATUS_TEXT: Record<string, { text: string; color: string }> = {
  PROVISIONING: { text: '添加中', color: 'processing' }, READY: { text: '就绪', color: 'success' }, FAILED: { text: '失败', color: 'error' },
  INSTALLING: { text: '安装中', color: 'processing' }, RUNNING: { text: '运行中', color: 'success' }, REMOVING: { text: '移除中', color: 'warning' },
  PAUSED: { text: '已暂停', color: 'warning' }, INTERRUPTED: { text: '已中断', color: 'warning' }, DONE: { text: '已完成', color: 'success' },
  ABORTED: { text: '已终止', color: 'default' }, ACTIVE: { text: '在线', color: 'success' }, DRAINING: { text: '排空中', color: 'warning' },
  OFFLINE: { text: '离线', color: 'error' }, UNKNOWN: { text: '未知', color: 'default' },
};

export function errorText(reason: unknown) { return reason instanceof Error ? reason.message : String(reason); }
export function timeText(millis: number) { return millis ? new Date(millis).toLocaleString('zh-CN', { hour12: false }) : '—'; }

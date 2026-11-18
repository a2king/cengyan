import type { OpsOverview, OpsRange, OpsSeries, QueryResponse, SessionInfo, StudioConfig, UserRow } from './types';

const SESSION_KEY = 'cengyan.studio.session';
const USER_KEY = 'cengyan.studio.user';
const ROLE_KEY = 'cengyan.studio.role';
const LOCAL_KEY = 'cengyan.studio.local';
const GRAPH_KEY = 'cengyan.studio.graph';

export const storage = {
  loadSession(): SessionInfo | null {
    const session = sessionStorage.getItem(SESSION_KEY);
    const user = sessionStorage.getItem(USER_KEY);
    const role = sessionStorage.getItem(ROLE_KEY);
    return session && user ? { session, user, role: role || 'read', local: sessionStorage.getItem(LOCAL_KEY) === 'true' } : null;
  },
  saveSession(info: SessionInfo) {
    sessionStorage.setItem(SESSION_KEY, info.session);
    sessionStorage.setItem(USER_KEY, info.user);
    sessionStorage.setItem(ROLE_KEY, info.role);
    if (info.local) sessionStorage.setItem(LOCAL_KEY, 'true'); else sessionStorage.removeItem(LOCAL_KEY);
  },
  clearSession() {
    sessionStorage.removeItem(SESSION_KEY);
    sessionStorage.removeItem(USER_KEY);
    sessionStorage.removeItem(ROLE_KEY);
    sessionStorage.removeItem(LOCAL_KEY);
    sessionStorage.removeItem(GRAPH_KEY);
  },
  loadGraph() {
    return sessionStorage.getItem(GRAPH_KEY) || '';
  },
  saveGraph(graph: string) {
    if (graph) sessionStorage.setItem(GRAPH_KEY, graph);
    else sessionStorage.removeItem(GRAPH_KEY);
  },
};

export class CengyanApi {
  private session: string;
  private graph = storage.loadGraph();
  private readonly onSession: (session: string) => void;
  private readonly onUnauthorized: () => void;

  constructor(session: string, onSession: (session: string) => void, onUnauthorized: () => void) {
    this.session = session;
    this.onSession = onSession;
    this.onUnauthorized = onUnauthorized;
  }

  async login(user: string, password: string): Promise<SessionInfo> {
    const response = await this.request('/api/login', { user, password });
    const row = response.rows?.[0] || [];
    const info: SessionInfo = {
      session: response.session || '',
      user: String(row[0] || user),
      role: String(row[1] || 'read'),
      local: response.local === true,
    };
    this.session = info.session;
    this.onSession(this.session);
    return info;
  }

  /** 会话令牌：集群管理接口也用它（请求头 X-Cengyan-Session）。 */
  token(): string {
    return this.session;
  }

  /** 首次部署设置 root 密码后，以 root 本地会话进入。 */
  adopt(info: SessionInfo) {
    this.session = info.session;
    this.onSession(this.session);
  }

  async logout(): Promise<void> {
    try { await this.request('/api/logout', {}); } catch { /* 退出只是丢弃令牌 */ }
    this.session = '';
  }

  async changePassword(current: string, password: string): Promise<void> {
    await this.request('/api/password', { current, password });
  }

  async listUsers(): Promise<UserRow[]> {
    const response = await this.request('/api/admin', { command: 'listUsers', timeoutMs: 15000 });
    return rowsAsObjects<UserRow>(response);
  }

  async createUser(user: string, password: string, role: string): Promise<void> {
    await this.request('/api/admin', { command: 'createUser', user, password, role, timeoutMs: 15000 });
  }

  async setRole(user: string, role: string): Promise<void> {
    await this.request('/api/admin', { command: 'setRole', user, role, timeoutMs: 15000 });
  }

  async setPassword(user: string, password: string): Promise<void> {
    await this.request('/api/admin', { command: 'setPassword', user, password, timeoutMs: 15000 });
  }

  async dropUser(user: string): Promise<void> {
    await this.request('/api/admin', { command: 'dropUser', user, timeoutMs: 15000 });
  }

  setGraph(graph: string) {
    this.graph = graph;
    storage.saveGraph(graph);
  }

  currentGraph(): string {
    return this.graph;
  }

  async query(query: string, timeoutMs = 10000): Promise<QueryResponse> {
    const use = /^\s*USE\s+([\p{L}][\p{L}\p{N}_]*)\s*;?\s*$/iu.exec(query);
    const target = use ? use[1] : usesCurrentGraph(query) ? this.graph : '';
    try {
      const response = await this.request('/api/query', { query, timeoutMs, graph: target });
      if (use) this.setGraph(target);
      return response;
    } catch (error) {
      if (target && target === this.graph && error instanceof Error
          && (error.message.includes(`图已删除：${target}`) || error.message.includes(`图不存在：${target}`))) {
        this.setGraph('');
      }
      throw error;
    }
  }

  async queries(): Promise<QueryResponse> {
    return this.request('/api/queries', {});
  }

  async cancelQuery(queryId: string): Promise<QueryResponse> {
    return this.request('/api/cancel', { queryId });
  }

  async admin(command: string, index = '', timeoutMs?: number, refresh = false): Promise<QueryResponse> {
    return this.request('/api/admin', {
      command,
      index,
      graph: this.graph,
      ...(timeoutMs ? { timeoutMs } : {}),
      ...(refresh ? { refresh: true } : {}),
    });
  }

  async showMembers(graph: string): Promise<QueryResponse> {
    return this.request('/api/admin', { command: 'showMembers', graph, timeoutMs: 60000 });
  }

  async addReplica(graph: string, replicas: number): Promise<QueryResponse> {
    return this.request('/api/admin', { command: 'addReplica', graph, replicas, timeoutMs: 120000 });
  }

  private authHeaders(): Record<string, string> {
    return this.session ? { 'X-Cengyan-Session': this.session } : {};
  }

  async opsOverview(): Promise<OpsOverview> {
    const response = await fetch('/api/ops/overview', { headers: this.authHeaders() });
    if (response.status === 401) this.onUnauthorized();
    const data = await response.json() as OpsOverview;
    if (!response.ok || data.code !== 'OK') throw new Error(data.message || '集群运维数据读取失败');
    return data;
  }

  /** 服务器趋势。未托管集群时 Studio 不记录趋势，返回 null。 */
  async opsSeries(range: OpsRange): Promise<OpsSeries | null> {
    const response = await fetch(`/api/ops/series?range=${range}`, { headers: this.authHeaders() });
    if (response.status === 401) this.onUnauthorized();
    if (response.status === 404) return null;
    const data = await response.json() as OpsSeries;
    if (!response.ok || data.code !== 'OK') throw new Error(data.message || '趋势数据读取失败');
    return data;
  }

  async opsShards(): Promise<Array<Record<string, unknown>>> {
    const response = await fetch('/api/ops/shards', { headers: this.authHeaders() });
    if (response.status === 401) this.onUnauthorized();
    const data = await response.json() as { code: string; message?: string; shards?: Array<Record<string, unknown>> };
    if (!response.ok || data.code !== 'OK') throw new Error(data.message || '分片拓扑读取失败');
    return data.shards || [];
  }

  async ping(): Promise<QueryResponse> {
    return this.request('/api/ping', {});
  }

  async checkSession(): Promise<QueryResponse> {
    return this.request('/api/session', {});
  }

  async config(): Promise<StudioConfig> {
    const response = await fetch('/api/config');
    const data = await response.json() as QueryResponse & Partial<StudioConfig>;
    if (!response.ok || data.code !== 'OK' || !Number.isFinite(data.graphMaxNodes) || !Number.isFinite(data.tableMaxNodes)) {
      throw new Error('Studio 配置读取失败');
    }
    return { graphMaxNodes: Number(data.graphMaxNodes), tableMaxNodes: Number(data.tableMaxNodes), clusterManaged: data.clusterManaged === true };
  }

  private async request(path: string, body: Record<string, unknown>): Promise<QueryResponse> {
    const response = await fetch(path, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        ...(this.session ? { 'X-Cengyan-Session': this.session } : {}),
      },
      body: JSON.stringify(body),
    });
    let data: QueryResponse;
    try {
      data = await response.json() as QueryResponse;
    } catch {
      throw new Error('Studio 服务返回了无效响应');
    }
    if (data.session && data.session !== this.session) {
      this.session = data.session;
      this.onSession(data.session);
    }
    // 只有会话失效才退出登录。FORBIDDEN 是权限不够（例如 write 执行 DDL），不能把用户踢出去；
    // 登录本身失败（密码错误）也不触发退出。
    if (path !== '/api/login' && path !== '/api/password' && (response.status === 401 || data.code === 'UNAUTHENTICATED')) {
      this.onUnauthorized();
    }
    if (!response.ok || data.code !== 'OK') {
      throw new Error(`${data.code || 'REQUEST_FAILED'} ${data.message || '请求失败'}`);
    }
    return data;
  }
}

export function rowsAsObjects<T extends object = Record<string, unknown>>(response: QueryResponse): T[] {
  const columns = response.columns || [];
  return (response.rows || []).map((row) => Object.fromEntries(columns.map((column, index) => [column, row[index]])) as T);
}

function usesCurrentGraph(query: string): boolean {
  return !/^\s*(?:SHOW\s+GRAPHS|SHOW\s+ALIASES|CREATE\s+GRAPH)\b/i.test(query);
}

export function escapeIdent(value: string): string {
  if (!/^[A-Za-z][A-Za-z0-9_]*$/.test(value) || value.length > 128) {
    throw new Error('名字只能使用英文字母、数字和下划线，并且要以字母开头');
  }
  return value;
}

export function graphFromRows(response: QueryResponse): Array<{ name: string; comment?: string; idType?: string; idLength?: number }> {
  const columns = response.columns || [];
  return (response.rows || []).map((row) => {
    const record = Object.fromEntries(columns.map((column, index) => [column, row[index]]));
    const comment = record.comment ?? row[1];
    const length = Number(record.vertexIdLength);
    return {
      name: String(record.name ?? row[0]),
      comment: comment == null || comment === '' ? undefined : String(comment),
      idType: record.vertexIdType == null ? undefined : String(record.vertexIdType),
      idLength: Number.isFinite(length) && length > 0 ? length : undefined,
    };
  });
}

import type { QueryResponse, SessionInfo } from './types';

const SESSION_KEY = 'cengyan.studio.session';
const USER_KEY = 'cengyan.studio.user';
const ROLE_KEY = 'cengyan.studio.role';
const GRAPH_KEY = 'cengyan.studio.graph';

export const storage = {
  loadSession(): SessionInfo | null {
    const session = sessionStorage.getItem(SESSION_KEY);
    const user = sessionStorage.getItem(USER_KEY);
    const role = sessionStorage.getItem(ROLE_KEY);
    return session && user ? { session, user, role: role || 'read' } : null;
  },
  saveSession(info: SessionInfo) {
    sessionStorage.setItem(SESSION_KEY, info.session);
    sessionStorage.setItem(USER_KEY, info.user);
    sessionStorage.setItem(ROLE_KEY, info.role);
  },
  clearSession() {
    sessionStorage.removeItem(SESSION_KEY);
    sessionStorage.removeItem(USER_KEY);
    sessionStorage.removeItem(ROLE_KEY);
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
  private readonly onSession: (session: string) => void;

  constructor(session: string, onSession: (session: string) => void) {
    this.session = session;
    this.onSession = onSession;
  }

  async login(user: string, password: string): Promise<SessionInfo> {
    const response = await this.request('/api/login', { user, password });
    const row = response.rows?.[0] || [];
    const info: SessionInfo = {
      session: response.session || '',
      user: String(row[0] || user),
      role: String(row[1] || 'read'),
    };
    this.session = info.session;
    this.onSession(this.session);
    return info;
  }

  async query(query: string, timeoutMs = 10000): Promise<QueryResponse> {
    return this.request('/api/query', { query, timeoutMs });
  }

  async queries(): Promise<QueryResponse> {
    return this.request('/api/queries', {});
  }

  async cancelQuery(queryId: string): Promise<QueryResponse> {
    return this.request('/api/cancel', { queryId });
  }

  async admin(command: string, index = '', timeoutMs?: number): Promise<QueryResponse> {
    return this.request('/api/admin', { command, index, ...(timeoutMs ? { timeoutMs } : {}) });
  }

  async showMembers(graph: string): Promise<QueryResponse> {
    return this.request('/api/admin', { command: 'showMembers', graph, timeoutMs: 60000 });
  }

  async addReplica(graph: string, replicas: number): Promise<QueryResponse> {
    return this.request('/api/admin', { command: 'addReplica', graph, replicas, timeoutMs: 120000 });
  }

  async ping(): Promise<QueryResponse> {
    return this.request('/api/ping', {});
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

export function escapeIdent(value: string): string {
  if (!/^[A-Za-z][A-Za-z0-9_]*$/.test(value) || value.length > 128) {
    throw new Error('名字只能使用英文字母、数字和下划线，并且要以字母开头');
  }
  return value;
}

export function graphFromRows(response: QueryResponse): string[] {
  return (response.rows || []).map((row) => String(row[0]));
}

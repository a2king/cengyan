export type Role = 'read' | 'write' | string;

export interface SessionInfo {
  session: string;
  user: string;
  role: Role;
}

export interface QueryResponse {
  code: string;
  message?: string;
  session?: string;
  columns?: string[];
  rows?: unknown[][];
  rowsAffected?: number;
  queryId?: string;
  collectedAt?: number;
  elapsedMs?: number;
}

export interface StudioConfig {
  graphMaxNodes: number;
  tableMaxNodes: number;
  /** Studio 以 --home 启动，启用了服务器和节点管理。 */
  clusterManaged?: boolean;
}

export interface GraphSpace {
  name: string;
  aliases: string[];
  comment?: string;
}

export interface TypeRow {
  name: string;
  id: number;
  schemaVersion: number;
  idType?: string;
  idLength?: number;
  comment?: string;
}

export interface TypeProperty {
  property: string;
  type: string;
  nullable: boolean;
  comment?: string;
}

export interface IndexRow {
  name: string;
  target: 'NODE' | 'REL';
  type: string;
  properties: string;
  unique: boolean;
  ready: boolean;
}

export interface StatsRow {
  target: 'NODE' | 'REL';
  name: string;
  count: number;
}

export interface OpsOverview {
  code: string;
  message?: string;
  nodeId?: string;
  role?: string;
  slots?: { capacity?: number; active?: number; reserved?: number; available?: number; memoryReserved?: number; diskReserved?: number };
  queuePending?: number;
  proxied?: number;
  runningQueries?: number;
  heapUsedBytes?: number;
  heapMaxBytes?: number;
  threadCount?: number;
  processCpuLoad?: number;
  systemCpuLoad?: number;
  systemMemoryBytes?: number;
  systemFreeMemoryBytes?: number;
  diskTotalBytes?: number;
  diskUsableBytes?: number;
  clientRequestsTotal?: number;
  clientCompletedTotal?: number;
  clientFailedTotal?: number;
  clientBytesInTotal?: number;
  clientBytesOutTotal?: number;
  computeNodes?: Array<Record<string, unknown>>;
  storageNodes?: Array<Record<string, unknown>>;
  shards?: Array<Record<string, unknown>>;
}

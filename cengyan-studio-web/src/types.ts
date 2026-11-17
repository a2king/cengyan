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
  idType?: string;
  idLength?: number;
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
  diskBusy?: number;
  diskReadBytesPerSec?: number;
  diskWriteBytesPerSec?: number;
  netInBytesPerSec?: number;
  netOutBytesPerSec?: number;
  clientRequestsTotal?: number;
  clientCompletedTotal?: number;
  clientFailedTotal?: number;
  clientBytesInTotal?: number;
  clientBytesOutTotal?: number;
  computeNodes?: Array<Record<string, unknown>>;
  storageNodes?: Array<Record<string, unknown>>;
  /** 只有未托管集群时（Compute 汇总）才在这里给分片；托管集群走 /api/ops/shards。 */
  shards?: Array<Record<string, unknown>>;
  shardSummary?: { total: number; healthy: number; degraded: number; unavailable: number };
  metadataLeaderId?: string;
  clusterHealth?: string;
  collectedAt?: number;
  events?: Array<{ ts: number; nodeId: string; level: string; event: string; message: string }>;
}

/** 服务器级指标。比例是 0..1，速率是字节/秒。 */
export type HostMetric = 'cpu' | 'memory' | 'diskUsed' | 'diskBusy' | 'diskRead' | 'diskWrite' | 'netIn' | 'netOut';
export type OpsRange = '1h' | '1d' | '7d';

export interface OpsHostSeries {
  id: string;
  name: string;
  address: string;
  ts: number[];
  avg: Record<HostMetric, Array<number | null>>;
  /** 1d、7d 才有：每段内的最大值。 */
  max?: Record<HostMetric, Array<number | null>>;
}

export interface OpsSeries {
  code: string;
  message?: string;
  range: OpsRange;
  stepMs: number;
  from: number;
  to: number;
  hosts: OpsHostSeries[];
}

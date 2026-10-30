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

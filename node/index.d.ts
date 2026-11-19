export type Level = 'debug' | 'info' | 'warn' | 'error' | 'fatal';

export interface LoggerOptions {
  url?: string;
  token?: string;
  project?: string;
  projectId?: string;
  platform?: string;
  source?: string;
  level?: Level | string;
  disabled?: boolean;
  heartbeatMs?: number;
  fetch?: typeof globalThis.fetch;
  write?: (line: string) => void;
}

export interface EventExtra {
  traceId?: string;
  actor?: string;
  dedupeKey?: string;
  now?: Date;
}

export interface LoggerStats {
  queued: number;
  dropped: number;
  sent: number;
  rejected: number;
  capacity: number;
}

export interface Logger {
  readonly level: Level;
  debug(event: string, msg: string, data?: unknown, extra?: EventExtra): void;
  info(event: string, msg: string, data?: unknown, extra?: EventExtra): void;
  warn(event: string, msg: string, data?: unknown, extra?: EventExtra): void;
  error(event: string, msg: string, data?: unknown, extra?: EventExtra): void;
  fatal(event: string, msg: string, data?: unknown, extra?: EventExtra): void;
  heartbeat(): void;
  stats(): LoggerStats;
  flush(): Promise<void>;
  shutdown(): Promise<void>;
}

export const CAPACITY: number;
export const BATCH_THRESHOLD: number;
export const MAX_BATCH: number;
export const FLUSH_MS: number;
export const HEARTBEAT_MS: number;
export const MAX_MSG_CHARS: number;
export const MAX_DATA_BYTES: number;

export function createLogger(options?: LoggerOptions): Logger;
export default createLogger;

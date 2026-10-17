import {JobsDefinition} from './JobsDefinition'
import {JobToRun} from './Worker'

export type LogLevels = 'debug' | 'info' | 'warn' | 'error' | 'none'

export const DEFAULT_MAX_TRIES_REACHED_RETENTION_MS = 1000 * 60 * 60 * 24 * 7

/**
 * A lease held for the duration of one job resolver execution.
 */
export interface WorkerExecutionGuardLease {
  release(): Promise<void>
}

/**
 * Acquires infrastructure protection for a claimed job before its resolver starts.
 */
export interface WorkerExecutionGuard {
  acquire(job: JobToRun): Promise<WorkerExecutionGuardLease>
}

/**
 * Job names this server can take. Undefined properties are ignored; an empty allow list takes nothing.
 */
export interface JobsFilter {
  /**
   * Only these jobs can be taken.
   */
  allow?: string[]
  /**
   * These jobs can't be taken.
   */
  deny?: string[]
}

export type GetJobsFilter = () => Promise<JobsFilter>

export interface StartWorkersConfig {
  /**
   * Object map of the jobs that this scheduler will execute.
   */
  jobs: JobsDefinition
  /**
   * Maximum number of tries for an event job before it is marked as 'maxTriesReached'.
   * This is a global default that can be overridden per event job definition.
   */
  maxTries?: number
  /**
   * Time in milliseconds to keep event jobs after they reach their maximum tries.
   * Defaults to one week. Set to null to keep them indefinitely.
   */
  maxTriesReachedRetentionMs?: number | null
  /**
   * Callback invoked when an event job reaches its maximum tries limit.
   * Use this to notify administrators (e.g., send an email alert).
   * The job will remain in the database with status 'maxTriesReached' until its
   * configured retention period expires.
   */
  onMaxTriesReached?: (job: JobToRun) => Promise<void>
  /**
   * Optional infrastructure guard acquired after a job is claimed and before its resolver starts.
   * The returned lease is held until the resolver's Promise settles, including late stale
   * executions. Acquisition failures safely requeue the job without consuming a business try.
   */
  executionGuard?: WorkerExecutionGuard
  /**
   * Optional function evaluated before each claim to decide which jobs this server can take,
   * for example to stop taking heavy jobs while the server is under load.
   * If it throws or takes more than 5 seconds, no jobs are taken on that claim.
   * Running jobs are not affected. It runs before every claim without caching, so keep it cheap.
   */
  getJobsFilter?: GetJobsFilter
  /**
   * Time in milliseconds to wait between each look without results for a job
   * to run at the database. Default is 3000.
   */
  pollInterval?: number
  /**
   * @deprecated The centralized scheduler no longer waits after successful job acquisition or
   * execution. This option is accepted for backwards compatibility and ignored.
   */
  cooldownPeriod?: number
  /**
   * Maximum number of concurrent job executions for this scheduler. Default is 4.
   */
  workersCount?: number
  /**
   * Number of database partitions used to spread job acquisition contention.
   * Each claim queries exactly one partition. Default is 1.
   */
  nPartitions?: number
  /**
   * Default time in milliseconds to lock a job for execution. Default is 30000 (30 seconds).
   * If a job is locked for longer than this time, it will be considered as failed.
   * This is to prevent a job from being executed multiple times at the same time.
   * You can extend this time inside a job by calling extendLockTime from context.
   * Individual jobs can override this value by setting their own lockTime.
   */
  defaultLockTime?: number
}

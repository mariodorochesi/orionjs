import {logger} from '@orion-js/logger'
import {JobsDefinition} from '../types/JobsDefinition'
import {GetJobsFilter, JobsFilter} from '../types/StartConfig'

// NOTE: getJobsFilter and canTakeJobs run before every claim without caching. A burst that fills
// N workers calls them N times. If user functions become expensive (I/O, remote load metrics),
// consider caching their results for a short TTL here instead of in each application.

export const DEFAULT_JOBS_FILTER_TIMEOUT_MS = 5000

/**
 * Checks that are still running, keyed by check name. A timed out check stays here until it
 * settles so later claims skip it immediately instead of waiting for the timeout again.
 */
export type PendingFilterChecks = Map<string, Promise<unknown>>

interface TakeableJobNamesOptions {
  jobs: JobsDefinition
  getJobsFilter?: GetJobsFilter
  jobNames: string[]
  timeoutMs?: number
  pendingChecks?: PendingFilterChecks
}

type ResolvedOptions = Required<Omit<TakeableJobNamesOptions, 'getJobsFilter'>>

const warnedUnknownJobNames = new Set<string>()

async function withTimeout<T>(promise: Promise<T>, timeoutMs: number, name: string): Promise<T> {
  let timer: ReturnType<typeof setTimeout>
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new Error(`${name} timed out after ${timeoutMs}ms`)), timeoutMs)
  })

  try {
    return await Promise.race([promise, timeout])
  } finally {
    clearTimeout(timer)
  }
}

async function runCheck<T>(options: ResolvedOptions, name: string, run: () => Promise<T>) {
  if (options.pendingChecks.has(name)) {
    throw new Error(`${name} is still running from a previous claim`)
  }

  const promise = Promise.resolve().then(run)
  options.pendingChecks.set(name, promise)
  void promise.then(
    () => options.pendingChecks.delete(name),
    () => options.pendingChecks.delete(name),
  )

  return await withTimeout(promise, options.timeoutMs, name)
}

function warnUnknownJobNames(jobs: JobsDefinition, filter: JobsFilter) {
  for (const jobName of [...(filter.allow ?? []), ...(filter.deny ?? [])]) {
    if (jobs[jobName] || warnedUnknownJobNames.has(jobName)) continue
    warnedUnknownJobNames.add(jobName)
    logger.warn(`getJobsFilter returned unknown job name "${jobName}"; it will be ignored.`)
  }
}

async function filterByJobsFilter(options: ResolvedOptions, getJobsFilter: GetJobsFilter) {
  try {
    const filter = (await runCheck(options, 'getJobsFilter', getJobsFilter)) ?? {}
    warnUnknownJobNames(options.jobs, filter)
    return options.jobNames.filter(jobName => {
      if (filter.allow && !filter.allow.includes(jobName)) return false
      if (filter.deny?.includes(jobName)) return false
      return true
    })
  } catch (error) {
    logger.error('Error in getJobsFilter; no jobs will be taken on this claim.', {error})
    return []
  }
}

async function canTakeJob(options: ResolvedOptions, jobName: string) {
  const canTakeJobs = options.jobs[jobName].canTakeJobs
  if (!canTakeJobs) return true

  try {
    return (await runCheck(options, `canTakeJobs of "${jobName}"`, canTakeJobs)) === true
  } catch (error) {
    logger.error(`Error in canTakeJobs of job "${jobName}"; it will not be taken.`, {error})
    return false
  }
}

/**
 * Applies the global getJobsFilter and each job's canTakeJobs to decide which jobs can be claimed.
 * Any error or timeout excludes the affected jobs.
 */
export async function getTakeableJobNames(
  inputOptions: TakeableJobNamesOptions,
): Promise<string[]> {
  const options: ResolvedOptions = {
    timeoutMs: DEFAULT_JOBS_FILTER_TIMEOUT_MS,
    pendingChecks: new Map(),
    ...inputOptions,
  }
  const jobNames = inputOptions.getJobsFilter
    ? await filterByJobsFilter(options, inputOptions.getJobsFilter)
    : options.jobNames

  const checks = await Promise.all(jobNames.map(jobName => canTakeJob(options, jobName)))
  return jobNames.filter((_, index) => checks[index])
}

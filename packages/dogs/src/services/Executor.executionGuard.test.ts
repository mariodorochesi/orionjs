import {describe, expect, it, mock} from 'bun:test'
import {generateId, sleep} from '@orion-js/helpers'
import {defineJob, scheduleJob, startWorkers} from '../index'
import type {WorkerExecutionGuard, WorkerExecutionGuardLease} from '../types/StartConfig'
import type {JobToRun} from '../types/Worker'
import {type ExecuteJobConfig, Executor} from './Executor'

function createDeferred<T = void>() {
  let resolve!: (value: T | PromiseLike<T>) => void
  let reject!: (reason?: unknown) => void
  const promise = new Promise<T>((promiseResolve, promiseReject) => {
    resolve = promiseResolve
    reject = promiseReject
  })

  return {promise, resolve, reject}
}

function createJobToRun(executionId = 'execution-1'): JobToRun {
  return {
    jobId: `job-${executionId}`,
    executionId,
    lockId: executionId,
    name: 'testJob',
    type: 'event',
    params: {executionId},
    tries: 1,
    lockTime: 10_000,
    priority: 100,
  }
}

function createHarness(options: {
  resolve: (params: unknown, context: unknown) => Promise<unknown>
  executionGuard?: WorkerExecutionGuard
}) {
  const executor = new Executor() as any
  const jobsRepo = {
    deleteEventJob: mock(async () => true),
    extendLockTime: mock(async () => true),
    markJobAsMaxTriesReached: mock(async () => true),
    rescheduleAfterExecutionGuardAcquisitionFailure: mock(async () => true),
    scheduleNextRun: mock(async () => true),
    setJobRecordPriority: mock(async () => true),
    updateLockTime: mock(async () => true),
  }
  executor.jobsRepo = jobsRepo
  executor.jobsHistoryRepo = {saveExecution: mock(async () => {})}

  const config: ExecuteJobConfig = {
    jobs: {
      testJob: {
        type: 'event',
        saveExecutionsFor: 0,
        resolve: options.resolve,
      } as any,
    },
    executionGuard: options.executionGuard,
    executionGuardRetryDelayMs: 1_000,
  }

  return {config, executor, jobsRepo}
}

async function execute(
  executor: Executor,
  config: ExecuteJobConfig,
  jobToRun = createJobToRun(),
  onStale = mock(() => {}),
) {
  await executor.executeJob(config, jobToRun, onStale)
}

async function waitUntil(check: () => boolean, timeout = 3_000) {
  const startedAt = Date.now()
  while (!check()) {
    if (Date.now() - startedAt > timeout) throw new Error('Timed out waiting for condition')
    await sleep(10)
  }
}

describe('Executor execution guard', () => {
  it('acquires the guard with the complete JobToRun before invoking the resolver', async () => {
    const acquisition = createDeferred<WorkerExecutionGuardLease>()
    const events: string[] = []
    const release = mock(async () => events.push('release'))
    const executionGuard: WorkerExecutionGuard = {
      acquire: mock(async job => {
        events.push(`acquire:${job.executionId}`)
        return acquisition.promise
      }),
    }
    const resolver = mock(async () => events.push('resolve'))
    const {config, executor} = createHarness({resolve: resolver, executionGuard})
    const jobToRun = createJobToRun()

    const execution = execute(executor, config, jobToRun)
    await Promise.resolve()
    await Promise.resolve()

    expect(executionGuard.acquire).toHaveBeenCalledWith(jobToRun)
    expect(resolver).not.toHaveBeenCalled()

    acquisition.resolve({release})
    await execution

    expect(events).toEqual(['acquire:execution-1', 'resolve', 'release'])
  })

  it('releases the lease after a successful resolver settles', async () => {
    const resolver = createDeferred<void>()
    const release = mock(async () => {})
    const executionGuard: WorkerExecutionGuard = {
      acquire: mock(async () => ({release})),
    }
    const {config, executor} = createHarness({
      resolve: mock(async () => resolver.promise),
      executionGuard,
    })

    const execution = execute(executor, config)
    await Promise.resolve()
    await Promise.resolve()
    expect(release).not.toHaveBeenCalled()

    resolver.resolve()
    await execution

    expect(release).toHaveBeenCalledTimes(1)
  })

  it('releases the lease after a resolver error', async () => {
    const release = mock(async () => {})
    const executionGuard: WorkerExecutionGuard = {
      acquire: mock(async () => ({release})),
    }
    const {config, executor} = createHarness({
      resolve: mock(async () => {
        throw new Error('resolver failed')
      }),
      executionGuard,
    })

    await execute(executor, config)

    expect(release).toHaveBeenCalledTimes(1)
  })

  it('releases each acquired lease exactly once', async () => {
    const release = mock(async () => {})
    const executionGuard: WorkerExecutionGuard = {
      acquire: mock(async () => ({release})),
    }
    const {config, executor} = createHarness({
      resolve: mock(async () => undefined),
      executionGuard,
    })

    await execute(executor, config)

    expect(executionGuard.acquire).toHaveBeenCalledTimes(1)
    expect(release).toHaveBeenCalledTimes(1)
  })

  it('does not invoke the resolver when guard acquisition fails', async () => {
    const resolver = mock(async () => undefined)
    const executionGuard: WorkerExecutionGuard = {
      acquire: mock(async () => {
        throw new Error('guard unavailable')
      }),
    }
    const {config, executor, jobsRepo} = createHarness({resolve: resolver, executionGuard})
    const jobToRun = createJobToRun()
    const before = Date.now()

    await execute(executor, config, jobToRun)

    expect(resolver).not.toHaveBeenCalled()
    expect(jobsRepo.rescheduleAfterExecutionGuardAcquisitionFailure).toHaveBeenCalledTimes(1)
    const [jobId, lockId, nextRunAt] =
      jobsRepo.rescheduleAfterExecutionGuardAcquisitionFailure.mock.calls[0]
    expect(jobId).toBe(jobToRun.jobId)
    expect(lockId).toBe(jobToRun.lockId)
    expect(nextRunAt.getTime()).toBeGreaterThanOrEqual(before + 1_000)
  })

  it('does not consume maxTries when acquisition fails before the resolver starts', async () => {
    const jobName = `guard-${generateId()}`
    const resolverTries: number[] = []
    const release = mock(async () => {})
    const onMaxTriesReached = mock(async () => {})
    const acquire = mock(async () => {
      if (acquire.mock.calls.length === 1) {
        throw new Error('guard temporarily unavailable')
      }
      return {release}
    })
    const executionGuard: WorkerExecutionGuard = {acquire}
    const job = defineJob({
      type: 'event',
      async resolve(_params, context) {
        resolverTries.push(context.tries)
      },
    })
    const instance = startWorkers({
      jobs: {[jobName]: job},
      executionGuard,
      maxTries: 1,
      onMaxTriesReached,
      pollInterval: 10,
      workersCount: 1,
    })

    await scheduleJob({name: jobName})
    await waitUntil(() => resolverTries.length === 1)
    await instance.stop()

    expect(executionGuard.acquire).toHaveBeenCalledTimes(2)
    expect(resolverTries).toEqual([1])
    expect(release).toHaveBeenCalledTimes(1)
    expect(onMaxTriesReached).not.toHaveBeenCalled()
  })

  it('does not turn a successful job into an error when release fails', async () => {
    const release = mock(async () => {
      throw new Error('release failed')
    })
    const executionGuard: WorkerExecutionGuard = {
      acquire: mock(async () => ({release})),
    }
    const {config, executor, jobsRepo} = createHarness({
      resolve: mock(async () => ({ok: true})),
      executionGuard,
    })

    await execute(executor, config)

    expect(release).toHaveBeenCalledTimes(1)
    expect(jobsRepo.deleteEventJob).toHaveBeenCalledTimes(1)
    expect(jobsRepo.scheduleNextRun).not.toHaveBeenCalled()
  })

  it('keeps concurrent jobs associated with independent leases', async () => {
    const resolvers = new Map<string, ReturnType<typeof createDeferred<void>>>()
    const releases = new Map<string, ReturnType<typeof mock>>()
    const executionGuard: WorkerExecutionGuard = {
      acquire: mock(async job => {
        const release = mock(async () => {})
        releases.set(job.executionId, release)
        return {release}
      }),
    }
    const {config, executor} = createHarness({
      resolve: mock(async params => {
        const executionId = (params as {executionId: string}).executionId
        const resolver = createDeferred<void>()
        resolvers.set(executionId, resolver)
        return resolver.promise
      }),
      executionGuard,
    })

    const first = execute(executor, config, createJobToRun('execution-1'))
    const second = execute(executor, config, createJobToRun('execution-2'))
    while (resolvers.size < 2) await Promise.resolve()

    resolvers.get('execution-1')?.resolve()
    await first
    expect(releases.get('execution-1')).toHaveBeenCalledTimes(1)
    expect(releases.get('execution-2')).not.toHaveBeenCalled()

    resolvers.get('execution-2')?.resolve()
    await second
    expect(releases.get('execution-2')).toHaveBeenCalledTimes(1)
  })

  it('keeps the lease while a stale resolver continues in the background', async () => {
    const resolver = createDeferred<void>()
    const release = mock(async () => {})
    const onStale = mock(() => {})
    const executionGuard: WorkerExecutionGuard = {
      acquire: mock(async () => ({release})),
    }
    const {config, executor} = createHarness({
      resolve: mock(async () => resolver.promise),
      executionGuard,
    })
    const jobToRun = {...createJobToRun(), lockTime: 5}

    const execution = execute(executor, config, jobToRun, onStale)
    while (onStale.mock.calls.length === 0) await sleep(2)

    expect(release).not.toHaveBeenCalled()
    resolver.resolve()
    await execution

    expect(release).toHaveBeenCalledTimes(1)
  })

  it('preserves the existing resolver behavior when no guard is configured', async () => {
    const resolver = mock(async () => ({ok: true}))
    const {config, executor, jobsRepo} = createHarness({resolve: resolver})

    await execute(executor, config)

    expect(resolver).toHaveBeenCalledTimes(1)
    expect(jobsRepo.deleteEventJob).toHaveBeenCalledTimes(1)
    expect(jobsRepo.rescheduleAfterExecutionGuardAcquisitionFailure).not.toHaveBeenCalled()
  })
})

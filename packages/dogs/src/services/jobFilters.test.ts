import {describe, expect, it, spyOn} from 'bun:test'
import {logger} from '@orion-js/logger'
import {JobsDefinition} from '../types/JobsDefinition'
import {getTakeableJobNames} from './jobFilters'

const resolve = async () => {}

function jobsDefinition(): JobsDefinition {
  return {
    lightJob: {type: 'event', resolve} as any,
    heavyJob: {type: 'event', resolve} as any,
    recurrentJob: {type: 'recurrent', resolve} as any,
  }
}

const inputJobNames = ['lightJob', 'heavyJob', 'recurrentJob']

describe('getTakeableJobNames', () => {
  it('should return every job when no filters are configured', async () => {
    const actualJobNames = await getTakeableJobNames({
      jobs: jobsDefinition(),
      jobNames: inputJobNames,
    })

    expect(actualJobNames).toEqual(inputJobNames)
  })

  it('should only take allowed jobs', async () => {
    const actualJobNames = await getTakeableJobNames({
      jobs: jobsDefinition(),
      getJobsFilter: async () => ({allow: ['lightJob', 'unknownJob']}),
      jobNames: inputJobNames,
    })

    expect(actualJobNames).toEqual(['lightJob'])
  })

  it('should take nothing with an empty allow list', async () => {
    const actualJobNames = await getTakeableJobNames({
      jobs: jobsDefinition(),
      getJobsFilter: async () => ({allow: []}),
      jobNames: inputJobNames,
    })

    expect(actualJobNames).toEqual([])
  })

  it('should not take denied jobs and ignore an undefined allow list', async () => {
    const actualJobNames = await getTakeableJobNames({
      jobs: jobsDefinition(),
      getJobsFilter: async () => ({allow: undefined, deny: ['heavyJob']}),
      jobNames: inputJobNames,
    })

    expect(actualJobNames).toEqual(['lightJob', 'recurrentJob'])
  })

  it('should apply allow and deny together', async () => {
    const actualJobNames = await getTakeableJobNames({
      jobs: jobsDefinition(),
      getJobsFilter: async () => ({allow: ['lightJob', 'heavyJob'], deny: ['heavyJob']}),
      jobNames: inputJobNames,
    })

    expect(actualJobNames).toEqual(['lightJob'])
  })

  it('should take everything with an empty filter object', async () => {
    const actualJobNames = await getTakeableJobNames({
      jobs: jobsDefinition(),
      getJobsFilter: async () => ({}),
      jobNames: inputJobNames,
    })

    expect(actualJobNames).toEqual(inputJobNames)
  })

  it('should take nothing when getJobsFilter throws', async () => {
    const actualJobNames = await getTakeableJobNames({
      jobs: jobsDefinition(),
      getJobsFilter: async () => {
        throw new Error('Load check failed')
      },
      jobNames: inputJobNames,
    })

    expect(actualJobNames).toEqual([])
  })

  it('should only take jobs whose canTakeJobs resolves to true', async () => {
    const mockJobs = jobsDefinition()
    mockJobs.lightJob.canTakeJobs = async () => true
    mockJobs.heavyJob.canTakeJobs = async () => false
    mockJobs.recurrentJob.canTakeJobs = async () => false

    const actualJobNames = await getTakeableJobNames({jobs: mockJobs, jobNames: inputJobNames})

    expect(actualJobNames).toEqual(['lightJob'])
  })

  it('should not take a job whose canTakeJobs throws', async () => {
    const mockJobs = jobsDefinition()
    mockJobs.heavyJob.canTakeJobs = async () => {
      throw new Error('Load check failed')
    }

    const actualJobNames = await getTakeableJobNames({jobs: mockJobs, jobNames: inputJobNames})

    expect(actualJobNames).toEqual(['lightJob', 'recurrentJob'])
  })

  it('should take nothing when getJobsFilter times out', async () => {
    const actualJobNames = await getTakeableJobNames({
      jobs: jobsDefinition(),
      getJobsFilter: () => new Promise(() => {}),
      jobNames: inputJobNames,
      timeoutMs: 10,
    })

    expect(actualJobNames).toEqual([])
  })

  it('should not take a job whose canTakeJobs times out', async () => {
    const mockJobs = jobsDefinition()
    mockJobs.heavyJob.canTakeJobs = () => new Promise(() => {})

    const actualJobNames = await getTakeableJobNames({
      jobs: mockJobs,
      jobNames: inputJobNames,
      timeoutMs: 10,
    })

    expect(actualJobNames).toEqual(['lightJob', 'recurrentJob'])
  })

  it('should skip a timed out check without waiting while it is still pending', async () => {
    const mockJobs = jobsDefinition()
    let resolveHeavyCheck!: (value: boolean) => void
    let heavyJobChecks = 0
    mockJobs.heavyJob.canTakeJobs = () => {
      heavyJobChecks++
      if (heavyJobChecks > 1) return Promise.resolve(true)
      return new Promise(resolve => {
        resolveHeavyCheck = resolve
      })
    }
    const inputOptions = {
      jobs: mockJobs,
      jobNames: inputJobNames,
      timeoutMs: 10,
      pendingChecks: new Map(),
    }

    await getTakeableJobNames(inputOptions)
    const startedAt = Date.now()
    const actualJobNames = await getTakeableJobNames(inputOptions)
    const actualDurationMs = Date.now() - startedAt

    expect(actualJobNames).toEqual(['lightJob', 'recurrentJob'])
    expect(actualDurationMs).toBeLessThan(10)
    expect(heavyJobChecks).toBe(1)

    resolveHeavyCheck(true)
    await new Promise(resolve => setTimeout(resolve, 0))
    expect(await getTakeableJobNames(inputOptions)).toEqual(inputJobNames)
    expect(heavyJobChecks).toBe(2)
  })

  it('should not take a job whose canTakeJobs throws synchronously', async () => {
    const mockJobs = jobsDefinition()
    mockJobs.heavyJob.canTakeJobs = (() => {
      throw new Error('Load check failed')
    }) as any

    const actualJobNames = await getTakeableJobNames({jobs: mockJobs, jobNames: inputJobNames})

    expect(actualJobNames).toEqual(['lightJob', 'recurrentJob'])
  })

  it('should warn once about unknown job names in getJobsFilter', async () => {
    const warn = spyOn(logger, 'warn').mockImplementation(() => {})
    const inputUnknownJobName = `unknownJob${Date.now()}`

    for (let i = 0; i < 2; i++) {
      await getTakeableJobNames({
        jobs: jobsDefinition(),
        getJobsFilter: async () => ({deny: [inputUnknownJobName, 'heavyJob']}),
        jobNames: inputJobNames,
      })
    }

    const actualWarnings = warn.mock.calls.filter(([message]) =>
      String(message).includes(inputUnknownJobName),
    )
    warn.mockRestore()
    expect(actualWarnings).toHaveLength(1)
  })

  it('should not call canTakeJobs for jobs excluded by getJobsFilter', async () => {
    const mockJobs = jobsDefinition()
    let heavyJobChecks = 0
    mockJobs.heavyJob.canTakeJobs = async () => {
      heavyJobChecks++
      return true
    }

    const actualJobNames = await getTakeableJobNames({
      jobs: mockJobs,
      getJobsFilter: async () => ({deny: ['heavyJob']}),
      jobNames: inputJobNames,
    })

    expect(actualJobNames).toEqual(['lightJob', 'recurrentJob'])
    expect(heavyJobChecks).toBe(0)
  })
})

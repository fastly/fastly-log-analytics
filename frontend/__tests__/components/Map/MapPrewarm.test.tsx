import { render, waitFor } from '@testing-library/react'
import { expect, it, vi } from 'vitest'

import { maplibreMockFactory } from '../../helpers/maplibre-mock'
import { MapPrewarm } from '@/components/Map/MapPrewarm'

const { setWorkerUrl, constructedWithWorker } = vi.hoisted(() => ({
  setWorkerUrl: vi.fn(),
  constructedWithWorker: vi.fn(),
}))

vi.mock('maplibre-gl', () => {
  const mapMock = maplibreMockFactory()
  class PrewarmMap extends mapMock.Map {
    constructor(options: ConstructorParameters<typeof mapMock.Map>[0]) {
      constructedWithWorker(setWorkerUrl.mock.calls.at(-1)?.[0])
      super(options)
    }
  }
  return { ...mapMock, Map: PrewarmMap, setWorkerUrl }
})

it('configures the worker before prewarming without another map component present', async () => {
  const { unmount } = render(<MapPrewarm />)
  await waitFor(() => expect(constructedWithWorker).toHaveBeenCalledTimes(1))
  expect(constructedWithWorker).toHaveBeenCalledWith('/maplibre-gl-worker.mjs')
  unmount()
})

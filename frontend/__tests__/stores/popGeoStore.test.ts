/**
 * @vitest-environment jsdom
 *
 * popGeoStore — module-singleton map from PoP code to {city,region,country}.
 * Populated from bootstrap; read by PopLabel.
 */
import { beforeEach, describe, expect, it } from 'vitest'
import { usePopGeoStore } from '@/stores/popGeoStore'

beforeEach(() => {
  usePopGeoStore.setState({ map: {} })
})

describe('popGeoStore', () => {
  it('defaults to empty map', () => {
    expect(usePopGeoStore.getState().map).toEqual({})
  })

  it('setMap populates the PoP lookup', () => {
    const geo = {
      SJC: { city: 'San Jose', region: 'California', country: 'US' },
      AMS: { city: 'Amsterdam', region: 'North Holland', country: 'NL' },
    }
    usePopGeoStore.getState().setMap(geo)
    expect(usePopGeoStore.getState().map).toEqual(geo)
    expect(usePopGeoStore.getState().map.SJC.city).toBe('San Jose')
  })

  it('setMap replaces the entire map', () => {
    usePopGeoStore.getState().setMap({ SJC: { city: 'San Jose', region: 'CA', country: 'US' } })
    usePopGeoStore.getState().setMap({ AMS: { city: 'Amsterdam', region: 'NH', country: 'NL' } })
    expect(usePopGeoStore.getState().map).toEqual({
      AMS: { city: 'Amsterdam', region: 'NH', country: 'NL' },
    })
    expect(usePopGeoStore.getState().map.SJC).toBeUndefined()
  })
})

import { useEffect, useState } from 'react'
import useBaseUrl from '@docusaurus/useBaseUrl'

interface LayerColumn {
  name?: string
  dtype?: string
  friendly_name?: string
  data_unit?: string
  description?: string
  is_key?: boolean
  is_required?: boolean
  theme?: string
  preferred_bin_definition?: string
  default_value?: string | number | boolean | null
}

export interface LayerDefinition {
  layer_id?: string
  layer_name?: string
  layer_type?: string
  path?: string
  theme?: string
  description?: string
  source_url?: string
  columns?: LayerColumn[]
}

interface LayerDefinitionFile {
  authority_name?: string
  tool_schema_version?: string
  layers?: LayerDefinition[]
}

type State =
  | { status: 'idle' }
  | { status: 'loading' }
  | { status: 'success'; data: LayerDefinitionFile }
  | { status: 'error'; message: string }

// ---- IN-MEMORY CACHE (so only 1 fetch per JSON file) ----
const cache = new Map<string, LayerDefinitionFile>()

const normalizeSrc = (src: string) => (src.startsWith('/') ? src : `/${src}`)

export function useLayerDefinitions(src: string): State {
  const resolvedSrc = useBaseUrl(normalizeSrc(src))
  const [state, setState] = useState<State>({ status: 'idle' })

  useEffect(() => {
    let subscribed = true

    async function load() {
      // Serve from cache immediately
      if (cache.has(resolvedSrc)) {
        if (subscribed) {
          setState({ status: 'success', data: cache.get(resolvedSrc)! })
        }
        return
      }

      setState({ status: 'loading' })

      try {
        type FetchLikeResponse = {
          ok: boolean
          status: number
          json: () => Promise<unknown>
        }

        type FetchLike = (input: string) => Promise<FetchLikeResponse>

        const fetchFn = (globalThis as { fetch?: FetchLike }).fetch
        if (typeof fetchFn !== 'function') {
          throw new Error('Global fetch unavailable')
        }

        const response = await fetchFn(resolvedSrc)
        if (!response.ok) {
          throw new Error(`Failed with status ${response.status}`)
        }

        const json = (await response.json()) as LayerDefinitionFile

        cache.set(resolvedSrc, json)

        if (subscribed) {
          setState({ status: 'success', data: json })
        }
      } catch (err) {
        const msg = err instanceof Error ? err.message : 'Unknown error'
        if (subscribed) {
          setState({ status: 'error', message: msg })
        }
      }
    }

    load()

    return () => {
      subscribed = false
    }
  }, [resolvedSrc])

  return state
}

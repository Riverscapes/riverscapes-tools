import React, { useEffect, useState } from 'react'
import useBaseUrl from '@docusaurus/useBaseUrl'

type LayerColumn = {
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

type LayerDefinition = {
  layer_id?: string
  layer_name?: string
  layer_type?: string
  path?: string
  theme?: string
  description?: string
  columns?: LayerColumn[]
}

type LayerDefinitionFile = {
  authority_name?: string
  tool_schema_version?: string
  layers?: LayerDefinition[]
}

type FetchState =
  | { status: 'idle' }
  | { status: 'loading' }
  | { status: 'success'; layers: LayerDefinition[] }
  | { status: 'error'; message: string }

type Props = {
  src: string
  title?: string
  showDescription?: boolean
}

const normalizeSrc = (src: string) => (src.startsWith('/') ? src : `/${src}`)

const emptyLayers: LayerDefinition[] = []

export default function LayerDefinitionTable({ src, title, showDescription = true }: Props) {
  const resolvedSrc = useBaseUrl(normalizeSrc(src))
  const [state, setState] = useState<FetchState>({ status: 'idle' })

  useEffect(() => {
    let isSubscribed = true
    setState({ status: 'loading' })

    const load = async () => {
      try {
        type FetchLikeResponse = {
          ok: boolean
          status: number
          json: () => Promise<unknown>
        }

        type FetchLike = (input: string) => Promise<FetchLikeResponse>

        const fetchFn = (globalThis as { fetch?: FetchLike }).fetch

        if (typeof fetchFn !== 'function') {
          throw new Error('Global fetch is unavailable in this environment')
        }

        const response = await fetchFn(resolvedSrc)
        if (!response.ok) {
          throw new Error(`Request failed with status ${response.status}`)
        }
        const payload = (await response.json()) as LayerDefinitionFile
        const layers = Array.isArray(payload.layers) ? payload.layers : emptyLayers
        if (isSubscribed) {
          setState({ status: 'success', layers })
        }
      } catch (error) {
        const message = error instanceof Error ? error.message : 'Unknown error'
        if (isSubscribed) {
          setState({ status: 'error', message })
        }
      }
    }

    load()

    return () => {
      isSubscribed = false
    }
  }, [resolvedSrc])

  if (state.status === 'loading' || state.status === 'idle') {
    return <p>Loading layer definitions…</p>
  }

  if (state.status === 'error') {
    return (
      <div role="alert">
        <strong>Unable to load layer definitions:</strong> {state.message}
      </div>
    )
  }

  const layers = state.layers ?? emptyLayers

  if (layers.length === 0) {
    return <p>No layers found in definition file.</p>
  }

  return (
    <div className="layer-definition-table">
      {title && <h3>{title}</h3>}
      <div className="table-responsive">
        <table>
          <thead>
            <tr>
              <th scope="col">ID</th>
              <th scope="col">Name</th>
              <th scope="col">Type</th>
              <th scope="col">Theme</th>
              <th scope="col">Path</th>
              {showDescription && <th scope="col">Description</th>}
            </tr>
          </thead>
          <tbody>
            {layers.map((layer, index) => (
              <tr key={layer.layer_id ?? `${layer.layer_name}-${index}`}>
                <td>{layer.layer_id ?? '—'}</td>
                <td>{layer.layer_name ?? '—'}</td>
                <td>{layer.layer_type ?? '—'}</td>
                <td>{layer.theme ?? '—'}</td>
                <td>{layer.path ? <code>{layer.path}</code> : <span aria-label="Path unavailable">—</span>}</td>
                {showDescription && <td>{layer.description ?? '—'}</td>}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}

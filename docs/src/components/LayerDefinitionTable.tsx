import React, { useEffect, useState } from 'react'
import useBaseUrl from '@docusaurus/useBaseUrl'
import Heading from '@theme/Heading'

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

interface LayerDefinition {
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

type FetchState =
  | { status: 'idle' }
  | { status: 'loading' }
  | { status: 'success'; layers: LayerDefinition[] }
  | { status: 'error'; message: string }

interface LayerDefinitionTableProps {
  src: string
  title?: string
  showDescription?: boolean
  theme?: string
}

const normalizeSrc = (src: string) => (src.startsWith('/') ? src : `/${src}`)

const emptyLayers: LayerDefinition[] = []

export const LayerDefinitionTable: React.FC<LayerDefinitionTableProps> = ({
  src,
  title,
  showDescription = true,
  theme,
}) => {
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

  let layers = state.layers ?? emptyLayers

  // Filter if a theme was provided
  if (theme) {
    layers = layers.filter((layer) => layer.theme === theme)
  }

  if (layers.length === 0) {
    return <p>No layers found in definition file.</p>
  }
  // const hasSourceUrl = layers.some((layer) => layer.source_url)
  return (
    <div className="layer-definition-table">
      {title && <Heading as="h3">{title}</Heading>}
      <div className="table-responsive">
        <table>
          <thead>
            <tr>
              <th scope="col">Layer</th>
              <th scope="col">Name</th>
              <th scope="col">Type</th>
              <th scope="col">Path</th>
              <th scope="col">Source</th>
              {showDescription && <th scope="col">Description</th>}
            </tr>
          </thead>
          <tbody>
            {layers.map((layer, index) => (
              <tr key={layer.layer_id ?? `${layer.layer_name}-${index}`}>
                <td>{layer.layer_id ?? '—'}</td>
                <td>{layer.layer_name ?? '—'}</td>
                <td>{layer.layer_type ?? '—'}</td>
                <td>{layer.path ? <code>{layer.path}</code> : <span aria-label="Path unavailable">—</span>}</td>
                <td>
                  {layer.source_url ? (
                    <a href={layer.source_url} target="_blank" rel="noopener noreferrer">
                      source
                    </a>
                  ) : (
                    '—'
                  )}
                </td>
                {showDescription && <td>{layer.description ?? '—'}</td>}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}

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
  | { status: 'success'; layer: LayerDefinition | null }
  | { status: 'error'; message: string }

interface LayerColumnsTableProps {
  src: string
  layerId: string
  title?: string
}

const normalizeSrc = (src: string) => (src.startsWith('/') ? src : `/${src}`)

export const LayerColumnsTable: React.FC<LayerColumnsTableProps> = ({ src, layerId, title }) => {
  const resolvedSrc = useBaseUrl(normalizeSrc(src))
  const [state, setState] = useState<FetchState>({ status: 'idle' })

  useEffect(() => {
    let subscribed = true
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
          throw new Error('Global fetch unavailable')
        }

        const response = await fetchFn(resolvedSrc)
        if (!response.ok) {
          throw new Error(`Failed with status ${response.status}`)
        }

        const json = (await response.json()) as LayerDefinitionFile
        const layers = Array.isArray(json.layers) ? json.layers : []

        const targetLayer = layers.find((layer) => layer.layer_id === layerId) ?? null

        if (subscribed) {
          setState({ status: 'success', layer: targetLayer })
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
  }, [resolvedSrc, layerId])

  // Loading & error states
  if (state.status === 'idle' || state.status === 'loading') {
    return <p>Loading column definitions…</p>
  }

  if (state.status === 'error') {
    return (
      <div role="alert">
        <strong>Error loading columns: </strong> {state.message}
      </div>
    )
  }

  const layer = state.layer
  if (!layer) {
    return <p>No layer found with ID.</p>
  }

  const columns = layer.columns ?? []

  if (columns.length === 0) {
    return <p>Layer has no column metadata.</p>
  }

  return (
    <div className="layer-columns-table" style={{ marginTop: '1.5rem' }}>
      {title && <Heading as="h3">{title}</Heading>}

      <div className="table-responsive">
        <table>
          <thead>
            <tr>
              <th>Column ID</th>
              <th>Friendly Name</th>
              <th>Units</th>
              <th>Description</th>
            </tr>
          </thead>
          <tbody>
            {columns.map((col, idx) => (
              <tr key={col.name ?? idx}>
                <td>{col.name ?? '—'}</td>
                <td>{col.friendly_name ?? '—'}</td>
                <td>{col.data_unit ?? '—'}</td>
                <td>{col.description ?? '—'}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}

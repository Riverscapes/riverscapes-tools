import React, { useEffect, useState } from 'react'
import useBaseUrl from '@docusaurus/useBaseUrl'
import LayerColumnsTable from './LayerColumnsTable'
import Heading from '@theme/Heading'

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
  columns?: LayerColumn[]
}

type LayerDefinitionFile = {
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
}

const normalizeSrc = (src: string) => (src.startsWith('/') ? src : `/${src}`)

export default function AllLayerColumnsTable({ src, title }: Props) {
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

        if (subscribed) {
          setState({ status: 'success', layers })
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

  // Loading & error states
  if (state.status === 'idle' || state.status === 'loading') {
    return <p>Loading column metadata…</p>
  }

  if (state.status === 'error') {
    return (
      <div role="alert">
        <strong>Error loading columns: </strong> {state.message}
      </div>
    )
  }

  const layers = state.layers ?? []

  // Only layers with columns
  const layersWithColumns = layers.filter((layer) => Array.isArray(layer.columns) && layer.columns.length > 0)

  if (layersWithColumns.length === 0) {
    return <p>No layers contain column metadata.</p>
  }

  return (
    <div className="all-layer-columns-table">
      {title && <Heading as="h2">{title}</Heading>}

      {layersWithColumns.map((layer, index) => (
        <section key={layer.layer_id ?? index} style={{ marginBottom: '2rem' }}>
          <h3>{layer.layer_name ?? layer.layer_id}</h3>

          {/* Inner component */}
          <LayerColumnsTable src={src} layerId={layer.layer_id ?? ''} title="Attributes" />
        </section>
      ))}
    </div>
  )
}

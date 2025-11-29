import React from 'react'
import Heading from '@theme/Heading'
import { useLayerDefinitions } from '../hooks/useLayerDefinitions'
import { LayerColumnsTable } from './LayerColumnsTable'

interface AllLayerColumnsTableProps {
  src: string
  title?: string
}

export const AllLayerColumnsTable: React.FC<AllLayerColumnsTableProps> = ({ src, title }) => {
  const state = useLayerDefinitions(src)

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

  const layers = state.data.layers ?? []

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
          <Heading as="h3">{layer.layer_name ?? layer.layer_id}</Heading>
          <LayerColumnsTable src={src} layerId={layer.layer_id ?? ''} title="Attributes" />
        </section>
      ))}
    </div>
  )
}

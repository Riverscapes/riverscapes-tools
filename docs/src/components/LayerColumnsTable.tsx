import React from 'react'
import Heading from '@theme/Heading'
import { useLayerDefinitions } from '../hooks/useLayerDefinitions'

interface LayerColumnsTableProps {
  src: string
  layerId: string
  title?: string
}

export const LayerColumnsTable: React.FC<LayerColumnsTableProps> = ({ src, layerId, title }) => {
  const state = useLayerDefinitions(src)

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

  const layers = state.data.layers ?? []
  const layer = layers.find((layer) => layer.layer_id === layerId) ?? null

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

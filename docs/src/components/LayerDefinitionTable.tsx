import React from 'react'
import Heading from '@theme/Heading'
import { useLayerDefinitions, LayerDefinition } from '../hooks/useLayerDefinitions'

interface LayerDefinitionTableProps {
  src: string
  title?: string
  showDescription?: boolean
  theme?: string
}

const emptyLayers: LayerDefinition[] = []

export const LayerDefinitionTable: React.FC<LayerDefinitionTableProps> = ({
  src,
  title,
  showDescription = true,
  theme,
}) => {
  const state = useLayerDefinitions(src)

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

  let layers = state.data.layers ?? emptyLayers

  // Filter if a theme was provided
  if (theme) {
    layers = layers.filter((layer) => layer.theme === theme)
  }

  if (layers.length === 0) {
    return <p>No layers found in definition file.</p>
  }

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

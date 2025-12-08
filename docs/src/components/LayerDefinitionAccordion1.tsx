import React, { useState } from 'react'
import Heading from '@theme/Heading'
import { useLayerDefinitions, LayerDefinition } from '../hooks/useLayerDefinitions'

interface LayerDefinitionTableProps {
  src: string
  title?: string
  showDescription?: boolean
  theme?: string
}

const emptyLayers: LayerDefinition[] = []

export const LayerDefinitionAccordion1: React.FC<LayerDefinitionTableProps> = ({
  src,
  title,
  showDescription = true,
  theme,
}) => {
  const state = useLayerDefinitions(src)

  const [openIndexes, setOpenIndexes] = useState<Set<number>>(new Set())

  if (state.status === 'idle' || state.status === 'loading') {
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

  if (theme) {
    layers = layers.filter((layer) => layer.theme === theme)
  }

  if (layers.length === 0) {
    return <p>No layers found in definition file.</p>
  }

  const handleToggle = (idx: number) => {
    setOpenIndexes((prev) => {
      const next = new Set(prev)
      if (next.has(idx)) {
        next.delete(idx)
      } else {
        next.add(idx)
      }
      return next
    })
  }

  const allOpen = openIndexes.size === layers.length

  const toggleAll = () => {
    if (allOpen) {
      setOpenIndexes(new Set())
    } else {
      setOpenIndexes(new Set(layers.map((_, idx) => idx)))
    }
  }

  return (
    <div className="layer-definition-list">
      {title && <Heading as="h3">{title}</Heading>}

      <div style={{ marginBottom: '1rem' }}>
        <button onClick={toggleAll}>{allOpen ? 'Collapse All' : 'Expand All'}</button>
      </div>

      <div style={{ display: 'flex', flexDirection: 'column', gap: '1rem', width: '100%' }}>
        {layers.map((layer, index) => (
          <details
            key={layer.layer_id ?? `${layer.layer_name}-${index}`}
            open={openIndexes.has(index)}
            onClick={(e) => {
              // Prevent summary click from toggling twice
              e.preventDefault()
              handleToggle(index)
            }}
            style={{
              width: '100%',
              borderBottom: '1px solid #eee',
              paddingBottom: '0.75rem',
              cursor: 'pointer',
            }}
          >
            <summary style={{ fontWeight: 500, minWidth: 180, userSelect: 'none' }}>{layer.layer_name ?? '—'}</summary>
            <div style={{ marginTop: '0.5rem', marginLeft: '2rem' }}>
              <p>
                <strong>Layer ID:</strong> {layer.layer_id ?? '—'}
              </p>
              <p>
                <strong>Type:</strong> {layer.layer_type ?? '—'}
              </p>
              <p>
                <strong>Path:</strong> {layer.path ? <code>{layer.path}</code> : '—'}
              </p>
              <p>
                <strong>Source:</strong>{' '}
                {layer.source_url ? (
                  <a href={layer.source_url} target="_blank" rel="noopener noreferrer">
                    {layer.source_url}
                  </a>
                ) : (
                  '—'
                )}
              </p>
              {showDescription && (
                <p>
                  <strong>Description:</strong> {layer.description ?? '—'}
                </p>
              )}
            </div>
          </details>
        ))}
      </div>
    </div>
  )
}

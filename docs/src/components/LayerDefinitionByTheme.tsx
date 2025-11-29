import React from 'react'
import { useLayerDefinitions } from '../hooks/useLayerDefinitions'
import { LayerDefinitionTable } from './LayerDefinitionTable'
import Heading from '@theme/Heading'

interface LayerDefinitionByThemeProps {
  src: string
  title?: string
}

export const LayerDefinitionByTheme: React.FC<LayerDefinitionByThemeProps> = ({ src, title }) => {
  const state = useLayerDefinitions(src)

  if (state.status === 'loading' || state.status === 'idle') {
    return <p>Loading layer themes…</p>
  }

  if (state.status === 'error') {
    return (
      <div role="alert">
        <strong>Unable to load layer themes:</strong> {state.message}
      </div>
    )
  }

  const layers = state.data.layers ?? []
  const themes = Array.from(new Set(layers.map((l) => l.theme).filter((t): t is string => Boolean(t))))

  return (
    <div className="layer-definition-by-theme">
      {title && <Heading as="h2">{title}</Heading>}

      {themes.map((theme) => (
        <section key={theme} style={{ marginBottom: '2rem' }}>
          <Heading as="h3">{theme}</Heading>
          <LayerDefinitionTable src={src} theme={theme} showDescription={true} />
        </section>
      ))}
    </div>
  )
}

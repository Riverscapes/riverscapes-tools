import React, { useEffect, useState } from 'react'
import useBaseUrl from '@docusaurus/useBaseUrl'
import { LayerDefinitionTable } from './LayerDefinitionTable'
import Heading from '@theme/Heading'

interface LayerDefinition {
  layer_id?: string
  layer_name?: string
  layer_type?: string
  path?: string
  theme?: string
  description?: string
}

interface LayerDefinitionFile {
  authority_name?: string
  tool_schema_version?: string
  layers?: LayerDefinition[]
}

type FetchState =
  | { status: 'idle' }
  | { status: 'loading' }
  | { status: 'success'; themes: string[] }
  | { status: 'error'; message: string }

interface LayerDefinitionByThemeProps {
  src: string
  title?: string
}

const normalizeSrc = (src: string) => (src.startsWith('/') ? src : `/${src}`)

export const LayerDefinitionByTheme: React.FC<LayerDefinitionByThemeProps> = ({ src, title }) => {
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
        const layers = Array.isArray(payload.layers) ? payload.layers : []

        const themes = Array.from(new Set(layers.map((l) => l.theme).filter((t): t is string => Boolean(t))))

        if (isSubscribed) {
          setState({ status: 'success', themes })
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
    return <p>Loading layer themes…</p>
  }

  if (state.status === 'error') {
    return (
      <div role="alert">
        <strong>Unable to load layer themes:</strong> {state.message}
      </div>
    )
  }

  const { themes } = state

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

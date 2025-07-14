import React from 'react'
import styles from './styles.module.css'
// import useBaseUrl from '@docusaurus/useBaseUrl'

export default function HomepageFeatures() {
  return (
    <div className={styles.container}>
      <section title="Home" className={styles.intro}>
        <p>
          This is a template site for developing the Riverscapes Docusaurus theme. The content on this site should be
          considered a placeholder only to test the theme and layout of the website. If you want the Riverscapes
          Developer Documentation please use one of the links below.
        </p>
      </section>

      <Section title="Other Riverscapes Sites">
        <CardGrid>
          <ResourceCard
            title="Riverscapes Consortium"
            description="The Riverscapes Consortium main site."
            link="https://riverscapes.net"
          />
          <ResourceCard
            title="Our Tools"
            description="Learn about each of our Riverscapes compliant tools."
            link="https://tools.riverscapes.net/"
          />
          <ResourceCard
            title="Data Exchange"
            description="Discover and download Riverscapes compliant data."
            link="https://data.riverscapes.net/"
          />
        </CardGrid>
      </Section>

      <Section title="Sub-pages of this site:">
        <CardGrid>
          <ResourceCard
            title="Standards & Compliance"
            description="Learn about riverscapes standards and how to make your tools and data compliant."
            link="standards"
          />
          <ResourceCard
            title="Riverscapes API"
            description="Learn how to use the Riverscapes API to access data."
            link="dev-tools/api"
          />
          <ResourceCard
            title="Documentation"
            description="Resources to build Riverscapes documentation and websites."
            link="documentation/documentation-websites"
          />
        </CardGrid>
      </Section>
    </div>
  )
}

function Section({ title, children }) {
  return (
    <div className={styles.section}>
      <h2>{title}</h2>
      {children}
    </div>
  )
}

function CardGrid({ children }) {
  return <div className={styles.grid}>{children}</div>
}

function ResourceCard({ title, description, link }) {
  return (
    <a href={link} className={styles.card}>
      {/* <img src={useBaseUrl('/img/card-image.jpg')} alt={title} className={styles.cardImage} /> */}
      <div className={styles.cardContent}>
        <h3>{title}</h3>
        <p>{description}</p>
      </div>
    </a>
  )
}

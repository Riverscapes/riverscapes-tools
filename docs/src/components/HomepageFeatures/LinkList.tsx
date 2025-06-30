import React from 'react'
import useDocusaurusContext from '@docusaurus/useDocusaurusContext'

export default function CustomComponent() {
  const { siteConfig } = useDocusaurusContext()
  console.log('siteConfig', siteConfig) 

  // Ensure links is typed as an array of strings
  const links = siteConfig.customFields.links as string[] | undefined;

  return (
    <ul>
      {links && links.map((link, index) => (
        <li key={index}>
          <a href={link} target="_blank" rel="noopener noreferrer">
            {link}
          </a>
        </li>
      ))}
    </ul>
  )
}

import { themes as prismThemes } from 'prism-react-renderer'
import type { Config } from '@docusaurus/types'
import type * as Preset from '@docusaurus/preset-classic'

// This runs in Node.js - Don't use client-side code here (browser APIs, JSX...)

const config: Config = {
  title: 'Riverscapes Tools', // Site title displayed in the browser tab
  tagline: 'Homepage for riverscapes tools documentation', // Short description shown in meta tags
  favicon: 'favicon.ico', // Path to site favicon

  future: {
    v4: true, // Enables compatibility with upcoming Docusaurus v4 features
  },

  url: 'https://tools.riverscapes.net', // The base URL of your site (no trailing slash)
  baseUrl: '/', // The sub-path where your site is served (used in GitHub Pages)

  // GitHub pages deployment config
  organizationName: 'Riverscapes', // GitHub org/user name
  projectName: 'riverscapes-tools', // GitHub repo name

  onBrokenLinks: 'throw', // Throw an error on broken links
  onBrokenMarkdownLinks: 'warn', // Warn instead of throwing for broken markdown links

  i18n: {
    defaultLocale: 'en', // Default language
    locales: ['en'], // Supported languages
  },

  themes: ['@riverscapes/docusaurus-theme'], // Shared custom theme used across sites

  presets: [
    [
      'classic', // Docusaurus classic preset for docs/blog
      {
        docs: {
          sidebarPath: './sidebars.ts', // Path to sidebar config
          routeBasePath: '/', // Serve docs at site root
          editUrl: 'https://github.com/Riverscapes/riverscapes-tools/tree/master/docs/sites/template', // "Edit this page" link
        },
      } satisfies Preset.Options,
    ],
  ],

  themeConfig: {
    image: 'img/logo.png', // Social sharing image

    navbar: {
      title: 'Riverscapes Tools', // Navbar title
      logo: {
        alt: 'Riverscapes Tools Logo', // Logo alt text
        src: 'img/logo.png', // Logo image path
      },
      items: [
        { to: '/anthro/', label: 'Anthro', position: 'left' },
        { to: '/brat/', label: 'BRAT', position: 'left' },
        { to: '/channelarea/', label: 'Channel Area', position: 'left' },
        { to: '/confinement/', label: 'Confinement', position: 'left' },
        { to: '/hydro/', label: 'Hydro', position: 'left' },
        { to: '/rcat/', label: 'RCAT', position: 'left' },
        { to: '/rme/', label: 'RME', position: 'left' },
        { to: '/rscontext/', label: 'RS Context', position: 'left' },
        { to: '/taudem/', label: 'TauDEM', position: 'left' },
        { to: '/vbet/', label: 'VBET', position: 'left' },
        { to: '/downloading-data', label: 'Download Data', position: 'left' },
        { to: '/viewing-projects', label: 'View Projects', position: 'left' },
        {
          href: 'https://github.com/Riverscapes/riverscapes-tools', // External GitHub link
          label: 'GitHub',
          position: 'right',
        },
      ],
    },

    prism: {
      theme: prismThemes.github, // Code block theme for light mode
      darkTheme: prismThemes.dracula, // Code block theme for dark mode
    },
  } satisfies Preset.ThemeConfig,
}

export default config

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
        gtag: {
          trackingID: 'G-HY6Z6ZF1FM',
          anonymizeIP: true, // Anonymize IP addresses for privacy
        },
        docs: {
          sidebarPath: './sidebars.ts', // Path to sidebar config
          routeBasePath: '/', // Serve docs at site root
          editUrl: 'https://github.com/Riverscapes/riverscapes-tools/tree/docs/docs/', // "Edit this page" link
        },
      } satisfies Preset.Options,
    ],
  ],

  themeConfig: {
    image: 'img/logo.png', // Social sharing image

    algolia: {
      // The application ID provided by Algolia
      appId: '4TGS8ZPIMY',

      // Public API key: it is safe to commit it
      apiKey: 'd084a7919fe7b5940d7125f14221eaca',

      indexName: 'tools.riverscapes.net',

      // Optional: see doc section below
      contextualSearch: true,

      // Optional: Specify domains where the navigation should occur through window.location instead on history.push. Useful when our Algolia config crawls multiple documentation sites and we want to navigate with window.location.href to them.
      // externalUrlRegex: "external\\.com|domain\\.com",

      // Optional: Replace parts of the item URLs from Algolia. Useful when using the same search index for multiple deployments using a different baseUrl. You can use regexp or string in the `from` param. For example: localhost:3000 vs myCompany.com/docs
      // replaceSearchResultPathname: {
      //   from: "/docs/", // or as RegExp: /\/docs\//
      //   to: "/",
      // },

      // Optional: Algolia search parameters
      // searchParameters: {},

      // Optional: path for search page that enabled by default (`false` to disable it)
      // searchPagePath: "search",

      // Optional: whether the insights feature is enabled or not on Docsearch (`false` by default)
      // insights: false,

      //... other Algolia params
    },

    navbar: {
      title: 'Riverscapes Tools', // Navbar title
      logo: {
        alt: 'Riverscapes Tools Logo', // Logo alt text
        src: 'img/logo.png', // Logo image path
      },
      items: [
        // {
        //   to: 'none',
        //   label: 'Production Grade',
        //   position: 'left',
        //   items: [
        //     { to: '/anthro/', label: 'Anthro'  },
        //     { to: '/brat/', label: 'BRAT' },
        //     { to: '/channelarea/', label: 'Channel Area' },
        //     { to: '/confinement/', label: 'Confinement' },
        //     { to: '/hydro/', label: 'Hydro'},
        //     { to: '/rcat/', label: 'RCAT'},
        //     { to: '/rme/', label: 'RME' },
        //     { to: '/rscontext/', label: 'RS Context'},
        //     { to: '/taudem/', label: 'TauDEM' },
        //     { to: '/vbet/', label: 'VBET'},
        // ]},
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

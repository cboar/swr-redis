import { defineConfig } from 'astro/config'
import starlight from '@astrojs/starlight'

export default defineConfig({
  site: 'https://cboar.github.io',
  base: '/swr-redis',
  trailingSlash: 'always',
  vite: {
    tsconfig: './tsconfig.json',
  },
  integrations: [
    starlight({
      title: 'swr-redis',
      description:
        'Stale-while-revalidate caching in Redis for Node.js, with typed caches and distributed fetch locking.',
      social: [
        { icon: 'github', label: 'GitHub', href: 'https://github.com/cboar/swr-redis' },
        { icon: 'npm', label: 'npm', href: 'https://www.npmjs.com/package/swr-redis' },
      ],
      editLink: {
        baseUrl: 'https://github.com/cboar/swr-redis/edit/main/docs/',
      },
      sidebar: [
        { label: 'Getting started', slug: 'index' },
        { label: 'Configuration', slug: 'configuration' },
        { label: 'Fetch results', slug: 'fetch-results' },
        { label: 'Pagination', slug: 'pagination' },
        { label: 'Consistency & failures', slug: 'consistency' },
        { label: 'Observability & errors', slug: 'observability' },
      ],
    }),
  ],
})

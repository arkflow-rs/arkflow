/*
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

import {themes as prismThemes} from 'prism-react-renderer';
import type {Config} from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';

// This runs in Node.js - Don't use client-side code here (browser APIs, JSX...)

const config: Config = {
  title: 'ArkFlow',
  tagline: 'High-performance Rust stream processing engine',
  favicon: 'img/favicon.svg',

  // Set the production url of your site here
  url: 'https://arkflow-rs.com',
  // Set the /<baseUrl>/ pathname under which your site is served
  // For GitHub pages deployment, it is often '/<projectName>/'
  baseUrl: '/',

  // GitHub pages deployment config.
  // If you aren't using GitHub pages, you don't need these.
  organizationName: 'arkflow-rs', // Usually your GitHub org/user name.
  projectName: 'arkflow', // Usually your repo name.

  onBrokenLinks: 'throw',
  onBrokenAnchors: 'throw',

  markdown: {
    mermaid: true,
    hooks: {
      onBrokenMarkdownLinks: 'throw',
    },
  },

  // Even if you don't use internationalization, you can use this field to set
  // useful metadata like html lang. For example, if your site is Chinese, you
  // may want to replace "en" with "zh-Hans".
  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  presets: [
    [
      'classic',
      {
        docs: {
          sidebarPath: './sidebars.ts',
          // The maintained (unversioned) tree is the primary docs served at
          // /docs/; released snapshots hang off /docs/0.5.x etc. and are
          // switched via the version dropdown. Version policy:
          // docs/DOCUMENTATION.md.
          lastVersion: 'current',
          versions: {
            current: {
              label: 'Next',
              banner: 'none',
            },
          },
          // Please change this to your repo.
          // Remove this to remove the "edit this page" links.
          editUrl:
            'https://github.com/arkflow-rs/arkflow/tree/main/docs/',
        },
        blog: {
          showReadingTime: true,
          feedOptions: {
            type: ['rss', 'atom'],
            xslt: true,
          },
          // Please change this to your repo.
          // Remove this to remove the "edit this page" links.
          editUrl:
            'https://github.com/arkflow-rs/arkflow/tree/main/docs/',
          // Useful options to enforce blogging best practices
          onInlineTags: 'warn',
          onInlineAuthors: 'warn',
          onUntruncatedBlogPosts: 'ignore',
        },
        theme: {
          customCss: './src/css/custom.css',
        },
      } satisfies Preset.Options,
    ],
  ],

  themeConfig: {
    // Replace with your project's social card
    image: 'img/arkflow.svg',
    announcementBar: {
      id: 'unified-kernel',
      content:
        '✨ <b>New architecture</b>: a unified streaming job runtime — kernel rebuild, durability, control plane, and web console. <a href="/docs/build/jobs">See what changed</a>',
      backgroundColor: '#1e3a8a',
      textColor: '#ffffff',
      isCloseable: true,
    },
    navbar: {
      title: 'ArkFlow',
      hideOnScroll: true,
      logo: {
        alt: 'ArkFlow',
        src: 'img/logo.svg',
      },
      items: [
        {
          type: 'docSidebar',
          sidebarId: 'tutorialSidebar',
          position: 'left',
          label: 'Docs',
        },
        {to: '/docs/sql', label: 'SQL', position: 'left'},
        {to: '/docs/reference/api', label: 'API', position: 'left'},
        {to: '/blog', label: 'Blog', position: 'left'},
        {
          type: 'search',
          position: 'right',
        },
        {
          type: 'docsVersionDropdown',
          position: 'right',
        },
        {
          href: 'https://github.com/arkflow-rs/arkflow',
          position: 'right',
          className: 'header-github-link',
          'aria-label': 'GitHub repository',
        },
      ],
    },
    footer: {
      style: 'dark',
      links: [
        {
          title: 'Learn',
          items: [
            {label: 'Get started', to: '/docs/get-started/install'},
            {label: 'Quickstart', to: '/docs/get-started/quickstart'},
            {label: 'Architecture', to: '/docs/build/architecture'},
            {label: 'Streaming jobs', to: '/docs/build/jobs'},
          ],
        },
        {
          title: 'Build',
          items: [
            {label: 'Components', to: '/docs/components'},
            {label: 'SQL reference', to: '/docs/sql'},
            {label: 'Recipes', to: '/docs/build/recipes'},
            {label: 'Examples', to: '/docs/reference/examples'},
          ],
        },
        {
          title: 'Operate',
          items: [
            {label: 'Kubernetes', to: '/docs/operate/kubernetes'},
            {label: 'Control plane', to: '/docs/operate/control-plane/overview'},
            {label: 'Web console', to: '/docs/operate/control-plane/console'},
            {label: 'Recovery runbook', to: '/docs/operate/recovery'},
          ],
        },
        {
          title: 'Reference',
          items: [
            {label: 'CLI', to: '/docs/reference/cli'},
            {label: 'HTTP API', to: '/docs/reference/api'},
            {label: 'Configuration', to: '/docs/reference/configuration'},
            {label: 'Compatibility', to: '/docs/reference/compatibility'},
          ],
        },
        {
          title: 'Community',
          items: [
            {label: 'GitHub', href: 'https://github.com/arkflow-rs/arkflow'},
            {label: 'Discord', href: 'https://discord.gg/CwKhzb8pux'},
            {label: 'Blog', to: '/blog'},
          ],
        },
      ],
      copyright: `Copyright © ${new Date().getFullYear()} ArkFlow. Built with Docusaurus.`,
    },
    prism: {
      theme: prismThemes.github,
      darkTheme: prismThemes.dracula,
      additionalLanguages: ['rust', 'http'],
    },
    mermaid: {
      theme: {
        light: 'neutral',
        dark: 'dark',
      },
      // Natural-width diagrams stay legible on phones; custom.css gives
      // .mermaid a horizontal scroll container (landing-page arch-diagram UX).
      options: {
        flowchart: {useMaxWidth: false},
      },
    },
  } satisfies Preset.ThemeConfig,
  themes: [
    '@docusaurus/theme-mermaid',
    [
      '@easyops-cn/docusaurus-search-local',
      {
        // Docs + blog are indexed; the landing page stays out.
        indexDocs: true,
        indexBlog: true,
        indexPages: false,
        highlightSearchTermsOnTargetPage: true,
      },
    ],
  ],
  plugins: [
    function disableIncompatibleWebpackBar() {
      return {
        name: 'disable-incompatible-webpackbar',
        configureWebpack(config: any) {
          return {
            mergeStrategy: {plugins: 'replace'},
            plugins: config.plugins?.filter(
              (plugin) =>
                (plugin as {constructor?: {name?: string}})?.constructor?.name !==
                'WebpackBarPlugin',
            ),
          };
        },
      };
    },
  ],
};

export default config;

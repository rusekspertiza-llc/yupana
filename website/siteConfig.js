/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// See https://docusaurus.io/docs/site-config for all the possible
// site configuration options.

const repoUrl = "https://github.com/rusexpertiza-llc/yupana";
const gitterUrl = "https://gitter.im/rusexpertiza-llc/yupana";

// List of projects/orgs using your project for the users page.
const siteConfig = {
  title: 'Yupana', // Title for your website.
  tagline: 'Yupana — система анализа розничных продаж',
  url: 'https://docs.yupana.org', // Your website URL
  cname: 'docs.yupana.org',
  baseUrl: '/', // Base URL for your project */
  // For github.io type URLs, you would set the url and baseUrl like:
  //   url: 'https://facebook.github.io',
  //   baseUrl: '/test-site/',

  // Used for publishing and more
  projectName: 'yupana-docs',
  organizationName: 'rusexpertiza-llc',
  // For top-level user or org sites, the organization is still the same.
  // e.g., for the https://JoelMarcey.github.io site, it would be set like...
  //   organizationName: 'JoelMarcey'
    
  algolia: {
    apiKey: 'c9d8bb456d6608bb7e12f07fcdb49c2b',
    indexName: 'yupana'
  },

  // For no header links in the top nav bar -> headerLinks: [],
  headerLinks: [
    {doc: 'architecture', label: 'Документация'},
    {href: '/api/org/yupana/index.html', label: 'API'},
    {href: repoUrl, label: 'GitHub', external: true }
//    {doc: 'api', label: 'API'},
//    {page: 'help', label: 'Help'},
  ],

  separateCss: [ 'api'],


  /* path to images for header/footer */
  headerIcon: 'img/yupana-logo.png',
  footerIcon: 'img/yupana-logo.png',
  favicon: 'img/favicon.ico',

  /* Colors for website */
  colors: {
    primaryColor: '#004d4d',
    secondaryColor: '#00a1a',
  },

  customDocsPath: "website/target/docs",

  /* Custom fonts for website */
  /*
  fonts: {
    myFont: [
      "Times New Roman",
      "Serif"
    ],
    myOtherFont: [
      "-apple-system",
      "system-ui"
    ]
  },
  */

  // This copyright info is used in /core/Footer.js and blog RSS/Atom feeds.
  copyright: `Copyright © Yupana, Первый ОФД ${new Date().getFullYear()}`,

  highlight: {
    // Highlight.js theme to use for syntax highlighting in code blocks.
    theme: 'default',
  },

  // Add custom scripts here that would be placed in <script> tags.
  scripts: ['https://buttons.github.io/buttons.js'],

  // On page navigation for the current documentation page.
  onPageNav: 'separate',
  // No .html extensions for paths.
  cleanUrl: true,

  // Open Graph and Twitter card images.
  ogImage: 'img/undraw_online.svg',
  twitterImage: 'img/undraw_tweetstorm.svg',

  // For sites with a sizable amount of content, set collapsible to true.
  // Expand/collapse the links and subcategories under categories.
  // docsSideNavCollapsible: true,

  // Show documentation's last contributor's name.
  // enableUpdateBy: true,

  // Show documentation's last update time.
  // enableUpdateTime: true,

  // You may provide arbitrary config keys to be used as needed by your
  // template. For example, if you need your repo's URL...
  // repoUrl: 'https://github.com/facebook/test-site',

  repoUrl,
  gitterUrl
};

module.exports = siteConfig;
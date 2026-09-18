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

import type {ReactNode} from 'react';
import Link from '@docusaurus/Link';
import Translate, {translate} from '@docusaurus/Translate';
import Layout from '@theme/Layout';

// Swizzled from @docusaurus/theme-classic NotFound: keeps the raw error info
// while routing visitors to the places they most likely wanted.

function NotFoundContent(): ReactNode {
  return (
    <main className="container margin-vert--xl af-notfound">
      <div className="af-notfound__code" aria-hidden="true">
        404
      </div>
      <h1 className="af-notfound__title">
        <Translate id="theme.NotFound.title">Page not found</Translate>
      </h1>
      <p className="af-notfound__lead">
        <Translate id="theme.NotFound.lead">
          The page you are looking for may have been renamed or removed. Try
          the search box in the navbar, or start from one of these:
        </Translate>
      </p>
      <div className="af-notfound__links">
        <Link className="button button--primary" to="/docs">
          <Translate id="theme.NotFound.docsHome">Docs home</Translate>
        </Link>
        <Link
          className="button button--secondary"
          to="/docs/get-started/quickstart">
          <Translate id="theme.NotFound.quickstart">Quickstart</Translate>
        </Link>
        <Link className="button button--secondary" to="/docs/reference/api">
          <Translate id="theme.NotFound.api">HTTP API</Translate>
        </Link>
        <Link
          className="button button--secondary"
          href="https://github.com/arkflow-rs/arkflow/issues">
          <Translate id="theme.NotFound.issue">Report an issue</Translate>
        </Link>
      </div>
    </main>
  );
}

export default function NotFound(): ReactNode {
  return (
    <Layout title={translate({id: 'theme.NotFound.title'})}>
      <NotFoundContent />
    </Layout>
  );
}

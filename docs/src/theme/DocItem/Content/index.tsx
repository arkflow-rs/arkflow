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

import React, {type ReactNode} from 'react';
import Content from '@theme-original/DocItem/Content';
import type {WrapperProps} from '@docusaurus/types';
import type DocItemContentType from '@theme/DocItem/Content';
import {useDoc} from '@docusaurus/plugin-content-docs/client';
import PipelineHeader from '@site/src/components/PipelineHeader';

type Props = WrapperProps<typeof DocItemContentType>;

// Wrapping swizzle of DocItem/Content: pages whose front matter declares
// `components: [...]` (the CI-enforced component-reference convention)
// get a pipeline position header above the title. All stock content
// behavior is delegated to the original component.

export default function ContentWrapper(props: Props): ReactNode {
  const {frontMatter, metadata} = useDoc();
  const declared = (frontMatter as {components?: unknown}).components;
  const typeNames = Array.isArray(declared)
    ? declared.filter((entry): entry is string => typeof entry === 'string')
    : [];

  return (
    <>
      {typeNames.length > 0 && (
        <PipelineHeader
          typeNames={typeNames}
          sourceDirName={metadata.sourceDirName}
        />
      )}
      <Content {...props} />
    </>
  );
}

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

import React, {
  isValidElement,
  type ComponentPropsWithoutRef,
  type ReactNode,
} from 'react';
import Container from '@theme-original/CodeBlock/Container';

type Props = {as?: 'div' | 'pre'} & ComponentPropsWithoutRef<'div'>;

// Wrapping swizzle of CodeBlock/Container: forwards the fence's
// language as a data attribute and mounts the terminal-bar language
// micro-label on untitled blocks (titled blocks keep the stock title
// only). No code-block context hook here: Container also renders for
// JSX <pre> content outside the CodeBlock context provider, so the
// language is parsed from the className the MDX pipeline attaches.

function languageFromClassName(className?: string): string | undefined {
  return className
    ?.split(' ')
    .find((cls) => cls.startsWith('language-'))
    ?.slice('language-'.length);
}

function hasTitleChild(children: ReactNode): boolean {
  return React.Children.toArray(children).some((child) => {
    if (!isValidElement(child)) {
      return false;
    }
    const className: unknown = (child.props as {className?: unknown}).className;
    return typeof className === 'string' && className.startsWith('codeBlockTitle');
  });
}

export default function CodeBlockContainerWrapper(props: Props): ReactNode {
  const language = languageFromClassName(props.className);

  return (
    <Container
      {...props}
      {...(language ? {'data-language': language} : {})}>
      {language && !hasTitleChild(props.children) ? (
        <span className="af-code-lang" aria-hidden="true">
          {language}
        </span>
      ) : null}
      {props.children}
    </Container>
  );
}

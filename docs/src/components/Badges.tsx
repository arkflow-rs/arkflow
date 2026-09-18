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
import clsx from 'clsx';

// Decoration kit for the docs visual system. The styling lives in
// custom.css (.af-badge / .af-dot) so plain markdown hub pages can use
// the same classes as inline HTML without importing React components.

/** Monospace badge showing a registry type name, e.g. `input/kafka`. */
export function KindBadge({children}: {children?: ReactNode}) {
  return <span className="af-badge af-badge--kind">{children}</span>;
}

/** Badge for a pipeline feature flag, e.g. DURABLE / ATOMIC. */
export function FeatureBadge({children}: {children?: ReactNode}) {
  return <span className="af-badge af-badge--feature">{children}</span>;
}

/** Status light. Active dots glow with the terminal accent color. */
export function StatusDot({active = false}: {active?: boolean}): ReactNode {
  return (
    <span
      className={clsx('af-dot', active && 'af-dot--active')}
      aria-hidden="true"
    />
  );
}

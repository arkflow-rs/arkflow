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
import {KindBadge, StatusDot} from '@site/src/components/Badges';

// Pipeline position header for component reference pages. Derived
// entirely from the CI-enforced `components:` front matter (plus the
// page directory for bare type names), so new component pages are
// decorated with no per-page markup.

type Role = 'input' | 'buffer' | 'processor' | 'output' | 'temporary' | 'codec';

const KIND_BY_DIR: Record<string, Role> = {
  '0-inputs': 'input',
  '1-buffers': 'buffer',
  '2-processors': 'processor',
  '3-outputs': 'output',
  '4-temporary': 'temporary',
  '5-codecs': 'codec',
};

const ROLES: Role[] = [
  'input',
  'buffer',
  'processor',
  'output',
  'temporary',
  'codec',
];

const LINEAR_STAGES: Role[] = ['input', 'buffer', 'processor', 'output'];

function isRole(value: string): value is Role {
  return ROLES.includes(value as Role);
}

/** Kind prefix wins; otherwise the page's component directory decides. */
export function deriveRole(
  typeName: string,
  sourceDirName: string | undefined,
): Role | null {
  const prefix = typeName.split('/')[0];
  if (isRole(prefix)) return prefix;
  if (sourceDirName) {
    const segment = sourceDirName
      .split('/')
      .find((part) => part in KIND_BY_DIR);
    if (segment) return KIND_BY_DIR[segment];
  }
  return null;
}

function RoleChip({role}: {role: Role}): ReactNode {
  return (
    <span className="af-pipeline__stage af-pipeline__stage--active">
      <StatusDot active />
      {role.toUpperCase()}
    </span>
  );
}

/** Linear stages INPUT → BUFFER → PROCESSOR → OUTPUT, one highlighted. */
function RoleStrip({active}: {active: Role}): ReactNode {
  return (
    <span className="af-pipeline__strip">
      {LINEAR_STAGES.map((stage, index) => (
        <span key={stage} className="af-pipeline__cell">
          {index > 0 && (
            <span className="af-pipeline__link" aria-hidden="true" />
          )}
          <span
            className={clsx(
              'af-pipeline__stage',
              stage === active && 'af-pipeline__stage--active',
            )}>
            <StatusDot active={stage === active} />
            {stage.toUpperCase()}
          </span>
        </span>
      ))}
    </span>
  );
}

export default function PipelineHeader({
  typeNames,
  sourceDirName,
}: {
  typeNames: string[];
  sourceDirName: string | undefined;
}): ReactNode {
  const roles = [
    ...new Set(
      typeNames
        .map((name) => deriveRole(name, sourceDirName))
        .filter((role): role is Role => role !== null),
    ),
  ];
  if (roles.length === 0) return null;
  const role = roles[0];
  const linear = LINEAR_STAGES.includes(role);

  return (
    <div
      className="af-pipeline"
      role="group"
      aria-label={`Component pipeline position: ${roles.join(', ')}`}>
      <span className="af-pipeline__types">
        {typeNames.map((name) => (
          <span key={name} className="af-pipeline__types-item">
            <KindBadge>{name}</KindBadge>
          </span>
        ))}
      </span>
      {linear ? <RoleStrip active={role} /> : <RoleChip role={role} />}
    </div>
  );
}

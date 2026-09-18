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

import React, {useEffect, useRef, type ReactNode} from 'react';
import DocRoot from '@theme-original/DocRoot';
import type {WrapperProps} from '@docusaurus/types';
import type DocRootType from '@theme/DocRoot';

type Props = WrapperProps<typeof DocRootType>;

// Wrapping swizzle of DocRoot: renders a fixed reading-progress bar at
// the top of doc pages. The fill is a pure function of scroll position
// (no transitions), so `prefers-reduced-motion` users get instant
// updates. Hidden outside the [0, 1] reading range.

function ReadingProgress(): ReactNode {
  const barRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    let raf = 0;
    const update = () => {
      raf = 0;
      const el = barRef.current;
      if (!el) return;
      const scrollable =
        document.documentElement.scrollHeight - window.innerHeight;
      const fraction =
        scrollable > 0 ? Math.min(1, window.scrollY / scrollable) : 0;
      el.style.setProperty('--af-reading-progress', fraction.toFixed(4));
      el.setAttribute('data-visible', String(fraction > 0.004));
    };
    const schedule = () => {
      if (!raf) raf = requestAnimationFrame(update);
    };
    update();
    window.addEventListener('scroll', schedule, {passive: true});
    window.addEventListener('resize', schedule);
    return () => {
      window.removeEventListener('scroll', schedule);
      window.removeEventListener('resize', schedule);
      if (raf) cancelAnimationFrame(raf);
    };
  }, []);

  return <div ref={barRef} className="af-reading-progress" aria-hidden="true" />;
}

export default function DocRootWrapper(props: Props): ReactNode {
  return (
    <>
      <ReadingProgress />
      <DocRoot {...props} />
    </>
  );
}

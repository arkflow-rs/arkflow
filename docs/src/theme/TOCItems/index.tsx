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
import TOCItems from '@theme-original/TOCItems';
import type {WrapperProps} from '@docusaurus/types';
import type TOCItemsType from '@theme/TOCItems';

type Props = WrapperProps<typeof TOCItemsType>;

// Wrapping swizzle of the stock TOCItems: the scroll-spy behavior is
// untouched, we only render a progress fill alongside the tree. The fill
// is a pure function of scroll position (no transitions), so users with
// `prefers-reduced-motion` get instant state changes.

export default function TOCItemsWrapper(props: Props): ReactNode {
  const trackRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    let raf = 0;
    const update = () => {
      raf = 0;
      const el = trackRef.current;
      if (!el) return;
      const scrollable =
        document.documentElement.scrollHeight - window.innerHeight;
      const fraction =
        scrollable > 0 ? Math.min(1, window.scrollY / scrollable) : 0;
      el.style.setProperty('--af-toc-progress', fraction.toFixed(4));
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

  return (
    <div className="af-toc">
      <div ref={trackRef} className="af-toc__progress" aria-hidden="true" />
      <TOCItems {...props} />
    </div>
  );
}

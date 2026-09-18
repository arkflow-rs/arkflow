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

import type {CSSProperties, MouseEvent, ReactNode} from 'react';
import {useEffect, useRef, useState} from 'react';
import clsx from 'clsx';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import Layout from '@theme/Layout';
import Heading from '@theme/Heading';

import styles from './index.module.css';

const PIPELINE_YAML = `streams:
  - id: orders-to-mysql
    input:
      type: kafka
      brokers: [localhost:9092]
      topics: [shop.orders]
    pipeline:
      processors:
        - type: json_to_arrow
        - type: sql
          query: "SELECT * FROM flow WHERE status = 'PAID'"
        - type: arrow_to_json
    output:
      type: sql
      output_type:
        type: mysql
        uri: mysql://root@localhost:3306/arkflow
      table_name: orders
    error_output:
      type: stdout`;

const PIPELINE_LINES = PIPELINE_YAML.split('\n');
const LINE_REVEAL_MS = 120;
const REDUCED_MOTION_QUERY = '(prefers-reduced-motion: reduce)';

function prefersReducedMotion(): boolean {
  return (
    typeof window !== 'undefined' &&
    window.matchMedia(REDUCED_MOTION_QUERY).matches
  );
}

/* ============================================================
 * Code window — YAML reveals line by line with a cursor, then a
 * "pipeline running" status pulses in. Reduced motion: static.
 * ============================================================ */

function CodeWindow() {
  const [visibleLines, setVisibleLines] = useState(0);
  const frameRef = useRef<HTMLDivElement>(null);
  const done = visibleLines >= PIPELINE_LINES.length;

  useEffect(() => {
    if (prefersReducedMotion()) {
      setVisibleLines(PIPELINE_LINES.length);
      return;
    }
    const timer = setInterval(() => {
      setVisibleLines((n) => {
        if (n >= PIPELINE_LINES.length) {
          clearInterval(timer);
          return n;
        }
        return n + 1;
      });
    }, LINE_REVEAL_MS);
    return () => clearInterval(timer);
  }, []);

  const handleTilt = (e: MouseEvent<HTMLDivElement>) => {
    const el = frameRef.current;
    if (!el || prefersReducedMotion()) return;
    const rect = el.getBoundingClientRect();
    const x = (e.clientX - rect.left) / rect.width - 0.5;
    const y = (e.clientY - rect.top) / rect.height - 0.5;
    el.style.transform = `perspective(900px) rotateX(${(-y * 4).toFixed(
      2,
    )}deg) rotateY(${(x * 5).toFixed(2)}deg)`;
  };

  const resetTilt = () => {
    if (frameRef.current) frameRef.current.style.transform = '';
  };

  return (
    <div
      className={styles.codeWindow}
      ref={frameRef}
      onMouseMove={handleTilt}
      onMouseLeave={resetTilt}>
      <div className={styles.codeChrome}>
        <span className={clsx(styles.dot, styles.dotRed)} />
        <span className={clsx(styles.dot, styles.dotYellow)} />
        <span className={clsx(styles.dot, styles.dotGreen, styles.dotBreathe)} />
        <span className={styles.codeTitle}>config.yaml</span>
      </div>
      <pre className={styles.codeBody}>
        <code>
          {PIPELINE_LINES.slice(0, visibleLines).map((line, index) => (
            <span key={index} className={styles.codeLine}>
              {line || ' '}
              {index === visibleLines - 1 && !done && (
                <span className={styles.cursor} aria-hidden="true" />
              )}
            </span>
          ))}
          {done && (
            <span className={styles.codeStatus}>
              <span className={styles.statusDot} aria-hidden="true" />
              pipeline running · 0 dropped records
            </span>
          )}
        </code>
      </pre>
    </div>
  );
}

/* ============================================================
 * Scroll reveal — content starts visible (SSR/no-JS safe), then
 * JS arms it below the fold and fades it in on intersection.
 * ============================================================ */

function useReveal<T extends HTMLElement>() {
  const ref = useRef<T>(null);
  useEffect(() => {
    const el = ref.current;
    if (!el || prefersReducedMotion()) return;
    el.classList.add(styles.revealPending);
    // A healthy observer reports the initial state immediately, even
    // when not intersecting. If it never calls back (broken webview),
    // never leave the content invisible.
    let gotCallback = false;
    const observer = new IntersectionObserver(
      (entries) => {
        gotCallback = true;
        for (const entry of entries) {
          if (entry.isIntersecting) {
            el.classList.add(styles.revealVisible);
            observer.disconnect();
          }
        }
      },
      {threshold: 0.15, rootMargin: '0px 0px -40px 0px'},
    );
    observer.observe(el);
    const failsafe = setTimeout(() => {
      if (!gotCallback) {
        observer.disconnect();
        el.classList.add(styles.revealVisible);
      }
    }, 1500);
    return () => {
      clearTimeout(failsafe);
      observer.disconnect();
    };
  }, []);
  return ref;
}

function Reveal({
  className,
  style,
  children,
}: {
  className?: string;
  style?: CSSProperties;
  children: ReactNode;
}) {
  const ref = useReveal<HTMLDivElement>();
  return (
    <div ref={ref} className={className} style={style}>
      {children}
    </div>
  );
}

function Hero() {
  const {siteConfig} = useDocusaurusContext();
  return (
    <header className={styles.hero}>
      <div className={styles.heroBackdrop} aria-hidden="true" />
      <div className={styles.heroGridLayer} aria-hidden="true" />
      <div className={clsx(styles.heroOrb, styles.heroOrbA)} aria-hidden="true" />
      <div className={clsx(styles.heroOrb, styles.heroOrbB)} aria-hidden="true" />
      <div className={clsx('container', styles.heroInner)}>
        <div className={styles.heroGrid}>
          <div className={styles.heroCopy}>
            <div className={styles.badgeRow}>
              <span className={styles.badge}>Apache-2.0</span>
              <span className={styles.badge}>Rust · Tokio · Arrow</span>
              <span className={styles.badge}>CNCF Landscape</span>
            </div>
            <Heading as="h1" className={styles.heroTitle}>
              Stream processing that{' '}
              <span className={styles.heroAccent}>never drops a record</span>
            </Heading>
            <p className={styles.heroSubtitle}>
              {siteConfig.tagline}. Write declarative YAML pipelines, process
              with SQL, and rely on write-ahead-log durability, checkpointed
              state, and a built-in control plane for fleet operations.
            </p>
            <div className={styles.heroButtons}>
              <Link
                className="button button--primary button--lg"
                to="/docs/get-started/quickstart">
                Get started
              </Link>
              <Link
                className={clsx('button button--lg', styles.ghostButton)}
                href="https://github.com/arkflow-rs/arkflow">
                GitHub
              </Link>
            </div>
            <p className={styles.heroHint}>
              <code>./arkflow --config config.yaml</code> — one binary, no
              cluster required.
            </p>
          </div>
          <CodeWindow />
        </div>
      </div>
    </header>
  );
}

type Feature = {icon: ReactNode; title: string; body: string};

const FEATURES: Feature[] = [
  {
    icon: '⚡',
    title: 'High performance',
    body: 'Rust on Tokio with a columnar Apache Arrow data model. Operators fuse into chains — no channel hop inside a pipeline stage.',
  },
  {
    icon: '🛡️',
    title: 'Durable by default',
    body: 'Every message is fsynced to a write-ahead log before processing. Cursors advance only through the highest contiguous acknowledged sequence.',
  },
  {
    icon: '🎯',
    title: 'Exactly-once, opt-in',
    body: 'Kafka transactional outputs close the duplicate window end-to-end. Checkpointed state restores jobs to a consistent cut.',
  },
  {
    icon: '🧮',
    title: 'Process with SQL',
    body: 'DataFusion-powered SQL, window functions, Python UDFs, and VRL transforms — plus Protobuf, Debezium CDC, and Schema Registry codecs.',
  },
  {
    icon: '🛰️',
    title: 'Fleet control plane',
    body: 'A Hub/Agent architecture with desired-state semantics, reconciliation, audited rollouts, and a web console with a job DAG editor.',
  },
  {
    icon: '🧩',
    title: 'One kernel, any topology',
    body: 'Streams and distributed jobs compile to the same JobSpec. Input → buffer → processors → output is all you declare; the kernel does the rest.',
  },
];

function Features() {
  return (
    <section className={styles.section}>
      <div className="container">
        <Heading as="h2" className={styles.sectionTitle}>
          Built for production data pipelines
        </Heading>
        <p className={styles.sectionLead}>
          From a single binary on your laptop to an audited fleet rollout.
        </p>
        <div className="row">
          {FEATURES.map((feature, index) => (
            <div key={feature.title} className="col col--4 margin-bottom--lg">
              <Reveal
                className={styles.featureCard}
                style={{transitionDelay: `${(index % 3) * 80}ms`}}>
                <div className={styles.featureIcon} aria-hidden="true">
                  {feature.icon}
                </div>
                <h3>{feature.title}</h3>
                <p>{feature.body}</p>
              </Reveal>
            </div>
          ))}
        </div>
      </div>
    </section>
  );
}

function PipelineBand() {
  const stages = [
    {label: 'Ingest', items: ['Kafka', 'HTTP', 'MQTT', 'NATS', 'Pulsar']},
    {label: 'Process', items: ['SQL', 'Python', 'VRL', 'Windows']},
    {label: 'Deliver', items: ['SQL', 'Kafka', 'Redis', 'InfluxDB', 'S3 WAL']},
  ];
  return (
    <section className={styles.band}>
      <div className="container">
        <Heading as="h2" className={styles.bandTitle}>
          One config, the whole journey
        </Heading>
        <Reveal className={styles.pipeline}>
          {stages.map((stage, index) => (
            <div key={stage.label} className={styles.pipelineStageWrap}>
              {index > 0 && (
                <div className={styles.pipelineArrow} aria-hidden="true">
                  →
                </div>
              )}
              <div className={styles.pipelineStage}>
                <div className={styles.pipelineLabel}>{stage.label}</div>
                <div className={styles.pipelineChips}>
                  {stage.items.map((item, itemIndex) => (
                    <span
                      key={item}
                      className={clsx(styles.chip, styles.chipFlow)}
                      style={{
                        animationDelay: `${index * 1.4 + itemIndex * 0.12}s`,
                      }}>
                      {item}
                    </span>
                  ))}
                </div>
              </div>
            </div>
          ))}
        </Reveal>
        <div className={styles.bandChips}>
          <span className={styles.guarantee}>
            ✓ WAL durability before processing
          </span>
          <span className={styles.guarantee}>
            ✓ At-least-once by default, exactly-once opt-in
          </span>
          <span className={styles.guarantee}>
            ✓ Checkpoint, crash, recover
          </span>
        </div>
        <div className={styles.bandCta}>
          <Link className="button button--primary button--lg" to="/docs/build/streams">
            Build your first pipeline
          </Link>
        </div>
      </div>
    </section>
  );
}

function DocsPaths() {
  const paths = [
    {
      title: 'Learn the concepts',
      body: 'Architecture, backpressure, delivery semantics, and the durability model.',
      to: '/docs/build/architecture',
      cta: 'Understand ArkFlow',
    },
    {
      title: 'Ship a use case',
      body: 'Kafka → SQL, CDC with Debezium, windowed aggregation — each with a validated example.',
      to: '/docs/build/recipes/kafka-to-sql',
      cta: 'Browse recipes',
    },
    {
      title: 'Run a fleet',
      body: 'Control plane, web console, rollouts, observability, and a recovery runbook.',
      to: '/docs/operate/overview',
      cta: 'Operate ArkFlow',
    },
  ];
  return (
    <section className={styles.section}>
      <div className="container">
        <Heading as="h2" className={styles.sectionTitle}>
          Where do you want to go?
        </Heading>
        <div className="row">
          {paths.map((path, index) => (
            <div key={path.title} className="col col--4 margin-bottom--lg">
              <Reveal
                className={styles.pathCardReveal}
                style={{transitionDelay: `${index * 90}ms`}}>
                <Link className={styles.pathCard} to={path.to}>
                  <h3>{path.title}</h3>
                  <p>{path.body}</p>
                  <span className={styles.pathCta}>{path.cta} →</span>
                </Link>
              </Reveal>
            </div>
          ))}
        </div>
      </div>
    </section>
  );
}

function CommunityBand() {
  return (
    <section className={styles.community}>
      <div className="container">
        <Reveal>
          <Heading as="h2" className={styles.communityTitle}>
            Join the flow
          </Heading>
          <p className={styles.communityLead}>
            ArkFlow is open source under Apache-2.0 and listed in the CNCF
            Landscape. Contributions, issues, and ideas are welcome.
          </p>
          <div className={styles.communityButtons}>
            <Link
              className="button button--primary button--lg"
              href="https://github.com/arkflow-rs/arkflow">
              Star on GitHub
            </Link>
            <Link
              className={clsx('button button--lg', styles.communityGhost)}
              href="https://discord.gg/CwKhzb8pux">
              Chat on Discord
            </Link>
          </div>
        </Reveal>
      </div>
    </section>
  );
}

export default function Home(): ReactNode {
  const {siteConfig} = useDocusaurusContext();
  return (
    <Layout
      title={`${siteConfig.title} — Rust stream processing engine`}
      description="High-performance Rust stream processing engine: WAL-durable pipelines, SQL processing, checkpointed state, and a control plane for fleet operations.">
      <Hero />
      <main>
        <Features />
        <PipelineBand />
        <DocsPaths />
        <CommunityBand />
      </main>
    </Layout>
  );
}

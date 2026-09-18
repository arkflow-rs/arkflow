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
import {Fragment, useEffect, useRef, useState} from 'react';
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

/* ============================================================
 * Architecture diagram — the anatomy of the engine, drawn as a
 * live SVG. Batches (mini columnar Arrow glyphs) ride the input
 * and output connectors, a light wave walks the kernel stages,
 * and hovering any node swaps the readout line under the card.
 * Static and fully readable with reduced motion or no SMIL.
 * ============================================================ */

const ARCH_INFO_DEFAULT =
  'Hover any node to inspect it — the light wave is a batch moving through the kernel.';

const ARCH_IO_Y = [224, 304, 384, 464];

const ARCH_SOURCES = [
  {
    label: 'Kafka',
    sub: 'consumer groups',
    info: 'Kafka input: consumer-group offsets make reads resumable and replayable.',
  },
  {
    label: 'MQTT',
    sub: 'QoS subscriptions',
    info: 'MQTT input: QoS-aware subscriptions; metadata columns tag topic and QoS.',
  },
  {
    label: 'HTTP',
    sub: 'REST push',
    info: 'HTTP input: push batches over REST — a good fit for edge and app events.',
  },
  {
    label: 'NATS',
    sub: 'core · JetStream',
    info: 'NATS input: core and JetStream subscriptions with durable cursors.',
  },
];

const ARCH_SINKS = [
  {
    label: 'MySQL',
    sub: 'upsert batches',
    info: 'SQL output: batched upserts into MySQL — same sink family for Postgres and more.',
  },
  {
    label: 'Kafka',
    sub: 'transactions',
    info: 'Kafka output: transactional producer closes the exactly-once duplicate window.',
  },
  {
    label: 'Redis',
    sub: 'hot path cache',
    info: 'Redis output: fast lane for enrichment caches, counters, and lookaside data.',
  },
  {
    label: 'InfluxDB',
    sub: 'metrics',
    info: 'InfluxDB output: windowed aggregates land as time-series metrics.',
  },
];

const ARCH_STAGES = [
  {
    x: 304,
    title: 'Input',
    sub: 'source reader',
    delay: 0,
    info: 'Input worker pulls batches from the source and attaches __meta_* metadata columns.',
  },
  {
    x: 429,
    title: 'WAL',
    sub: 'fsync · replay',
    delay: 0.3,
    info: 'Write-ahead log fsyncs every batch before processing — restart the process, lose nothing.',
  },
  {
    x: 554,
    title: 'Buffer',
    sub: 'windows · joins',
    delay: 0.6,
    info: 'In-memory queue or windowing strategy: tumbling, sliding, session — with stream joins.',
  },
  {
    x: 679,
    title: 'Process',
    sub: 'SQL · UDF · VRL',
    delay: 0.9,
    bars: true,
    info: 'Processor chain over Arrow RecordBatches: DataFusion SQL, Python UDFs, VRL, codecs.',
  },
  {
    x: 804,
    title: 'Output',
    sub: 'ordered sink',
    delay: 1.2,
    info: 'Ordered writer with at-least-once acks; Kafka transactions make it exactly-once.',
  },
];

const ARCH_KERNEL_CHIPS = [
  {
    x: 300,
    label: 'Arrow MessageBatch',
    info: 'MessageBatch wraps an Arrow RecordBatch — columnar end to end, so SQL runs without reserializing.',
  },
  {
    x: 500,
    label: 'bounded channels · backpressure',
    info: 'Stage edges are bounded flume channels: backpressure propagates upstream, nothing buffers unboundedly.',
  },
  {
    x: 700,
    label: 'checkpointed state',
    info: 'Stateful mutations stage until the processing ack commits; checkpoints restore a consistent cut.',
  },
];

const ARCH_BAR_HEIGHTS = [34, 20, 30, 14, 26];
const ARCH_BAR_COLORS = ['#38bdf8', '#60a5fa', '#7dd3fc', '#60a5fa', '#38bdf8'];

const ARCH_SOURCE_PATHS = [
  'M176,250 C250,250 240,330 300,330',
  'M176,330 C230,330 240,330 300,330',
  'M176,410 C250,410 240,330 300,330',
  'M176,490 C250,490 240,330 300,330',
];

const ARCH_SINK_PATHS = [
  'M896,330 C966,330 950,250 1020,250',
  'M896,330 C966,330 950,330 1020,330',
  'M896,330 C966,330 950,410 1020,410',
  'M896,330 C966,330 950,490 1020,490',
];

const ARCH_CTRL_PATH = 'M600,100 L600,164';
const ARCH_ERROR_PATH = 'M725,379 L725,424';

/** A flowing record batch: a tiny three-column Arrow glyph. */
function ArchParticle({
  pathId,
  begin,
  dur,
}: {
  pathId: string;
  begin: number;
  dur: number;
}) {
  return (
    <g className={styles.particle}>
      <circle r={11} fill="url(#afPGlow)" />
      <rect x={-6.5} y={-2} width={3} height={9} rx={1} className={styles.pBarA} />
      <rect x={-1.5} y={-7} width={3} height={14} rx={1} className={styles.pBarB} />
      <rect x={3.5} y={-4} width={3} height={11} rx={1} className={styles.pBarC} />
      <animateMotion
        dur={`${dur}s`}
        begin={`${begin}s`}
        repeatCount="indefinite"
        rotate="auto">
        <mpath href={`#${pathId}`} />
      </animateMotion>
    </g>
  );
}

function ArchitectureSection() {
  const [info, setInfo] = useState(ARCH_INFO_DEFAULT);
  const [stats, setStats] = useState({inV: 128_940, doneV: 128_940});

  useEffect(() => {
    if (prefersReducedMotion()) return;
    const timer = setInterval(() => {
      setStats((s) => ({
        inV: s.inV + 780 + Math.floor(Math.random() * 2400),
        // "processed" trails one tick behind "in" — in flight, never dropped.
        doneV: s.inV,
      }));
    }, 700);
    return () => clearInterval(timer);
  }, []);

  const hover = (text: string) => ({
    onMouseEnter: () => setInfo(text),
    onMouseLeave: () => setInfo(ARCH_INFO_DEFAULT),
    onFocus: () => setInfo(text),
    onBlur: () => setInfo(ARCH_INFO_DEFAULT),
  });

  const fmt = (n: number) => n.toLocaleString('en-US');

  return (
    <section className={styles.archSection}>
      <div className="container">
        <Heading as="h2" className={styles.archTitle}>
          Under the hood
        </Heading>
        <p className={styles.archLead}>
          Streams and distributed jobs compile to the same JobSpec and run on
          one execution kernel. Batches stay durable, SQL does the heavy
          lifting, sinks stay ordered — while a Hub/Agent control plane keeps
          the fleet honest.
        </p>
        <Reveal className={styles.archCard}>
          <div className={styles.archScrollWrap}>
            <div className={styles.archScroll}>
            <svg
              viewBox="0 0 1200 610"
              className={styles.archSvg}
              role="img"
              aria-labelledby="afArchTitle afArchDesc">
              <title id="afArchTitle">ArkFlow engine architecture</title>
              <desc id="afArchDesc">
                Sources feed the ArkFlow engine — input, WAL, buffer,
                processors, output over Arrow record batches — supervised by a
                Hub/Agent control plane, and write to sinks.
              </desc>
              <defs>
                <linearGradient id="afEngGrad" x1="0" y1="0" x2="1" y2="1">
                  <stop offset="0%" stopColor="#1d4ed8" />
                  <stop offset="100%" stopColor="#0ea5e9" />
                </linearGradient>
                <radialGradient id="afGlowGrad">
                  <stop offset="0%" stopColor="rgba(56,189,248,0.55)" />
                  <stop offset="100%" stopColor="rgba(56,189,248,0)" />
                </radialGradient>
                <radialGradient id="afPGlow">
                  <stop offset="0%" stopColor="rgba(125,211,252,0.5)" />
                  <stop offset="100%" stopColor="rgba(125,211,252,0)" />
                </radialGradient>
                <marker
                  id="afArrow"
                  viewBox="0 0 10 10"
                  refX="8.5"
                  refY="5"
                  markerWidth="6.5"
                  markerHeight="6.5"
                  orient="auto-start-reverse">
                  <path d="M0,0 L10,5 L0,10 Z" fill="#60a5fa" />
                </marker>
                <marker
                  id="afArrowMuted"
                  viewBox="0 0 10 10"
                  refX="8.5"
                  refY="5"
                  markerWidth="6"
                  markerHeight="6"
                  orient="auto-start-reverse">
                  <path d="M0,0 L10,5 L0,10 Z" fill="#94a3b8" />
                </marker>
                <marker
                  id="afArrowErr"
                  viewBox="0 0 10 10"
                  refX="8.5"
                  refY="5"
                  markerWidth="6"
                  markerHeight="6"
                  orient="auto-start-reverse">
                  <path d="M0,0 L10,5 L0,10 Z" fill="#f87171" />
                </marker>
              </defs>

              {/* Connectors — drawn first so nodes sit on top. */}
              {ARCH_SOURCE_PATHS.map((d, i) => (
                <path
                  key={`src-${i}`}
                  id={`afSrc${i}`}
                  d={d}
                  className={clsx(styles.conn, styles.connFlow)}
                  markerEnd="url(#afArrow)"
                />
              ))}
              {ARCH_SINK_PATHS.map((d, i) => (
                <path
                  key={`sink-${i}`}
                  id={`afSink${i}`}
                  d={d}
                  className={clsx(styles.conn, styles.connFlow)}
                  markerEnd="url(#afArrow)"
                />
              ))}
              <path
                id="afCtrl"
                d={ARCH_CTRL_PATH}
                className={styles.connCtrl}
                markerEnd="url(#afArrowMuted)"
              />
              <path
                d={ARCH_ERROR_PATH}
                className={styles.connErr}
                markerEnd="url(#afArrowErr)"
              />
              {ARCH_STAGES.slice(0, 4).map((stage) => (
                <path
                  key={`hop-${stage.x}`}
                  d={`M${stage.x + 94},330 L${stage.x + 121},330`}
                  className={styles.internalArrow}
                  markerEnd="url(#afArrow)"
                />
              ))}

              {/* Control plane band. */}
              <g
                tabIndex={0}
                className={styles.hoverable}
                {...hover(
                  'Control plane: the Hub holds desired state, Agents reconcile jobs onto nodes, and the console edits and audits rollouts.',
                )}>
                <rect
                  x="330"
                  y="24"
                  width="540"
                  height="72"
                  rx="12"
                  className={styles.ctrlBand}
                />
                <text x="600" y="46" textAnchor="middle" className={styles.tCtrl}>
                  CONTROL PLANE · ARKFLOW-SERVER
                </text>
                {[
                  {x: 355, label: 'Web console'},
                  {x: 525, label: 'Hub'},
                  {x: 695, label: 'Agents'},
                ].map((chip) => (
                  <Fragment key={chip.label}>
                    <rect
                      x={chip.x}
                      y="56"
                      width="150"
                      height="30"
                      rx="8"
                      className={styles.ctrlChipRect}
                    />
                    <text
                      x={chip.x + 75}
                      y="75"
                      textAnchor="middle"
                      className={styles.tChip}>
                      {chip.label}
                    </text>
                  </Fragment>
                ))}
              </g>
              <text x="620" y="136" className={styles.tTiny}>
                desired state · reconcile
              </text>

              {/* Column headers. */}
              <text x="106" y="204" textAnchor="middle" className={styles.tCol}>
                SOURCES
              </text>
              <text x="1094" y="204" textAnchor="middle" className={styles.tCol}>
                SINKS
              </text>

              {/* Engine container. */}
              <rect
                x="283"
                y="173"
                width="634"
                height="380"
                rx="18"
                fill="none"
                stroke="rgba(29,78,216,0.16)"
                strokeWidth="9"
              />
              <rect
                x="281.5"
                y="171.5"
                width="637"
                height="383"
                rx="17"
                fill="none"
                stroke="rgba(56,189,248,0.18)"
                strokeWidth="3.5"
              />
              <rect
                x="280"
                y="170"
                width="640"
                height="386"
                rx="16"
                fill="rgba(11,19,34,0.78)"
                stroke="url(#afEngGrad)"
                strokeWidth="1.6"
              />
              <text x="600" y="204" textAnchor="middle" className={styles.tEngine}>
                ARKFLOW ENGINE
              </text>
              <text x="600" y="224" textAnchor="middle" className={styles.tEngineSub}>
                streams &amp; jobs compile to one JobSpec · unified execution kernel
              </text>

              {/* Kernel stages. */}
              {ARCH_STAGES.map((stage) => (
                <g
                  key={stage.title}
                  tabIndex={0}
                  className={styles.hoverable}
                  {...hover(stage.info)}>
                  <ellipse
                    cx={stage.x + 46}
                    cy="330"
                    rx="74"
                    ry="50"
                    fill="url(#afGlowGrad)"
                    className={styles.stageGlow}
                    style={{animationDelay: `${stage.delay}s`}}
                  />
                  <rect
                    x={stage.x}
                    y="285"
                    width="92"
                    height="90"
                    rx="12"
                    className={styles.stageRect}
                    style={{animationDelay: `${stage.delay}s`}}
                  />
                  <text
                    x={stage.x + 46}
                    y={stage.bars ? 310 : 332}
                    textAnchor="middle"
                    className={styles.tStage}>
                    {stage.title}
                  </text>
                  <text
                    x={stage.x + 46}
                    y={stage.bars ? 325 : 350}
                    textAnchor="middle"
                    className={styles.tStageSub}>
                    {stage.sub}
                  </text>
                  {stage.bars &&
                    ARCH_BAR_HEIGHTS.map((h, i) => (
                      <rect
                        key={i}
                        x={695 + i * 13}
                        y={368 - h}
                        width="8"
                        height={h}
                        rx="2"
                        fill={ARCH_BAR_COLORS[i]}
                        className={styles.procBar}
                        style={{
                          animationDelay: `${i * 0.13}s`,
                          animationDuration: `${0.9 + (i % 3) * 0.25}s`,
                        }}
                      />
                    ))}
                </g>
              ))}

              {/* Error output branch. */}
              <g
                tabIndex={0}
                className={styles.hoverable}
                {...hover(
                  'Batches a processor failed on divert here — the stream never stalls or drops them silently.',
                )}>
                <rect
                  x="655"
                  y="428"
                  width="140"
                  height="34"
                  rx="8"
                  className={styles.errRect}
                />
                <text x="725" y="449" textAnchor="middle" className={styles.tErr}>
                  error output
                </text>
              </g>

              {/* Kernel fact chips. */}
              {ARCH_KERNEL_CHIPS.map((chip) => (
                <g
                  key={chip.label}
                  tabIndex={0}
                  className={styles.hoverable}
                  {...hover(chip.info)}>
                  <rect
                    x={chip.x}
                    y="486"
                    width="180"
                    height="30"
                    rx="8"
                    className={styles.kernelRect}
                  />
                  <text
                    x={chip.x + 90}
                    y="505"
                    textAnchor="middle"
                    className={styles.tKernel}>
                    {chip.label}
                  </text>
                </g>
              ))}

              {/* Sources and sinks. */}
              {ARCH_SOURCES.map((source, i) => (
                <g
                  key={source.label}
                  tabIndex={0}
                  className={styles.hoverable}
                  {...hover(source.info)}>
                  <rect
                    x="36"
                    y={ARCH_IO_Y[i]}
                    width="140"
                    height="52"
                    rx="10"
                    className={styles.ioRect}
                  />
                  <text x="106" y={ARCH_IO_Y[i] + 24} textAnchor="middle" className={styles.tLabel}>
                    {source.label}
                  </text>
                  <text x="106" y={ARCH_IO_Y[i] + 40} textAnchor="middle" className={styles.tSub}>
                    {source.sub}
                  </text>
                </g>
              ))}
              {ARCH_SINKS.map((sink, i) => (
                <g
                  key={sink.label}
                  tabIndex={0}
                  className={styles.hoverable}
                  {...hover(sink.info)}>
                  <rect
                    x="1024"
                    y={ARCH_IO_Y[i]}
                    width="140"
                    height="52"
                    rx="10"
                    className={styles.ioRect}
                  />
                  <text x="1094" y={ARCH_IO_Y[i] + 24} textAnchor="middle" className={styles.tLabel}>
                    {sink.label}
                  </text>
                  <text x="1094" y={ARCH_IO_Y[i] + 40} textAnchor="middle" className={styles.tSub}>
                    {sink.sub}
                  </text>
                </g>
              ))}

              {/* Flowing batches. */}
              {ARCH_SOURCE_PATHS.map((_, i) => (
                <Fragment key={`psrc-${i}`}>
                  <ArchParticle pathId={`afSrc${i}`} begin={0} dur={2.6} />
                  <ArchParticle pathId={`afSrc${i}`} begin={-1.3} dur={2.6} />
                </Fragment>
              ))}
              {ARCH_SINK_PATHS.map((_, i) => (
                <Fragment key={`psink-${i}`}>
                  <ArchParticle pathId={`afSink${i}`} begin={-0.4} dur={2.2} />
                  <ArchParticle pathId={`afSink${i}`} begin={-1.5} dur={2.2} />
                </Fragment>
              ))}
              <g className={styles.particle}>
                <circle r="4" fill="#93c5fd" opacity="0.9" />
                <animateMotion dur="3.2s" begin="-1s" repeatCount="indefinite">
                  <mpath href="#afCtrl" />
                </animateMotion>
              </g>
            </svg>
            </div>
            <span className={styles.scrollHint} aria-hidden="true">
              Swipe to explore
              <span className={styles.scrollHintArrow}>→</span>
            </span>
          </div>
          <div className={styles.archFooter}>
            <div className={styles.archInfo}>
              <span className={styles.infoDot} aria-hidden="true" />
              {info}
            </div>
            <div className={styles.archStats}>
              <span>
                records in <b>{fmt(stats.inV)}</b>
              </span>
              <span>
                processed <b>{fmt(stats.doneV)}</b>
              </span>
              <span className={styles.statOk}>
                dropped <b>0</b>
              </span>
            </div>
          </div>
        </Reveal>
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
        <ArchitectureSection />
        <DocsPaths />
        <CommunityBand />
      </main>
    </Layout>
  );
}

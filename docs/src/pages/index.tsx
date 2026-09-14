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

function Hero() {
  const {siteConfig} = useDocusaurusContext();
  return (
    <header className={styles.hero}>
      <div className={styles.heroBackdrop} aria-hidden="true" />
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
          <div className={styles.codeWindow}>
            <div className={styles.codeChrome}>
              <span className={clsx(styles.dot, styles.dotRed)} />
              <span className={clsx(styles.dot, styles.dotYellow)} />
              <span className={clsx(styles.dot, styles.dotGreen)} />
              <span className={styles.codeTitle}>config.yaml</span>
            </div>
            <pre className={styles.codeBody}>
              <code>{PIPELINE_YAML}</code>
            </pre>
          </div>
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
          {FEATURES.map((feature) => (
            <div key={feature.title} className="col col--4 margin-bottom--lg">
              <div className={styles.featureCard}>
                <div className={styles.featureIcon} aria-hidden="true">
                  {feature.icon}
                </div>
                <h3>{feature.title}</h3>
                <p>{feature.body}</p>
              </div>
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
        <div className={styles.pipeline}>
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
                  {stage.items.map((item) => (
                    <span key={item} className={styles.chip}>
                      {item}
                    </span>
                  ))}
                </div>
              </div>
            </div>
          ))}
        </div>
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
          {paths.map((path) => (
            <div key={path.title} className="col col--4 margin-bottom--lg">
              <Link className={styles.pathCard} to={path.to}>
                <h3>{path.title}</h3>
                <p>{path.body}</p>
                <span className={styles.pathCta}>{path.cta} →</span>
              </Link>
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

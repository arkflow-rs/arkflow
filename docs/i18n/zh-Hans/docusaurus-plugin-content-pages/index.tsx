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

// zh-Hans localization of the landing page (docs/src/pages/index.tsx). The
// English original is the canonical source; keep this copy structurally in
// sync when the original changes. Code samples stay in English by policy.

import type {CSSProperties, MouseEvent, ReactNode} from 'react';
import {Fragment, useEffect, useRef, useState} from 'react';
import clsx from 'clsx';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import Layout from '@theme/Layout';
import Heading from '@theme/Heading';

import styles from '../../../src/pages/index.module.css';

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
              流水线运行中 · 0 条记录被丢弃
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
              一套<span className={styles.heroAccent}>永不丢失记录</span>
              的流处理引擎
            </Heading>
            <p className={styles.heroSubtitle}>
              {siteConfig.tagline}。用声明式 YAML 编写流水线，以 SQL
              处理数据，依托预写日志（WAL）持久化、检查点状态，以及内置的控制平面完成机群运维。
            </p>
            <div className={styles.heroButtons}>
              <Link
                className="button button--primary button--lg"
                to="/zh-Hans/docs/get-started/quickstart">
                快速开始
              </Link>
              <Link
                className={clsx('button button--lg', styles.ghostButton)}
                href="https://github.com/arkflow-rs/arkflow">
                GitHub
              </Link>
            </div>
            <p className={styles.heroHint}>
              <code>./arkflow --config config.yaml</code> ——
              单一二进制，无需集群。
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
    title: '高性能',
    body: 'Rust + Tokio，列式 Apache Arrow 数据模型。算子融合为链——流水线内部没有跨通道跳转。',
  },
  {
    icon: '🛡️',
    title: '默认持久化',
    body: '每条消息在处理前先 fsync 到预写日志。游标只按最高连续确认序列号推进。',
  },
  {
    icon: '🎯',
    title: '可选精确一次',
    body: 'Kafka 事务输出端到端消除重复窗口。检查点状态将作业恢复到一致切面。',
  },
  {
    icon: '🧮',
    title: '用 SQL 处理',
    body: 'DataFusion 驱动的 SQL、窗口函数、Python UDF 与 VRL 变换——另有 Protobuf、Debezium CDC 与 Schema Registry 编解码器。',
  },
  {
    icon: '🛰️',
    title: '机群控制平面',
    body: 'Hub/Agent 架构，期望状态语义、调和机制、可审计的发布流程，以及带作业 DAG 编辑器的 Web 控制台。',
  },
  {
    icon: '🧩',
    title: '同一内核，任意拓扑',
    body: '流与分布式作业编译为同一 JobSpec。你只需声明 输入 → 缓冲 → 处理器 → 输出，其余交给内核。',
  },
];

function Features() {
  return (
    <section className={styles.section}>
      <div className="container">
        <Heading as="h2" className={styles.sectionTitle}>
          为生产级数据流水线而生
        </Heading>
        <p className={styles.sectionLead}>
          从笔记本上的单个二进制，到经过审计的机群发布。
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
    {label: '接入', items: ['Kafka', 'HTTP', 'MQTT', 'NATS', 'Pulsar']},
    {label: '处理', items: ['SQL', 'Python', 'VRL', '窗口']},
    {label: '投递', items: ['SQL', 'Kafka', 'Redis', 'InfluxDB', 'S3 WAL']},
  ];
  return (
    <section className={styles.band}>
      <div className="container">
        <Heading as="h2" className={styles.bandTitle}>
          一份配置，贯穿全链路
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
            ✓ 处理前先落 WAL 持久化
          </span>
          <span className={styles.guarantee}>
            ✓ 默认至少一次，可选精确一次
          </span>
          <span className={styles.guarantee}>
            ✓ 检查点、崩溃、恢复
          </span>
        </div>
        <div className={styles.bandCta}>
          <Link className="button button--primary button--lg" to="/zh-Hans/docs/build/streams">
            构建你的第一条流水线
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
  '悬停任意节点查看说明 —— 图中的光波就是一个流经内核的批次。';

const ARCH_IO_Y = [224, 304, 384, 464];

const ARCH_SOURCES = [
  {
    label: 'Kafka',
    sub: '消费者组',
    info: 'Kafka 输入：消费者组偏移量让读取可恢复、可重放。',
  },
  {
    label: 'MQTT',
    sub: 'QoS 订阅',
    info: 'MQTT 输入：QoS 感知订阅；元数据列标记主题与 QoS。',
  },
  {
    label: 'HTTP',
    sub: 'REST 推送',
    info: 'HTTP 输入：通过 REST 推送批次——适合边缘与应用事件。',
  },
  {
    label: 'NATS',
    sub: 'core · JetStream',
    info: 'NATS 输入：core 与 JetStream 订阅，配备持久游标。',
  },
];

const ARCH_SINKS = [
  {
    label: 'MySQL',
    sub: '批量 upsert',
    info: 'SQL 输出：批量 upsert 到 MySQL——Postgres 等也属同一输出家族。',
  },
  {
    label: 'Kafka',
    sub: '事务',
    info: 'Kafka 输出：事务生产者关闭精确一次的重复窗口。',
  },
  {
    label: 'Redis',
    sub: '热路径缓存',
    info: 'Redis 输出：为 enrichment 缓存、计数器和旁路数据提供快速通道。',
  },
  {
    label: 'InfluxDB',
    sub: '指标',
    info: 'InfluxDB 输出：窗口聚合结果以时序指标形式落地。',
  },
];

const ARCH_STAGES = [
  {
    x: 304,
    title: '输入',
    sub: '源读取',
    delay: 0,
    info: '输入工作者从数据源拉取批次，并附加 __meta_* 元数据列。',
  },
  {
    x: 429,
    title: 'WAL',
    sub: 'fsync · 重放',
    delay: 0.3,
    info: '预写日志在处理前将每个批次 fsync——进程重启，数据不丢。',
  },
  {
    x: 554,
    title: '缓冲',
    sub: '窗口 · 连接',
    delay: 0.6,
    info: '内存队列或窗口策略：滚动、滑动、会话——支持流连接。',
  },
  {
    x: 679,
    title: '处理',
    sub: 'SQL · UDF · VRL',
    delay: 0.9,
    bars: true,
    info: '基于 Arrow RecordBatch 的处理器链：DataFusion SQL、Python UDF、VRL、编解码器。',
  },
  {
    x: 804,
    title: '输出',
    sub: '有序写出',
    delay: 1.2,
    info: '有序写入器配合至少一次确认；Kafka 事务可升级为精确一次。',
  },
];

const ARCH_KERNEL_CHIPS = [
  {
    x: 300,
    label: 'Arrow MessageBatch',
    info: 'MessageBatch 包装 Arrow RecordBatch——端到端列式，SQL 无需重新序列化即可执行。',
  },
  {
    x: 500,
    label: '有界通道 · 背压',
    info: '阶段边界是有界的 flume 通道：背压向上游传播，不做无界缓冲。',
  },
  {
    x: 700,
    label: '检查点状态',
    info: '有状态变更先暂存，处理确认提交后生效；检查点恢复一致切面。',
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
          深入引擎内部
        </Heading>
        <p className={styles.archLead}>
          流与分布式作业编译为同一 JobSpec，运行在同一个执行内核上。批次保持持久，SQL
          承担重活，输出保持有序——同时 Hub/Agent 控制平面守护整个机群。
        </p>
        <Reveal className={styles.archCard}>
          <div className={styles.archScrollWrap}>
            <div className={styles.archScroll}>
            <svg
              viewBox="0 0 1200 610"
              className={styles.archSvg}
              role="img"
              aria-labelledby="afArchTitle afArchDesc">
              <title id="afArchTitle">ArkFlow 引擎架构</title>
              <desc id="afArchDesc">
                数据源接入 ArkFlow 引擎——经输入、WAL、缓冲、处理器、输出，以 Arrow
                record batch 流转——由 Hub/Agent 控制平面统一调度，并写入下游。
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
                  '控制平面：Hub 保存期望状态，Agent 将作业调和到节点，控制台编辑并审计发布。',
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
                  控制平面 · ARKFLOW-SERVER
                </text>
                {[
                  {x: 355, label: 'Web 控制台'},
                  {x: 525, label: 'Hub'},
                  {x: 695, label: 'Agent'},
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
                期望状态 · 调和
              </text>

              {/* Column headers. */}
              <text x="106" y="204" textAnchor="middle" className={styles.tCol}>
                数据来源
              </text>
              <text x="1094" y="204" textAnchor="middle" className={styles.tCol}>
                输出目标
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
                ARKFLOW 引擎
              </text>
              <text x="600" y="224" textAnchor="middle" className={styles.tEngineSub}>
                流与作业编译为同一 JobSpec · 统一执行内核
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
                  '处理器处理失败的批次会转入这里——流不会因此停顿，也不会悄悄丢弃。',
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
                  错误输出
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
              滑动探索
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
                流入记录 <b>{fmt(stats.inV)}</b>
              </span>
              <span>
                已处理 <b>{fmt(stats.doneV)}</b>
              </span>
              <span className={styles.statOk}>
                丢弃 <b>0</b>
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
      title: '掌握核心概念',
      body: '架构、背压、投递语义与持久化模型。',
      to: '/zh-Hans/docs/build/architecture',
      cta: '理解 ArkFlow',
    },
    {
      title: '落地一个场景',
      body: 'Kafka → SQL、Debezium CDC、窗口聚合——每个场景都有经过校验的示例。',
      to: '/docs/build/recipes/kafka-to-sql',
      cta: '浏览实战配方',
    },
    {
      title: '运维一套机群',
      body: '控制平面、Web 控制台、发布流程、可观测性与恢复手册。',
      to: '/docs/operate/overview',
      cta: '运维 ArkFlow',
    },
  ];
  return (
    <section className={styles.section}>
      <div className="container">
        <Heading as="h2" className={styles.sectionTitle}>
          你想去哪里？
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
            加入 ArkFlow
          </Heading>
          <p className={styles.communityLead}>
            ArkFlow 是基于 Apache-2.0 的开源项目，已收录进 CNCF
            Landscape。欢迎贡献代码、提交 Issue 与分享想法。
          </p>
          <div className={styles.communityButtons}>
            <Link
              className="button button--primary button--lg"
              href="https://github.com/arkflow-rs/arkflow">
              在 GitHub 点个 Star
            </Link>
            <Link
              className={clsx('button button--lg', styles.communityGhost)}
              href="https://discord.gg/CwKhzb8pux">
              到 Discord 聊聊
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
      title={`${siteConfig.title} — Rust 流处理引擎`}
      description="高性能 Rust 流处理引擎：WAL 持久化流水线、SQL 处理、检查点状态，以及面向机群运维的控制平面。">
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

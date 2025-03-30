import type {ReactNode} from 'react';
import clsx from 'clsx';
import Heading from '@theme/Heading';
import styles from './styles.module.css';

type FeatureItem = {
    title: string;
    Svg: React.ComponentType<React.ComponentProps<'svg'>>;
    description: ReactNode;
};

const FeatureList: FeatureItem[] = [
    {
        title: 'High Performance',
        Svg: require('@site/static/img/home-1.svg').default,
        description: (
            <>
                Built on Rust and Tokio async runtime, offering excellent performance and low latency.
            </>
        ),
    },
    {
        title: 'Multiple Data Sources',
        Svg: require('@site/static/img/home-2.svg').default,
        description: (
            <>
                Support for Kafka, MQTT, HTTP, files, and other input/output sources.
            </>
        ),
    },
    {
        title: 'Powerful Processing Capabilities',
        Svg: require('@site/static/img/home-3.svg').default,
        description: (
            <>
                Built-in SQL queries, JSON processing, Protobuf encoding/decoding,
                batch processing, and other processors.
            </>
        ),
    },
    {
        title: 'Extensible',
        Svg: require('@site/static/img/home-4.svg').default,
        description: (
            <>
                Modular design, easy to extend with new input, output, and processor components.
            </>
        ),
    },
];

function Feature({title, Svg, description}: FeatureItem) {
    return (
        <div className={clsx('col col--3')}>
            <div className="text--center">
                <Svg className={styles.featureSvg} role="img"/>
            </div>
            <div className="text--center padding-horiz--md">
                <Heading as="h3">{title}</Heading>
                <p>{description}</p>
            </div>
        </div>
    );
}

export default function HomepageFeatures(): ReactNode {
    return (
        <section className={styles.features}>
            <div className="container">
                <div className="row">
                    {FeatureList.map((props, idx) => (
                        <Feature key={idx} {...props} />
                    ))}
                </div>
            </div>
        </section>
    );
}

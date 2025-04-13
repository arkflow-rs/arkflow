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

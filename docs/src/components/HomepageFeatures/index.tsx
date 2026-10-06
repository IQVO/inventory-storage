import type {ReactNode} from 'react';
import clsx from 'clsx';
import Link from '@docusaurus/Link';
import Heading from '@theme/Heading';
import styles from './styles.module.css';

type FeatureItem = {
  title: string;
  to: string;
  description: ReactNode;
};

const FeatureList: FeatureItem[] = [
  {
    title: 'Chaotic Storage',
    to: '/docs/business-context/chaotic-storage',
    description: (
      <>
        No fixed product location. An inbound item goes to any free bin and the
        system records the exact bin — which is why a stow is invalid without
        both an item-scan and a location-scan.
      </>
    ),
  },
  {
    title: 'Revocable Reservations',
    to: '/docs/business-context/revocable-reservations',
    description: (
      <>
        Allocation is a revocable, expiring claim — never a hard binding to one
        holding. A blocked pod or a short pick can always be re-satisfied
        elsewhere, so a failed delivery never strands an order.
      </>
    ),
  },
  {
    title: 'Usable Inventory',
    to: '/docs/business-context/usable-inventory',
    description: (
      <>
        On-hand minus active reservations minus held or unlocated stock.
        Usable — not total — is what constrains release, and it is exposed
        explicitly rather than left for callers to derive.
      </>
    ),
  },
  {
    title: 'Bin-Accurate Truth',
    to: '/docs/ddd/aggregate-design-canvas',
    description: (
      <>
        Every physical item has exactly one known bin, or is explicitly flagged
        Unlocated. Cycle counting reconciles the two, and loss is never
        silent.
      </>
    ),
  },
];

function Feature({title, to, description}: FeatureItem) {
  return (
    <div className={clsx('col col--3')}>
      <Link to={to} className={styles.featureCard}>
        <Heading as="h3" className={styles.featureTitle}>
          {title}
        </Heading>
        <p className={styles.featureBody}>{description}</p>
      </Link>
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

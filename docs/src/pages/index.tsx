import type {ReactNode} from 'react';
import clsx from 'clsx';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import Layout from '@theme/Layout';
import Heading from '@theme/Heading';

import styles from './index.module.css';

function StudyDisclaimer() {
  return (
    <div
      style={{
        background: '#fef3c7',
        color: '#78350f',
        textAlign: 'center',
        padding: '0.6rem 1rem',
        fontSize: '0.9rem',
        borderBottom: '1px solid #f59e0b',
      }}>
      ⚠️ <strong>Study project</strong> — an educational DDD exercise
      following real industry-standard patterns (WMS/WES/WCS, chaotic
      storage, CloudEvents). Not a production system. Not affiliated with,
      endorsed by, or representative of the reference operator or any other company.
    </div>
  );
}

function HomepageHeader() {
  const {siteConfig} = useDocusaurusContext();
  return (
    <header className={clsx('hero', styles.heroBanner)}>
      <StudyDisclaimer />
      <div className="container">
        <p className={styles.eyebrow}>
          warehouse-systems · WMS tier · Core subdomain
        </p>
        <Heading as="h1" className={styles.heroTitle}>
          {siteConfig.title}
        </Heading>
        <p className={styles.heroSubtitle}>{siteConfig.tagline}</p>
        <p className={styles.heroLead}>
          industry-style chaotic stow, bin-accurate location, and revocable
          reservations — the stock reality every other bounded context on the
          platform plans against.
        </p>
        <div className={styles.buttons}>
          <Link className="button button--primary button--lg" to="/docs/overview">
            Read the docs
          </Link>
          <Link
            className="button button--secondary button--lg"
            to="/docs/api-reference">
            API Reference
          </Link>
          <Link className="button button--secondary button--lg" to="/docs/adr">
            ADRs
          </Link>
        </div>
      </div>
    </header>
  );
}

export default function Home(): ReactNode {
  const {siteConfig} = useDocusaurusContext();
  return (
    <Layout
      title={siteConfig.title}
      description="Documentation for the Inventory & Storage bounded context: chaotic stow, bin-accurate location, revocable reservations and usable inventory.">
      <HomepageHeader />
      <main>
        <section className={styles.invariant}>
          <div className="container">
            <blockquote className={styles.invariantQuote}>
              Every physical item has exactly one known bin, <strong>or</strong>{' '}
              it is flagged <code>Unlocated</code>.
            </blockquote>
            <p className={styles.invariantCaption}>
              The invariant this entire bounded context exists to protect.{' '}
              <Link to="/docs/business-context/domain-vision">
                Why it reads that way →
              </Link>
            </p>
          </div>
        </section>
      </main>
    </Layout>
  );
}

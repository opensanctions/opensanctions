import Head from 'next/head'
import styles from '../styles/Home.module.css'

export default function Home() {
  return (
    <div className={styles.container}>
      <Head>
        <title>OpenSanctions.org</title>
        <link rel="icon" href="https://assets.pudo.org/img/favicon.ico" />
      </Head>

      <main className={styles.main}>
        <h1 className={styles.title}>
          OpenSanctions.org
        </h1>

        <p className={styles.description}>
          An open source, high quality database of international sanctions and
          persons of interest data.
        </p>

        <p className={styles.description}>
          <a className={styles.link} href="https://data.opensanctions.org/datasets/latest/index.json">data (json)</a>
          {' · '}
          <a className={styles.link} href="https://github.com/pudo/opensanctions">code</a>
        </p>
      </main>
    </div>
  )
}

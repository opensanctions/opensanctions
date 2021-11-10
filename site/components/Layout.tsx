import React from 'react';
import Head from 'next/head';
import { useRouter } from 'next/router';

import Navbar from './Navbar';
import Footer from './Footer';

import styles from '../styles/Layout.module.scss';
import { IContent } from '../lib/types';
import { BASE_URL, SITE } from '../lib/constants';

type LayoutBaseProps = {
  title?: string,
  description?: string | null,
  structured?: any,
}

function LayoutBase({ title, description, structured, children }: React.PropsWithChildren<LayoutBaseProps>) {
  const router = useRouter()
  const url = `${BASE_URL}${router.asPath}`
  return (
    <>
      <Head>
        {title && (
          <>
            <title>{title} - {SITE}</title>
            <meta property="og:title" content={title} />
            <meta property="twitter:title" content={title} />
          </>
        )}
        <link rel="apple-touch-icon" sizes="180x180" href="/static/apple-touch-icon.png" />
        <link rel="icon" type="image/png" sizes="32x32" href="/static/favicon-32x32.png" />
        <link rel="icon" type="image/png" sizes="16x16" href="/static/favicon-16x16.png" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <meta name="twitter:card" content="summary" />
        <meta name="twitter:site" content="@open_sanctions" />
        <meta name="twitter:creator" content="@pudo" />
        {!!description && (
          <>
            <meta property="og:description" content={description.trim()} />
            <meta name="description" content={description.trim()} />
          </>
        )}
        {structured && (
          <script type="application/ld+json">{JSON.stringify(structured, null, 2)}</script>
        )}
        <meta property="og:image" content="/static/card.jpg" />
        <meta name="og:site" content={SITE} />
        <meta property="og:url" content={url} />
        {/* <link rel="canonical" href={url} /> */}
      </Head>
      <div className={styles.page}>
        <Navbar />
        {children}
      </div>
      <Footer />
    </>
  )
}


type LayoutContentProps = {
  content: IContent
}


function LayoutContent({ content, children }: React.PropsWithChildren<LayoutContentProps>) {
  return (
    <LayoutBase title={content.title} description={content.summary}>
      {children}
    </LayoutBase>
  )
}

export default class Layout {
  static Base = LayoutBase;
  static Content = LayoutContent;
}
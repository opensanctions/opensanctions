import { useEffect } from 'react'
import { useRouter } from 'next/router'
import type { AppProps } from 'next/app'
import { SSRProvider } from '@react-aria/ssr';

import * as gtag from '../lib/gtag'

import '../styles/globals.scss'

export default function OpenSanctionsApp({ Component, pageProps }: AppProps) {
  const router = useRouter()
  useEffect(() => {
    const handleRouteChange = (url: string) => {
      gtag.pageview(url)
    }
    router.events.on('routeChangeComplete', handleRouteChange)
    return () => {
      router.events.off('routeChangeComplete', handleRouteChange)
    }
  }, [router.events])

  return (
    <SSRProvider>
      <Component {...pageProps} />
    </SSRProvider>
  );
}

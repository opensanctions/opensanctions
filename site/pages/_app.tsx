import '../styles/globals.scss'

import { useEffect } from 'react'
import { useRouter } from 'next/router'
import type { AppProps } from 'next/app'

import * as gtag from '../lib/gtag'

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

  return <Component {...pageProps} />
}

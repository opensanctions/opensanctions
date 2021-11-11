import fs from 'fs'
import { join } from 'path'
import { BASE_URL } from './constants';
import { IArticleInfo, IDataset, isCollection } from "./types";

const PAGES = ['/', '/docs/about/', '/docs/faq/', '/docs/usage/', '/reference/', '/contact/', '/sponsor/', '/datasets/', '/docs/', '/docs/contribute/']

const sitemapPath = join(process.cwd(), 'public', 'sitemap.xml')

function writeUrl(url: string, lastmod?: string, changefreq: string = 'weekly', priority: number = 0.5) {
  const lastmodTag = !!lastmod ? `<lastmod>${lastmod}</lastmod>` : ''
  const fullUrl = BASE_URL + url
  return `<url>
    <loc>${fullUrl}</loc>
    <priority>${priority}</priority>
    <changefreq>${changefreq}</changefreq>
    ${lastmodTag}
  </url>`
}

export default function writeSitemap(datasets: Array<IDataset>, articles: Array<IArticleInfo>, entityIds: Array<string>) {
  const urls = PAGES.map(url => writeUrl(url));
  datasets.forEach((dataset) => {
    const priority = isCollection(dataset) ? 0.9 : 0.7
    const lastmod = dataset.last_change ? dataset.last_change.split('T')[0] : undefined
    urls.push(writeUrl(`/datasets/${dataset.name}/`, lastmod, 'daily', priority))
  })
  articles.forEach((a) => {
    urls.push(writeUrl(a.path, a.date, 'weekly', 0.8))
  })
  entityIds.forEach((id) => {
    urls.push(writeUrl(`/entities/${id}/`, undefined, 'weekly', 0.3))
  })
  const body = urls.join('\n')
  const xml = `<?xml version="1.0" encoding="UTF-8"?>
    <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
      ${body}
    </urlset>`
  fs.writeFile(sitemapPath, xml, { encoding: 'utf-8' }, () => { })
}
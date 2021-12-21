import { join } from 'path'
import matter from 'gray-matter'
import { promises } from 'fs';
import { IContent, IArticleInfo, IArticle } from "./types";
import { BASE_URL } from './constants';
import { markdownToHtml } from './util';

// derived from: https://github.com/vercel/next.js/blob/canary/examples/blog-starter/lib/api.js

const contentDirectory = join(process.cwd(), '_content')
const articleDirectory = join(process.cwd(), '_articles')


export async function readContent(path: string): Promise<any> {
  const contents = await promises.readFile(path, 'utf8')
  return matter(contents)
}


export async function getContentBySlug(slug: string): Promise<IContent> {
  const realSlug = slug.replace(/\.md$/, '')
  const path = join(contentDirectory, `${realSlug}.md`)
  const { data, content } = await readContent(path)
  return {
    slug: realSlug,
    title: data.title,
    path: data.path || `/docs/${realSlug}/`,
    content: markdownToHtml(content),
    summary: data.summary || null,
  }
}

export async function getContents(): Promise<Array<IContent>> {
  const files = await promises.readdir(contentDirectory);
  const contents = await Promise.all(files.map((path) => getContentBySlug(path)))
  return contents;
}


export async function getArticleBySlug(slug: string): Promise<IArticle> {
  const realSlug = slug.replace(/\.md$/, '')
  const path = join(articleDirectory, `${realSlug}.md`)
  const { data, content } = await readContent(path)
  const date = realSlug.split('-').slice(0, 3).join('-')
  return {
    slug: realSlug,
    date: data.date || date,
    path: `/articles/${realSlug}`,
    url: `${BASE_URL}/articles/${realSlug}`,
    title: data.title || realSlug,
    draft: data.draft || false,
    content: markdownToHtml(content),
    summary: data.summary || null,
  }
}

export async function getArticles(): Promise<Array<IArticleInfo>> {
  const files = await promises.readdir(articleDirectory);
  const articles = await Promise.all(files.map((path) => getArticleBySlug(path)))
  const infos = articles.map((article) => {
    const { content, ...rest } = article;
    return rest
  }).filter((info) => !info.draft)
  return infos.sort((a, b) => (a.slug.localeCompare(b.slug) * -1))
}

export function getStaticContentProps(slug: string) {
  return async () => {
    return {
      props: {
        content: await getContentBySlug(slug)
      }
    }
  }
}
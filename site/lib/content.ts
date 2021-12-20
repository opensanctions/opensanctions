import { join } from 'path'
import matter from 'gray-matter'
import { promises } from 'fs';
import { IContent, IArticleInfo, IArticle } from "./types";
import { BASE_URL } from './constants';
import { markdownToHtml } from './util';

// derived from: https://github.com/vercel/next.js/blob/canary/examples/blog-starter/lib/api.js

const contentDirectory = join(process.cwd(), '_content')
const articleDirectory = join(contentDirectory, 'articles')


export async function readContent(filePath: string): Promise<any> {
  const fullPath = join(contentDirectory, `${filePath}.md`)
  const contents = await promises.readFile(fullPath, 'utf8')
  return matter(contents)
}


export async function getContentBySlug(slug: string): Promise<IContent> {
  const realSlug = slug.replace(/\.md$/, '')
  const { data, content } = await readContent(realSlug)
  return {
    slug: realSlug,
    title: data.title,
    content: markdownToHtml(content),
    summary: data.summary || null,
  }
}

export async function getArticleBySlug(slug: string): Promise<IArticle> {
  const realSlug = slug.replace(/\.md$/, '')
  const { data, content } = await readContent(`articles/${realSlug}`)
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
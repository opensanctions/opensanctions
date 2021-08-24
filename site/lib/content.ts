import fs from 'fs'
import { join } from 'path'
import matter from 'gray-matter'

// derived from: https://github.com/vercel/next.js/blob/canary/examples/blog-starter/lib/api.js

const contentDirectory = join(process.cwd(), '_content')

export interface IContent {
  slug: string
  content: string
  title: string
  summary: string | null
}


export async function getContentBySlug(slug: string): Promise<IContent> {
  const realSlug = slug.replace(/\.md$/, '')
  const fullPath = join(contentDirectory, `${realSlug}.md`)
  const fileContents = fs.readFileSync(fullPath, 'utf8')
  const { data, content } = matter(fileContents)
  return {
    slug: realSlug,
    title: data.title,
    content,
    summary: data.summary || null,
  }
}

export function getStaticContentProps(slug: string) {
  return async () => {
    const content = await getContentBySlug(slug)
    return {
      props: {
        content
      }
    }
  }
}
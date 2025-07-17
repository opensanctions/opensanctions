export type ServerSearchParams = { [key: string]: string | string[] | undefined }

export type PageProps = {
  searchParams?: Promise<ServerSearchParams>
  params: Promise<{ [key: string]: string }>
}

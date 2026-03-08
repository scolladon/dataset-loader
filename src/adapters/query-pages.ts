import { type QueryResult } from '../ports/types.js'

export async function* queryPages<T>(
  first: QueryResult<T>,
  queryMore: (url: string) => Promise<QueryResult<T>>
): AsyncIterable<QueryResult<T>> {
  yield first
  let page = first
  while (!page.done && page.nextRecordsUrl) {
    page = await queryMore(page.nextRecordsUrl)
    yield page
  }
}

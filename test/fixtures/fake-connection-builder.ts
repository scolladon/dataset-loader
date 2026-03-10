export type RequestHandler = (
  url: string,
  method: string,
  body?: string
) => unknown

interface Route {
  match: (url: string, method: string) => boolean
  handle: (url: string, method: string, body?: string) => unknown
}

const DEFAULT_FALLBACK = { totalSize: 0, done: true, records: [] }

export class RouteAction {
  private readonly commit: (route: Route) => FakeConnectionBuilder
  private readonly methodCheck: ((method: string) => boolean) | null
  private readonly urlIncludes: readonly string[]
  private readonly urlExcludes: readonly string[]
  private readonly customPredicate:
    | ((url: string, method: string) => boolean)
    | null

  constructor(
    commit: (route: Route) => FakeConnectionBuilder,
    options: {
      methodCheck?: (method: string) => boolean
      urlIncludes?: string[]
      urlExcludes?: string[]
      predicate?: (url: string, method: string) => boolean
    }
  ) {
    this.commit = commit
    this.methodCheck = options.methodCheck ?? null
    this.urlIncludes = [...(options.urlIncludes ?? [])]
    this.urlExcludes = [...(options.urlExcludes ?? [])]
    this.customPredicate = options.predicate ?? null
  }

  including(urlContains: string): RouteAction {
    return new RouteAction(this.commit, {
      methodCheck: this.methodCheck ?? undefined,
      urlIncludes: [...this.urlIncludes, urlContains],
      urlExcludes: [...this.urlExcludes],
      predicate: this.customPredicate ?? undefined,
    })
  }

  excluding(urlContains: string): RouteAction {
    return new RouteAction(this.commit, {
      methodCheck: this.methodCheck ?? undefined,
      urlIncludes: [...this.urlIncludes],
      urlExcludes: [...this.urlExcludes, urlContains],
      predicate: this.customPredicate ?? undefined,
    })
  }

  returns(response: unknown): FakeConnectionBuilder {
    return this.register(() => response)
  }

  throws(error: Error | string): FakeConnectionBuilder {
    const err = typeof error === 'string' ? new Error(error) : error
    return this.register(() => {
      throw err
    })
  }

  calls(fn: RequestHandler): FakeConnectionBuilder {
    return this.register(fn)
  }

  private register(handler: RequestHandler): FakeConnectionBuilder {
    return this.commit({ match: this.buildMatcher(), handle: handler })
  }

  private buildMatcher(): (url: string, method: string) => boolean {
    const methodCheck = this.methodCheck
    const urlIncludes = [...this.urlIncludes]
    const urlExcludes = [...this.urlExcludes]
    const customPredicate = this.customPredicate

    return (url: string, method: string): boolean => {
      if (customPredicate) return customPredicate(url, method)
      if (methodCheck && !methodCheck(method)) return false
      for (const pattern of urlIncludes) {
        if (!url.includes(pattern)) return false
      }
      for (const pattern of urlExcludes) {
        if (url.includes(pattern)) return false
      }
      return true
    }
  }
}

export class FakeConnectionBuilder {
  private readonly routes: Route[] = []
  private fallbackResponse: unknown = DEFAULT_FALLBACK

  when(predicate: (url: string, method: string) => boolean): RouteAction {
    return new RouteAction(this.addRoute.bind(this), { predicate })
  }

  onGet(urlContains: string): RouteAction {
    return new RouteAction(this.addRoute.bind(this), {
      methodCheck: m => m === 'GET',
      urlIncludes: [urlContains],
    })
  }

  onPost(urlContains: string): RouteAction {
    return new RouteAction(this.addRoute.bind(this), {
      methodCheck: m => m === 'POST',
      urlIncludes: [urlContains],
    })
  }

  onPatch(urlContains: string): RouteAction {
    return new RouteAction(this.addRoute.bind(this), {
      methodCheck: m => m === 'PATCH',
      urlIncludes: [urlContains],
    })
  }

  onQuery(urlContains: string): RouteAction {
    return new RouteAction(this.addRoute.bind(this), {
      methodCheck: m => m === 'GET',
      urlIncludes: ['/query', urlContains],
    })
  }

  withFallback(response: unknown): this {
    this.fallbackResponse = response
    return this
  }

  build(): RequestHandler {
    const routes = [...this.routes]
    const fallback = this.fallbackResponse

    return (url: string, method: string, body?: string): unknown => {
      for (const route of routes) {
        if (route.match(url, method)) {
          return route.handle(url, method, body)
        }
      }
      return fallback
    }
  }

  private addRoute(route: Route): FakeConnectionBuilder {
    this.routes.push(route)
    return this
  }
}

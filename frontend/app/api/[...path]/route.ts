import { proxyUpstreamRequest } from '@/lib/api'

export const runtime = 'nodejs'
export const dynamic = 'force-dynamic'

export async function GET(request: Request): Promise<Response> {
  return proxyUpstreamRequest(request)
}

export async function POST(request: Request): Promise<Response> {
  return proxyUpstreamRequest(request)
}

export async function PUT(request: Request): Promise<Response> {
  return proxyUpstreamRequest(request)
}

export async function PATCH(request: Request): Promise<Response> {
  return proxyUpstreamRequest(request)
}

export async function DELETE(request: Request): Promise<Response> {
  return proxyUpstreamRequest(request)
}

export async function HEAD(request: Request): Promise<Response> {
  return proxyUpstreamRequest(request)
}

export async function OPTIONS(request: Request): Promise<Response> {
  return proxyUpstreamRequest(request)
}

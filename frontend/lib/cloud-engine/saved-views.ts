import type { components } from "@/types/api.generated";
import { z } from "zod";

type SavedView = components["schemas"]["SavedViewRecord"];
const STORAGE_KEY = "flp-saved-views-v2:";
const viewInput = z.object({
  id: z.string().nullable().optional(),
  service_id: z.string().optional(),
  name: z.string().trim().min(1).optional(),
  filters_json: z.string().optional(),
  time_range_type: z.string().nullable().optional(),
  start_time: z.string().nullable().optional(),
  end_time: z.string().nullable().optional(),
  page: z.string().nullable().optional(),
  created_at: z.string().nullable().optional(),
});

function read(serviceId: string): SavedView[] {
  const raw = localStorage.getItem(STORAGE_KEY + encodeURIComponent(serviceId));
  if (!raw) return [];
  const parsed: unknown = JSON.parse(raw);
  if (!Array.isArray(parsed)) throw new Error("Invalid saved views");
  return parsed.filter((view): view is SavedView =>
    view?.service_id === serviceId && typeof view.id === "string",
  );
}

function error(status: number, code: string): Response {
  return Response.json({ detail: { error: code } }, { status });
}

export async function handleSavedViews(request: Request): Promise<Response | null> {
  const url = new URL(request.url);
  const match = url.pathname.match(/^\/api\/views(?:\/([^/]+))?(?:\/([^/]+))?\/?$/);
  if (!match) return null;
  const method = request.method.toUpperCase();
  const parsed = ["POST", "PUT"].includes(method)
    ? viewInput.safeParse(await request.clone().json().catch(() => null)) : null;
  if (parsed && (!parsed.success ||
      method === "POST" && (parsed.data.name === undefined || parsed.data.filters_json === undefined))) {
    return error(422, "invalid_view");
  }
  const body = parsed?.success ? parsed.data : null;
  const scopedPath = Boolean(match[2]) || (method === "GET" || method === "POST") && Boolean(match[1]);
  const serviceId = scopedPath
    ? decodeURIComponent(match[1])
    : body?.service_id || url.searchParams.get("service_id") || request.headers.get("x-service-id");
  if (!serviceId) return error(400, "service_id_required");
  if (body?.service_id && body.service_id !== serviceId) return error(400, "service_id_mismatch");
  const viewId = match[2] ? decodeURIComponent(match[2])
    : ["PUT", "DELETE"].includes(method) && match[1] ? decodeURIComponent(match[1]) : null;

  try {
    const views = read(serviceId);
    if (method === "GET" && !viewId) {
      const limit = Math.min(2000, Math.max(1, Number(url.searchParams.get("limit")) || 500));
      return Response.json(views.slice(0, limit));
    }
    if (method === "POST" && !viewId) {
      if (typeof body?.name !== "string" || typeof body.filters_json !== "string") {
        return error(422, "view_name_and_filters_required");
      }
      const id = body.id || crypto.randomUUID();
      const saved = { ...body, id, service_id: serviceId, name: body.name,
        filters_json: body.filters_json, created_at: new Date().toISOString() };
      const index = views.findIndex((view) => view.id === id);
      if (index < 0) views.push(saved);
      else views[index] = saved;
      localStorage.setItem(STORAGE_KEY + encodeURIComponent(serviceId), JSON.stringify(views));
      return Response.json({ id, status: "success" }, { status: 201 });
    }
    if (viewId && ["PUT", "DELETE"].includes(method)) {
      const index = views.findIndex((view) => view.id === viewId);
      if (index < 0) return error(404, "view_not_found");
      if (method === "DELETE") views.splice(index, 1);
      else {
        if (!body) return error(422, "invalid_view");
        views[index] = { ...views[index], ...body, id: viewId, service_id: serviceId };
      }
      localStorage.setItem(STORAGE_KEY + encodeURIComponent(serviceId), JSON.stringify(views));
      return method === "DELETE" ? new Response(null, { status: 204 })
        : Response.json({ id: viewId, status: "success" });
    }
    return error(405, "method_not_allowed");
  } catch {
    return error(503, "saved_views_storage_unavailable");
  }
}

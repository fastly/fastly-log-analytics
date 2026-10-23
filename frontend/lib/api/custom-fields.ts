import { adminFetch, client, getApiBase } from '@/lib/api'
import { unwrapClient } from '@/lib/api-client'
import type { components } from '@/types/api.generated'

export type CustomField = components["schemas"]["CustomField"]
export type CustomFieldUpdate = components["schemas"]["CustomFieldUpdate"]
export type VclLintResult = components["schemas"]["VclLintResponse"]
export type VclLintRequest = components["schemas"]["VclLintRequest"]

export const customFieldsApi = {
  listCustomFields: async (service_id: string) => {
    return await unwrapClient(
      client.GET("/api/services/{service_id}/custom-fields", {
        params: { path: { service_id } }
      }),
      "Failed to list custom fields",
    )
  },

  createCustomField: async (service_id: string, field: Omit<CustomField, "created_at" | "updated_at">) => {
    return await unwrapClient(
      client.POST("/api/services/{service_id}/custom-fields", {
        params: { path: { service_id } },
        body: field
      }),
      "Failed to create custom field",
    )
  },

  updateCustomField: async (service_id: string, field_name: string, updates: CustomFieldUpdate) => {
    return await unwrapClient(
      client.PATCH("/api/services/{service_id}/custom-fields/{field_name}", {
        params: { path: { service_id, field_name } },
        body: updates
      }),
      "Failed to update custom field",
    )
  },

  deleteCustomField: async (service_id: string, field_name: string) => {
    return await unwrapClient(
      client.DELETE("/api/services/{service_id}/custom-fields/{field_name}", {
        params: { path: { service_id, field_name } }
      }),
      "Failed to delete custom field",
    )
  },

  validateCustomVcl: async (service_id: string, body: VclLintRequest) => {
    return await unwrapClient(
      client.POST("/api/services/{service_id}/custom-fields/validate-vcl", {
        params: { path: { service_id } },
        body
      }),
      "Failed to validate VCL",
    )
  },

  exportCustomFields: async (service_id: string) => {
    // Raw fetch (not typed `client`): this endpoint returns a CSV body;
    // openapi-fetch's middleware would try to JSON-parse and corrupt
    // it. ``adminFetch`` stamps the standard client-context headers.
    const response = await adminFetch(`${getApiBase()}/api/services/${service_id}/custom-fields/export`, {
      headers: { 'x-service-id': service_id }
    });
    if (!response.ok) throw new Error("Failed to export custom fields");
    return response.blob();
  },

  importCustomFields: async (service_id: string, fields: components["schemas"]["CustomFieldsImportBody"]["custom_fields"]) => {
    return await unwrapClient(
      client.POST("/api/services/{service_id}/custom-fields/import", {
        params: { path: { service_id } },
        body: { custom_fields: fields }
      }),
      "Failed to import custom fields",
    )
  }
}

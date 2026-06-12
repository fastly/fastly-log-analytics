import { client, extractApiError } from '@/lib/api'
import type { components } from '@/types/api.generated'

export type CustomField = components["schemas"]["CustomField"]
export type CustomFieldUpdate = components["schemas"]["CustomFieldUpdate"]
export type VclLintResult = components["schemas"]["VclLintResponse"]
export type VclLintRequest = components["schemas"]["VclLintRequest"]

export const customFieldsApi = {
  listCustomFields: async (service_id: string) => {
    const { data, error } = await client.GET("/api/services/{service_id}/custom-fields", {
      params: { path: { service_id } }
    })
    if (error) throw new Error(extractApiError(error) || "Failed to list custom fields")
    return data
  },

  createCustomField: async (service_id: string, field: Omit<CustomField, "created_at" | "updated_at">) => {
    const { data, error } = await client.POST("/api/services/{service_id}/custom-fields", {
      params: { path: { service_id } },
      body: field as any
    })
    if (error) throw new Error(extractApiError(error) || "Failed to create custom field");
    return data
  },

  updateCustomField: async (service_id: string, field_name: string, updates: CustomFieldUpdate) => {
    const { data, error } = await client.PATCH("/api/services/{service_id}/custom-fields/{field_name}", {
      params: { path: { service_id, field_name } },
      body: updates as any
    })
    if (error) throw new Error(extractApiError(error) || "Failed to update custom field");
    return data
  },

  deleteCustomField: async (service_id: string, field_name: string) => {
    const { data, error } = await client.DELETE("/api/services/{service_id}/custom-fields/{field_name}", {
      params: { path: { service_id, field_name } }
    })
    if (error) throw new Error(extractApiError(error) || "Failed to delete custom field")
    return data
  },

  validateCustomVcl: async (service_id: string, body: VclLintRequest) => {
    const { data, error } = await client.POST("/api/services/{service_id}/custom-fields/validate-vcl", {
      params: { path: { service_id } },
      body
    })
    if (error) throw new Error(extractApiError(error) || "Failed to validate VCL");
    return data
  },

  exportCustomFields: async (service_id: string) => {
    // Raw fetch (not typed `client`): this endpoint returns a CSV body;
    // openapi-fetch's middleware would try to JSON-parse and corrupt it.
    const { getApiBase } = await import('@/lib/api');
    const response = await fetch(`${getApiBase()}/api/services/${service_id}/custom-fields/export`, {
      headers: { 'x-service-id': service_id }
    });
    if (!response.ok) throw new Error("Failed to export custom fields");
    return response.blob();
  },

  importCustomFields: async (service_id: string, fields: any[]) => {
    const { data, error } = await client.POST("/api/services/{service_id}/custom-fields/import", {
      params: { path: { service_id } },
      body: { custom_fields: fields } as any
    });
    if (error) throw new Error(extractApiError(error) || "Failed to import custom fields");
    return data;
  }
}

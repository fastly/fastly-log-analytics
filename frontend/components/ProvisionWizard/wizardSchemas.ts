import { z } from 'zod'

export const storageStepSchema = z.object({
  endpoint_name: z.string().min(1),
  fos_region: z.string().min(1),
  fos_bucket_name: z.string().min(1),
  log_period: z.number().int().min(1),
  edge_only: z.boolean(),
  sample_rate: z.number().min(1).max(100),
  custom_condition: z.string(),
  rum_log_period: z.number().int().min(1),
  cdn_prefix: z.string(),
  cdn_shield: z.string().min(1),
  enable_cron_sync: z.boolean(),
  delete_after: z.boolean(),
  log_retention_days: z.number().int().min(0),
  rum_retention_days: z.number().int().min(0),
  faro_version: z.string().nullable(),
  rum_custom_condition: z.string(),
})

export type StorageStepFormValues = z.infer<typeof storageStepSchema>

export const credentialsStepSchema = z.object({
  token: z.string().min(1, 'Fastly API token is required'),
})

export const domainStepSchema = z.object({
  cdn_prefix: z.string(),
  cdn_shield: z.string().min(1),
})

export const WizardDraftHeaderSchema = z.object({
  version: z.number(),
  mode: z.enum(['provision', 'ingest']),
  step: z.string(),
})

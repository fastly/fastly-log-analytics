import { join } from 'node:path'

// Do not copy process.env: deployment DSNs, credentials and uv/Python env-file
// loaders must be removed BEFORE uv starts, not after Python imports config.
const HOST_ENV_KEYS = [
  'PATH', 'HOME', 'USER', 'LOGNAME',
  'LANG', 'LC_ALL', 'LC_CTYPE', 'TZ',
  'SYSTEMROOT', 'WINDIR', 'COMSPEC', 'PATHEXT',
] as const

interface MockOAuth {
  backendPort: number
  frontendPort: number
  registryPath: string
}

export function backendTestEnvironment(
  sandbox: string,
  oauth?: MockOAuth,
): NodeJS.ProcessEnv {
  const env: NodeJS.ProcessEnv = { NODE_ENV: 'test' }
  for (const key of HOST_ENV_KEYS) {
    if (process.env[key] !== undefined) env[key] = process.env[key]
  }
  Object.assign(env, {
    UV_NO_ENV_FILE: '1',
    UV_NO_CONFIG: '1',
    PYTHON_DOTENV_DISABLED: '1',
    DEBUG_RESPONSES: 'true',
    FASTLY_MOCK_MODE: '1',
    FLA_DEV_NO_CRONS: '1',
    // The external scheduler branch is selected before the no-crons guard.
    SCHEDULER_MODE: 'inprocess',
    INGEST_MODE: 'sync',
    SERVING_MODE: 'file',
    SSE_BACKPLANE: 'local',
    OTEL_EXPORTER: 'none',
    CONTRACT_CONFIGS_DIR: join(sandbox, 'configs'),
    CONTRACT_DATA_DIR: join(sandbox, 'data'),
  })
  if (oauth) {
    Object.assign(env, {
      OAUTH_MOCK_IDP: '1',
      OAUTH_MOCK_IDP_ISSUER: `http://127.0.0.1:${oauth.backendPort}/mock-idp`,
      OAUTH_PROVIDERS_CONFIG_PATH: oauth.registryPath,
      OAUTH_FLOW_STATE_SECRET: 'e2e-oauth-flow-state-secret-0123456789',
      OAUTH_GOOGLE_CLIENT_ID: 'e2e-mock-client-id',
      OAUTH_GOOGLE_CLIENT_SECRET: 'e2e-mock-client-secret',
      OAUTH_REDIRECT_BASE: `http://127.0.0.1:${oauth.frontendPort}`,
    })
  }
  return env
}

let runtimeCloudMode = false;

export const IS_CLOUD_MODE: boolean =
  process.env.NEXT_PUBLIC_DEPLOYMENT_MODE === "cloud" ||
  process.env.NEXT_PUBLIC_STATIC_EXPORT === "1" ||
  process.env.NEXT_PUBLIC_RUNTIME_MODE === "cloud";

export function isCloudMode(): boolean {
  return IS_CLOUD_MODE || runtimeCloudMode;
}

export function setCloudModeRuntime(enabled: boolean): void {
  runtimeCloudMode = enabled;
}

export function isCloudConfigMutation(method: string, path: string): boolean {
  if (method === "POST" && ["/api/provision/execute", "/api/provision/teardown"].includes(path)) {
    return true;
  }
  if (method === "PATCH" && /^\/api\/provision\/services\/[^/]+\/ngwaf-workspace$/.test(path)) {
    return true;
  }
  if (method === "POST" && /^\/api\/admin\/services\/[^/]+\/(?:reconcile|rollback)$/.test(path)) {
    return true;
  }
  if (method === "PATCH" && /^\/api\/admin\/services\/[^/]+$/.test(path)) {
    return true;
  }
  if (method === "POST" && /^\/api\/services\/[^/]+\/rum\/(?:enable|disable|upgrade|settings)$/.test(path)) {
    return true;
  }
  if ((method === "GET" || method === "POST") && /^\/api\/services\/[^/]+\/logging-settings\/update$/.test(path)) {
    return true;
  }
  if (method === "POST" && /^\/api\/services\/[^/]+\/log-fields$/.test(path)) {
    return true;
  }
  if (/^\/api\/services\/[^/]+\/custom-fields(?:\/import|\/[^/]+)?$/.test(path)) {
    if (path.endsWith("/validate-vcl") || path.endsWith("/export")) return false;
    return ["POST", "PATCH", "DELETE"].includes(method);
  }
  if (method === "POST" && /^\/api\/services\/[^/]+\/scoring\/(?:matrix-versions\/[^/]+\/restore|matrix\/versions\/restore|enable|disable|teardown|retrain|rotate-key)$/.test(path)) {
    return true;
  }
  return ["PUT", "POST", "PATCH", "DELETE"].includes(method) &&
    /^\/api\/services\/[^/]+\/scoring\/(?:threshold|enforce-threshold|enforce-status-code|exclude-regex|labels(?:\/[^/]+)?)$/.test(path);
}

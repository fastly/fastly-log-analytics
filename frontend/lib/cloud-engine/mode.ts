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

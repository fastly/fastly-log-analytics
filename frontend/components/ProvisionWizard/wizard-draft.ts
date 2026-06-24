"use client";

import {
  INITIAL_CONFIG,
  WIZARD_DRAFT_VERSION,
  type PersistedConfig,
  type ProvisionConfig,
  type WizardDraft,
} from "./types";

export const WIZARD_DRAFT_KEY = "provision-wizard-draft-v1";

const SECRET_KEYS = ["fos_access_key", "fos_secret_key", "cdn_secret"] as const;

export function stripSecretsFromConfig(config: ProvisionConfig): PersistedConfig {
  const out: any = { ...config };
  for (const key of SECRET_KEYS) {
    delete out[key];
  }
  return out as PersistedConfig;
}

export function mergePersistedConfig(
  persisted: PersistedConfig,
): ProvisionConfig {
  return {
    ...INITIAL_CONFIG,
    ...persisted,
    fos_access_key: "",
    fos_secret_key: "",
    cdn_secret: "",
  };
}

function safeLocalStorage(): Storage | null {
  try {
    if (typeof window === "undefined") return null;
    return window.localStorage;
  } catch {
    return null;
  }
}

export function loadDraft(): WizardDraft | null {
  const ls = safeLocalStorage();
  if (!ls) return null;
  try {
    const raw = ls.getItem(WIZARD_DRAFT_KEY);
    if (!raw) return null;
    const parsed = JSON.parse(raw) as WizardDraft;
    if (!parsed || typeof parsed !== "object") return null;
    if (parsed.version !== WIZARD_DRAFT_VERSION) {
      try {
        ls.removeItem(WIZARD_DRAFT_KEY);
      } catch {
        /* ignore */
      }
      return null;
    }
    if (!parsed.mode || parsed.step === "mode") return null;
    return parsed;
  } catch {
    return null;
  }
}

export function saveDraft(draft: WizardDraft): void {
  const ls = safeLocalStorage();
  if (!ls) return;
  try {
    const sanitized: WizardDraft = {
      ...draft,
      config: stripSecretsFromConfig({
        ...INITIAL_CONFIG,
        ...(draft.config as ProvisionConfig),
      }),
    };
    ls.setItem(WIZARD_DRAFT_KEY, JSON.stringify(sanitized));
  } catch {
    /* private-mode browsers throw; intentional no-op */
  }
}

export function clearDraft(): void {
  const ls = safeLocalStorage();
  if (!ls) return;
  try {
    ls.removeItem(WIZARD_DRAFT_KEY);
  } catch {
    /* ignore */
  }
}

export function subscribeDraft(cb: (next: WizardDraft | null) => void): () => void {
  if (typeof window === "undefined") return () => undefined;
  const handler = (e: StorageEvent) => {
    if (e.key !== WIZARD_DRAFT_KEY) return;
    if (e.newValue === null) {
      cb(null);
      return;
    }
    try {
      const parsed = JSON.parse(e.newValue) as WizardDraft;
      if (parsed && parsed.version === WIZARD_DRAFT_VERSION) {
        cb(parsed);
      } else {
        cb(null);
      }
    } catch {
      cb(null);
    }
  };
  window.addEventListener("storage", handler);
  return () => window.removeEventListener("storage", handler);
}

export function makeDraftId(): string {
  if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") {
    return crypto.randomUUID();
  }
  return `draft-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
}

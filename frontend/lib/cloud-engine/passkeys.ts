import {
  base64URLStringToBuffer,
  bufferToBase64URLString,
} from "@simplewebauthn/browser";
import createClient from "openapi-fetch";
import { getApiBase, unwrapClient } from "@/lib/api";
import type { paths } from "./cloud-api.generated";

function getClient() {
  return createClient<paths>({
    baseUrl: getApiBase(),
    fetch: (req) => globalThis.fetch(req),
  });
}

function decode(value: string): Uint8Array<ArrayBuffer> {
  return new Uint8Array(base64URLStringToBuffer(value));
}

function encode(value: ArrayBuffer): string {
  return bufferToBase64URLString(value);
}

function credentialJSON(
  credential: Credential | null,
): Record<string, unknown> {
  if (!credential) throw new Error("No passkey was selected");
  const publicKey = credential as PublicKeyCredential;
  if (typeof publicKey.toJSON === "function")
    return publicKey.toJSON() as unknown as Record<string, unknown>;
  const response = publicKey.response;
  return {
    id: publicKey.id,
    rawId: encode(publicKey.rawId),
    type: publicKey.type,
    authenticatorAttachment: publicKey.authenticatorAttachment,
    clientExtensionResults: publicKey.getClientExtensionResults(),
    response: {
      clientDataJSON: encode(response.clientDataJSON),
      ...("attestationObject" in response
        ? {
            attestationObject: encode(
              (response as AuthenticatorAttestationResponse).attestationObject,
            ),
            transports:
              (
                response as AuthenticatorAttestationResponse
              ).getTransports?.() ?? [],
          }
        : {
            authenticatorData: encode(
              (response as AuthenticatorAssertionResponse).authenticatorData,
            ),
            signature: encode(
              (response as AuthenticatorAssertionResponse).signature,
            ),
            userHandle: (response as AuthenticatorAssertionResponse).userHandle
              ? encode((response as AuthenticatorAssertionResponse).userHandle!)
              : null,
          }),
    },
  };
}

export async function registerPasskey(
  displayName: string,
  inviteToken?: string,
) {
  if (!navigator.credentials?.create)
    throw new Error("Passkeys require a secure browser on this domain");
  const client = getClient();
  const challenge = await unwrapClient(
    client.POST("/api/auth/register/options", {
      body: {
        display_name: displayName,
        ...(inviteToken ? { invite_token: inviteToken } : {}),
      },
    }),
  );
  const options =
    challenge.public_key as unknown as PublicKeyCredentialCreationOptions;
  const credential = await navigator.credentials.create({
    publicKey: {
      ...options,
      challenge: decode(options.challenge as unknown as string),
      user: {
        ...options.user,
        id: decode(options.user.id as unknown as string),
      },
      excludeCredentials: options.excludeCredentials?.map((item) => ({
        ...item,
        id: decode(item.id as unknown as string),
      })),
    },
  });
  return unwrapClient(
    client.POST("/api/auth/register/verify", {
      body: {
        challenge_id: challenge.challenge_id,
        credential: credentialJSON(credential),
      },
    }),
  );
}

export async function signInWithPasskey() {
  if (!navigator.credentials?.get)
    throw new Error("Passkeys require a secure browser on this domain");
  const client = getClient();
  const challenge = await unwrapClient(
    client.POST("/api/auth/login/options", { body: {} }),
  );
  const options =
    challenge.public_key as unknown as PublicKeyCredentialRequestOptions;
  const credential = await navigator.credentials.get({
    publicKey: {
      ...options,
      challenge: decode(options.challenge as unknown as string),
      allowCredentials: options.allowCredentials?.map((item) => ({
        ...item,
        id: decode(item.id as unknown as string),
      })),
    },
  });
  return unwrapClient(
    client.POST("/api/auth/login/verify", {
      body: {
        challenge_id: challenge.challenge_id,
        credential: credentialJSON(credential),
      },
    }),
  );
}

export function passkeysSupported(): boolean {
  return (
    typeof window !== "undefined" &&
    typeof window.PublicKeyCredential !== "undefined" &&
    Boolean(navigator.credentials)
  );
}

export const authenticateWithPasskey = signInWithPasskey;

import { z } from "zod";

// Configure before schema imports so Zod never probes CSP-forbidden eval.
z.config({ jitless: true });

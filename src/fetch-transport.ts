import { AsyncLocalStorage } from "node:async_hooks";
import type { FetchOptions } from "./fetch-util.js";

export type FetchTransport = (url: string, options: FetchOptions) => Promise<Response>;

const transports = new AsyncLocalStorage<FetchTransport>();

export function withFetchTransport<T>(transport: FetchTransport, run: () => T): T {
    return transports.run(transport, run);
}

export function currentFetchTransport(): FetchTransport | undefined {
    return transports.getStore();
}

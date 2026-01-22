import { AsyncLocalStorage } from "node:async_hooks";

export const asyncLocalStorage = new AsyncLocalStorage();

export function getKey(key) {
    const store = asyncLocalStorage.getStore();
    return store && store[key];
}
export function run(context, fn) {
    asyncLocalStorage.run(context, fn);
}

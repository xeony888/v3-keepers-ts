

export async function timer(f: () => any) {
    const start = Date.now();
    const result = await f();
    const time = Date.now() - start;
    return { result, time };
}
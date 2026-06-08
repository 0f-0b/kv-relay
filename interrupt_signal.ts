const controller = new AbortController();
const abort = () => {
  Deno.removeSignalListener("SIGINT", abort);
  Deno.removeSignalListener("SIGTERM", abort);
  controller.abort(new DOMException("Interrupted", "AbortError"));
};
Deno.addSignalListener("SIGINT", abort);
Deno.addSignalListener("SIGTERM", abort);
export const signal = controller.signal;

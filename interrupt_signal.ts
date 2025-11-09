const controller = new AbortController();
Deno.addSignalListener("SIGINT", function abort() {
  Deno.removeSignalListener("SIGINT", abort);
  controller.abort(new DOMException("Interrupted", "AbortError"));
});
export const signal = controller.signal;

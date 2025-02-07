export interface SerializerOptions {
  hostObjects?: readonly object[];
  transferredArrayBuffers?: (ArrayBuffer | number)[];
  forStorage?: boolean;
}

export const { deserialize, serialize } =
  // @ts-ignore Accessing Deno internals
  Deno[Deno.internal].core as {
    readonly deserialize: (
      buffer: Uint8Array,
      options?: SerializerOptions,
    ) => unknown;
    readonly serialize: (
      value: unknown,
      options?: SerializerOptions,
      errorCallback?: (message: string) => unknown,
    ) => Uint8Array<ArrayBuffer>;
  };

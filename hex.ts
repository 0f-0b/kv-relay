const hexTable = (() => {
  const alphabet = "0123456789abcdef";
  const table: string[] = [];
  for (let i = 0; i < 256; i++) {
    table.push(alphabet[i >> 4] + alphabet[i & 0xf]);
  }
  return table;
})();

export function encodeHex(buf: Uint8Array): string {
  let result = "";
  for (const b of buf) {
    result += hexTable[b];
  }
  return result;
}

export function decodeHex(str: string): Uint8Array {
  if (!/^(?:[0-9A-Fa-f]{2})*$/.test(str = str.replace(/\s/g, ""))) {
    throw new TypeError("Invalid hex string");
  }
  return Uint8Array.from(
    { length: str.length / 2 },
    (_, i) => parseInt(str.substring(i * 2, (i + 1) * 2), 16),
  );
}

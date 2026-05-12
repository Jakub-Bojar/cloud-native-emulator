// Allocate and touch pages every 4KiB so the kernel actually commits them
// (a fresh Buffer.alloc is zero-filled but may not be backed by physical pages
// until written).
export function holdRam(mb: number): Buffer {
  const blob = Buffer.alloc(mb * 1024 * 1024);
  for (let i = 0; i < blob.length; i += 4096) blob[i] = 1;
  return blob;
}

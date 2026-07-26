package org.elece.index.filter;

/**
 * Minimal implementation of the 128-bit Murmur3 hash (x64 variant), used to derive the pair of independent hash values
 * that the {@link BloomFilter} feeds into its double hashing scheme. Hashing the serialized bytes of a key (rather than
 * relying on {@link Object#hashCode()}) yields a stable, well distributed digest across restarts and across key types.
 */
final class Murmur3 {
    private static final long C1 = 0x87c37b91114253d5L;
    private static final long C2 = 0x4cf5ad432745937fL;

    private Murmur3() {
        // private constructor
    }

    static long[] hash128(byte[] data, int seed) {
        int length = data.length;
        long h1 = seed & 0xFFFFFFFFL;
        long h2 = seed & 0xFFFFFFFFL;
        int blockCount = length >> 4;

        for (int block = 0; block < blockCount; block++) {
            int base = block << 4;
            long k1 = getLongLittleEndian(data, base);
            long k2 = getLongLittleEndian(data, base + 8);

            k1 *= C1;
            k1 = Long.rotateLeft(k1, 31);
            k1 *= C2;
            h1 ^= k1;
            h1 = Long.rotateLeft(h1, 27);
            h1 += h2;
            h1 = h1 * 5 + 0x52dce729L;

            k2 *= C2;
            k2 = Long.rotateLeft(k2, 33);
            k2 *= C1;
            h2 ^= k2;
            h2 = Long.rotateLeft(h2, 31);
            h2 += h1;
            h2 = h2 * 5 + 0x38495ab5L;
        }

        long k1 = 0;
        long k2 = 0;
        int tail = blockCount << 4;
        int remaining = length & 15;
        for (int index = 0; index < remaining; index++) {
            long value = (long) data[tail + index] & 0xff;
            if (index < 8) {
                k1 |= value << (8 * index);
            } else {
                k2 |= value << (8 * (index - 8));
            }
        }
        if (remaining > 8) {
            k2 *= C2;
            k2 = Long.rotateLeft(k2, 33);
            k2 *= C1;
            h2 ^= k2;
        }
        if (remaining > 0) {
            k1 *= C1;
            k1 = Long.rotateLeft(k1, 31);
            k1 *= C2;
            h1 ^= k1;
        }

        h1 ^= length;
        h2 ^= length;
        h1 += h2;
        h2 += h1;
        h1 = finalMix(h1);
        h2 = finalMix(h2);
        h1 += h2;
        h2 += h1;

        return new long[]{h1, h2};
    }

    private static long getLongLittleEndian(byte[] data, int index) {
        return ((long) data[index] & 0xff)
                | (((long) data[index + 1] & 0xff) << 8)
                | (((long) data[index + 2] & 0xff) << 16)
                | (((long) data[index + 3] & 0xff) << 24)
                | (((long) data[index + 4] & 0xff) << 32)
                | (((long) data[index + 5] & 0xff) << 40)
                | (((long) data[index + 6] & 0xff) << 48)
                | (((long) data[index + 7] & 0xff) << 56);
    }

    private static long finalMix(long value) {
        value ^= value >>> 33;
        value *= 0xff51afd7ed558ccdL;
        value ^= value >>> 33;
        value *= 0xc4ceb9fe1a85ec53L;
        value ^= value >>> 33;
        return value;
    }
}

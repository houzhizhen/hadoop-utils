package com.baidu.hadoop.hdfs.ec.rs;

import java.util.Arrays;

/**
 * Self-contained RS(4,2) demo using simple GF(2^8) log/antilog tables
 * (adapted from the ReedSolomon toy implementation).
 */
public class RS42Example {
// --- GF helpers (same primitive poly 0x11d) ---
static final int GF_SIZE = 256;
static final int PRIMITIVE = 0x11d;
static int[] alphaTo = new int[GF_SIZE];
static int[] indexOf = new int[GF_SIZE];
static {
    int x = 1;
    for (int i = 0; i < GF_SIZE - 1; i++) {
        alphaTo[i] = x;
        indexOf[x] = i;
        x <<= 1;
        if ((x & 0x100) != 0) x ^= PRIMITIVE;
    }
    indexOf[0] = -1;
}
static int gfMul(int a, int b) {
    if (a == 0 || b == 0) return 0;
    int s = indexOf[a] + indexOf[b];
    if (s >= GF_SIZE - 1) s -= (GF_SIZE - 1);
    return alphaTo[s];
}
static int gfDiv(int a, int b) {
    if (a == 0) return 0;
    if (b == 0) throw new ArithmeticException("div by zero");
    int d = indexOf[a] - indexOf[b];
    if (d < 0) d += (GF_SIZE - 1);
    return alphaTo[d];
}

// --- Build a simple Vandermonde parity matrix (m x k) ---
static int[][] buildParityMatrix(int k, int m) {
    int[][] mat = new int[m][k];
    for (int r = 0; r < m; r++) {
        for (int c = 0; c < k; c++) {
            mat[r][c] = alphaTo[((r + 1) * c) % (GF_SIZE - 1)];
        }
    }
    return mat;
}

// --- Encode: produce parity blocks ---
static byte[][] encode(int k, int m, byte[][] data) {
    int len = data[0].length;
    int[][] parityMat = buildParityMatrix(k, m);
    byte[][] parity = new byte[m][len];
    for (int r = 0; r < m; r++) {
        for (int i = 0; i < len; i++) {
            int v = 0;
            for (int c = 0; c < k; c++) {
                v ^= gfMul(parityMat[r][c], data[c][i] & 0xFF);
            }
            parity[r][i] = (byte) v;
        }
    }
    return parity;
}

// --- Invert k x k matrix over GF(2^8) via Gaussian elimination ---
static int[][] invertMatrix(int[][] A) {
    int k = A.length;
    int[][] aug = new int[k][2 * k];
    for (int i = 0; i < k; i++) {
        System.arraycopy(A[i], 0, aug[i], 0, k);
        aug[i][i + k] = 1;
    }
    for (int i = 0; i < k; i++) {
        if (aug[i][i] == 0) {
            int swap = i;
            for (int r = i + 1; r < k; r++) if (aug[r][i] != 0) { swap = r; break; }
            if (swap != i) { int[] tmp = aug[i]; aug[i] = aug[swap]; aug[swap] = tmp; }
        }
        int inv = gfDiv(1, aug[i][i]);
        for (int c = 0; c < 2 * k; c++) aug[i][c] = gfMul(aug[i][c], inv);
        for (int r = 0; r < k; r++) {
            if (r == i) continue;
            int factor = aug[r][i];
            if (factor != 0) {
                for (int c = 0; c < 2 * k; c++) {
                    aug[r][c] ^= gfMul(factor, aug[i][c]);
                }
            }
        }
    }
    int[][] inv = new int[k][k];
    for (int i = 0; i < k; i++) System.arraycopy(aug[i], k, inv[i], 0, k);
    return inv;
}

// --- Decode from any k available shards (availableIndices length == k) ---
static byte[][] decode(int k, int m, byte[][] available, int[] availableIndices) {
    int blockLen = available[0].length;
    // Build kxk Vandermonde matrix using indices
    int[][] A = new int[k][k];
    for (int i = 0; i < k; i++) {
        int idx = availableIndices[i];
        for (int j = 0; j < k; j++) {
            A[i][j] = alphaTo[(idx * j) % (GF_SIZE - 1)];
        }
    }
    int[][] invA = invertMatrix(A);
    // Multiply invA by available to get original k data shards
    byte[][] data = new byte[k][blockLen];
    for (int i = 0; i < k; i++) {
        for (int b = 0; b < blockLen; b++) {
            int val = 0;
            for (int j = 0; j < k; j++) {
                val ^= gfMul(invA[i][j], available[j][b] & 0xFF);
            }
            data[i][b] = (byte) val;
        }
    }
    return data;
}

// --- Demo main: RS(4,2) ---
public static void main(String[] args) {
    int k = 4, m = 2;          // RS(4,2) -> n = 6
    int shardSize = 12;       // bytes per shard (small for demo)

    // create 4 data shards
    byte[][] data = new byte[k][shardSize];
    for (int i = 0; i < k; i++) {
        for (int j = 0; j < shardSize; j++) data[i][j] = (byte) ( (i+1) * (j+1) & 0xFF );
    }

    System.out.println("Original Data Shards:");
    for (int i = 0; i < k; i++) System.out.println("D"+i+" = " + Arrays.toString(data[i]));

    // encode parity
    byte[][] parity = encode(k, m, data);
    for (int i = 0; i < m; i++) System.out.println("P"+i+" = " + Arrays.toString(parity[i]));

    // build full shard array: indices 0..3 data, 4..5 parity
    byte[][] shards = new byte[k + m][];
    for (int i = 0; i < k; i++) shards[i] = data[i];
    for (int i = 0; i < m; i++) shards[k + i] = parity[i];

    // simulate losing two shards, e.g., lose D1 (index 1) and P0 (index 4)
    boolean[] present = new boolean[k + m];
    Arrays.fill(present, true);
    present[1] = false;  // lose data shard D1
    present[4] = false;  // lose parity shard P0

    System.out.println("\nSimulate lost shards: indices 1 and 4");

    // collect ANY k available shards and their indices
    byte[][] available = new byte[k][];
    int[] availIdx = new int[k];
    int ptr = 0;
    for (int i = 0; i < k + m && ptr < k; i++) {
        if (present[i]) {
            available[ptr] = shards[i];
            availIdx[ptr] = i;
            ptr++;
        }
    }

    System.out.println("Available shard indices used for decode: " + Arrays.toString(availIdx));

    // decode -> recover original k data shards
    byte[][] recoveredData = decode(k, m, available, availIdx);

    System.out.println("\nRecovered Data Shards:");
    for (int i = 0; i < k; i++) System.out.println("D"+i+" = " + Arrays.toString(recoveredData[i]));

    // Optional: verify recovered equals original
    boolean ok = true;
    for (int i = 0; i < k; i++) if (!Arrays.equals(recoveredData[i], data[i])) ok = false;
    System.out.println("\nRecovery success: " + ok);
}
}
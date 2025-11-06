package com.baidu.hadoop.hdfs.ec.rs;

import java.util.Arrays;

/**
 * Simple Reed-Solomon implementation over GF(2^8)
 * RS(n,k) => n total symbols, k data symbols, n-k parity symbols
 */
public class ReedSolomon {
private final int n;        // total symbols
private final int k;        // data symbols
private final int m;        // parity symbols
private final int[] alphaTo;  // anti-log table
private final int[] indexOf;  // log table
private final int gfSize = 256;
private final int primitivePoly = 0x11d;  // AES / common primitive polynomial for GF(2^8)
private final int[][] generator;  // parity generator matrix (m x k)

public ReedSolomon(int k, int m) {
    this.k = k;
    this.m = m;
    this.n = k + m;
    this.alphaTo = new int[gfSize];
    this.indexOf = new int[gfSize];
    initTables();
    this.generator = buildGeneratorMatrix();
}

/** Initialize GF(2^8) log/antilog tables */
private void initTables() {
    int x = 1;
    for (int i = 0; i < gfSize - 1; i++) {
        alphaTo[i] = x;
        indexOf[x] = i;
        x <<= 1;
        if ((x & 0x100) != 0) {
            x ^= primitivePoly;
        }
    }
    indexOf[0] = -1;
}

/** Multiply in GF(2^8) */
private int gfMul(int a, int b) {
    if (a == 0 || b == 0) return 0;
    int sum = indexOf[a] + indexOf[b];
    if (sum >= gfSize - 1) sum -= (gfSize - 1);
    return alphaTo[sum];
}

/** Divide in GF(2^8) */
private int gfDiv(int a, int b) {
    if (a == 0) return 0;
    if (b == 0) throw new ArithmeticException("divide by zero");
    int diff = indexOf[a] - indexOf[b];
    if (diff < 0) diff += (gfSize - 1);
    return alphaTo[diff];
}

/** Build a simple Vandermonde generator matrix for parity (systematic RS) */
private int[][] buildGeneratorMatrix() {
    int[][] matrix = new int[m][k];
    for (int r = 0; r < m; r++) {
        for (int c = 0; c < k; c++) {
            matrix[r][c] = alphaTo[(r + 1) * c % (gfSize - 1)];
        }
    }
    return matrix;
}

/** Encode: compute parity bytes given data bytes */
public byte[][] encode(byte[][] dataBlocks) {
    if (dataBlocks.length != k) throw new IllegalArgumentException("dataBlocks != k");
    int blockLen = dataBlocks[0].length;
    byte[][] parity = new byte[m][blockLen];

    for (int j = 0; j < m; j++) {
        for (int b = 0; b < blockLen; b++) {
            int sum = 0;
            for (int i = 0; i < k; i++) {
                int mul = gfMul(generator[j][i], dataBlocks[i][b] & 0xFF);
                sum ^= mul;
            }
            parity[j][b] = (byte) sum;
        }
    }
    return parity;
}

/**
 * Decode from any k available blocks (data or parity)
 * knownIndices[] = indices (0..n-1) of the available blocks
 */
public byte[][] decode(byte[][] availableBlocks, int[] knownIndices) {
    if (availableBlocks.length != k) throw new IllegalArgumentException("need exactly k available blocks");
    int blockLen = availableBlocks[0].length;

    // Build Vandermonde matrix for known positions
    int[][] A = new int[k][k];
    for (int i = 0; i < k; i++) {
        int idx = knownIndices[i];
        for (int j = 0; j < k; j++) {
            A[i][j] = alphaTo[(idx * j) % (gfSize - 1)];
        }
    }

    // Invert A (Gaussian elimination over GF)
    int[][] invA = invertMatrix(A);

    // Recover original k data blocks
    byte[][] data = new byte[k][blockLen];
    for (int i = 0; i < k; i++) {
        for (int b = 0; b < blockLen; b++) {
            int val = 0;
            for (int j = 0; j < k; j++) {
                int mul = gfMul(invA[i][j], availableBlocks[j][b] & 0xFF);
                val ^= mul;
            }
            data[i][b] = (byte) val;
        }
    }
    return data;
}

/** Gaussian elimination matrix inversion over GF(2^8) */
private int[][] invertMatrix(int[][] A) {
    int k = A.length;
    int[][] aug = new int[k][2 * k];
    for (int i = 0; i < k; i++) {
        System.arraycopy(A[i], 0, aug[i], 0, k);
        aug[i][i + k] = 1;
    }

    // Forward elimination
    for (int i = 0; i < k; i++) {
        if (aug[i][i] == 0) {
            // swap
            for (int r = i + 1; r < k; r++) {
                if (aug[r][i] != 0) {
                    int[] tmp = aug[i];
                    aug[i] = aug[r];
                    aug[r] = tmp;
                    break;
                }
            }
        }
        int inv = gfDiv(1, aug[i][i]);
        for (int j = 0; j < 2 * k; j++) {
            aug[i][j] = gfMul(aug[i][j], inv);
        }
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
    for (int i = 0; i < k; i++) {
        System.arraycopy(aug[i], k, inv[i], 0, k);
    }
    return inv;
}

// --- Demo ---
public static void main(String[] args) {
    int k = 4, m = 2; // RS(6,4)
    ReedSolomon rs = new ReedSolomon(k, m);

    // Create example data
    byte[][] data = new byte[k][10];
    for (int i = 0; i < k; i++) {
        Arrays.fill(data[i], (byte) (i + 1));
    }

    // Encode
    byte[][] parity = rs.encode(data);
    System.out.println("Parity blocks:");
    for (byte[] p : parity) System.out.println(Arrays.toString(p));

    // Simulate losing block 1 and 4 (two erasures)
    byte[][] available = { data[0], data[2], data[3], parity[0] };
    int[] indices = { 0, 2, 3, 4 }; // which blocks we have

    // Decode (reconstruct original 4 data)
    byte[][] recovered = rs.decode(available, indices);
    System.out.println("Recovered data:");
    for (byte[] r : recovered) System.out.println(Arrays.toString(r));
}
}
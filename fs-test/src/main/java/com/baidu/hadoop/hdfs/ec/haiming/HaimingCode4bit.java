package com.baidu.hadoop.hdfs.ec.haiming;


import java.util.Scanner;

public class HaimingCode4bit {

// Generate Hamming(7,4) code from 4 data bits
public static int[] generateCode(int[] dataBits) {
    int[] code = new int[7];

    // Positions: [1,2,3,4,5,6,7]
    // Parity bits at positions 1, 2, 4
    code[2] = dataBits[0];
    code[4] = dataBits[1];
    code[5] = dataBits[2];
    code[6] = dataBits[3];

    // Calculate parity bits
    code[0] = code[2] ^ code[4] ^ code[6]; // p1
    code[1] = code[2] ^ code[5] ^ code[6]; // p2
    code[3] = code[4] ^ code[5] ^ code[6]; // p4

    return code;
}

// Detect and correct a single-bit error
public static int detectAndCorrect(int[] code) {
    int p1 = code[0] ^ code[2] ^ code[4] ^ code[6];
    int p2 = code[1] ^ code[2] ^ code[5] ^ code[6];
    int p4 = code[3] ^ code[4] ^ code[5] ^ code[6];

    int errorPosition = (p4 << 2) + (p2 << 1) + p1;

    if (errorPosition != 0) {
        System.out.println("Error detected at position: " + errorPosition);
        // Correct it
        code[errorPosition - 1] ^= 1;
        System.out.println("Error corrected.");
    } else {
        System.out.println("No error detected.");
    }

    return errorPosition;
}

public static void main(String[] args) {

    int[] dataBits = new int[]{1, 0 , 0, 1};

    int[] code = generateCode(dataBits);
    System.out.print("Generated Hamming Code: ");
    for (int bit : code) System.out.print(bit);
    System.out.println();

    // Introduce an error for demo
    code[4] ^= 1; // flip one bit
    System.out.print("Received (with error):  ");
    for (int bit : code) System.out.print(bit);
    System.out.println();

    detectAndCorrect(code);
    System.out.print("Corrected code: ");
    for (int bit : code) System.out.print(bit);
    System.out.println();
}
}

package com.baidu.hadoop.hdfs.ec.haiming;

public class HammingCode {

// 编码函数：将数据位编码为海明码
public static int[] encode(int[] data) {
    int n = data.length;
    int r = 0;
    // 确定校验位数量 r（满足 2^r >= n + r + 1）
    while (Math.pow(2, r) < n + r + 1) {
        r++;
    }
    int totalLength = n + r;
    int[] hammingCode = new int[totalLength];
    int dataIndex = 0;
    int codeIndex = 0;

    // 填充数据位和校验位位置
    for (int i = 0; i < totalLength; i++) {
        if (isPowerOfTwo(i + 1)) {
            // 校验位位置（跳过）
            hammingCode[i] = 0; // 初始化为0，后续计算
        } else {
            // 数据位位置
            hammingCode[i] = data[dataIndex++];
        }
    }

    // 计算校验位
    for (int i = 0; i < r; i++) {
        int parity = 0;
        for (int j = 0; j < totalLength; j++) {
            // 检查位置j是否被第i个校验位覆盖
            if ((j + 1 == 1) && (1 << i == 1)) {
                parity ^= hammingCode[j];
            }
        }
        hammingCode[(1 << i) - 1] = parity; // 校验位位于2^i - 1位置
    }

    return hammingCode;
}

// 解码函数：从海明码解码并纠正单比特错误
public static int[] decode(int[] hammingCode) {
    int totalLength = hammingCode.length;
    int r = (int) (Math.log(totalLength) / Math.log(2)) + 1; // 校验位数量
    int n = totalLength - r; // 数据位数量
    int[] correctedCode = new int[totalLength];
    System.arraycopy(hammingCode, 0, correctedCode, 0, totalLength);

    // 计算校正因子（Syndrome）
    int syndrome = 0;
    for (int i = 0; i < r; i++) {
        int parity = 0;
        for (int j = 0; j < totalLength; j++) {
            if ((j + 1 == 1) & (1 << i == 1)) {
                parity ^= correctedCode[j];
            }
        }
        syndrome |= (parity << i); // 拼接校正因子
    }

    // 纠正错误（如果校正因子非零）
    if (syndrome != 0) {
        // 翻转错误位
        correctedCode[syndrome - 1] ^= 1;
    }

    // 提取数据位（跳过校验位位置）
    int[] data = new int[n];
    int dataIndex = 0;
    for (int i = 0; i < totalLength; i++) {
        if (!isPowerOfTwo(i + 1)) {
            data[dataIndex++] = correctedCode[i];
        }
    }
    return data;
}

// 辅助函数：检查是否为2的幂
private static boolean isPowerOfTwo(int num) {
    return (num & (num - 1)) == 0 && num != 0;
}

// 测试示例
public static void main(String[] args) {
    // 示例数据：6位数据 101101
    int[] data = {1, 0, 1, 1, 0, 1};
    System.out.println("原始数据: ");
    printArray(data);

    // 编码
    int[] encoded = encode(data);
    System.out.println("海明码: ");
    printArray(encoded);

    // 模拟错误：翻转第5位（索引4）
    int[] received = encoded.clone();
    received[4] ^= 1; // 翻转第5位（索引从0开始，实际是第5位）
    System.out.println("接收到的海明码（含错误）: ");
    printArray(received);

    // 解码并纠正错误
    int[] decoded = decode(received);
    System.out.println("解码后的数据: ");
    printArray(decoded);
}

// 辅助函数：打印数组
private static void printArray(int[] arr) {
    for (int num : arr) {
        System.out.print(num);
    }
    System.out.println();
}
}
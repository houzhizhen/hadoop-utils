package com.baidu.io;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.GZIPOutputStream;

public class EmptyGzipDemo {
public static void main(String[] args) throws Exception {
    Path emptyGz = Files.createTempFile("empty-", ".gz");
    Path oneByteGz = Files.createTempFile("onebyte-", ".gz");

    writeGzip(emptyGz, new byte[0]);
    writeGzip(oneByteGz, new byte[]{'a'});

    System.out.println("empty.gz   = " + Files.size(emptyGz) + " bytes, path=" + emptyGz);
    System.out.println("onebyte.gz = " + Files.size(oneByteGz) + " bytes, path=" + oneByteGz);
}

private static void writeGzip(Path path, byte[] data) throws IOException {
    try (OutputStream os = Files.newOutputStream(path);
         GZIPOutputStream gos = new GZIPOutputStream(os)) {
        if (data.length > 0) {
            gos.write(data);
        }
    }
}
}

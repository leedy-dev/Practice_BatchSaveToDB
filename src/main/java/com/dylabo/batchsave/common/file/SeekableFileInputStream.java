package com.dylabo.batchsave.common.file;

import org.apache.parquet.io.SeekableInputStream;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class SeekableFileInputStream extends SeekableInputStream {

    private final FileInputStream fileInputStream;
    private final FileChannel fileChannel;

    public SeekableFileInputStream(FileInputStream fileInputStream) {
        this.fileInputStream = fileInputStream;
        this.fileChannel = fileInputStream.getChannel();
    }

    @Override
    public long getPos() throws IOException {
        return fileChannel.position();
    }

    @Override
    public void seek(long newPos) throws IOException {
        fileChannel.position(newPos);
    }

    @Override
    public int read() throws IOException {
        return fileInputStream.read();
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return fileInputStream.read(b, off, len);
    }

    @Override
    public int read(ByteBuffer byteBuffer) throws IOException {
        return fileChannel.read(byteBuffer);
    }

    @Override
    public void readFully(byte[] b) throws IOException {
        int bytesRead = 0;
        while (bytesRead < b.length) {
            int result = fileInputStream.read(b, bytesRead, b.length - bytesRead);
            if (result == -1) {
                throw new IOException("Reached end of file before reading fully");
            }
            bytesRead += result;
        }
    }

    @Override
    public void readFully(byte[] b, int off, int len) throws IOException {
        int bytesRead = 0;
        while (bytesRead < len) {
            int result = fileInputStream.read(b, off + bytesRead, len - bytesRead);
            if (result == -1) {
                throw new IOException("Reached end of file before reading fully");
            }
            bytesRead += result;
        }
    }

    @Override
    public void readFully(ByteBuffer buffer) throws IOException {
        while (buffer.remaining() > 0) {
            int bytesRead = fileChannel.read(buffer);
            if (bytesRead == -1) {
                throw new IOException("Reached end of file before fully reading ByteBuffer");
            }
        }
    }

    @Override
    public void close() throws IOException {
        fileInputStream.close();
    }


}
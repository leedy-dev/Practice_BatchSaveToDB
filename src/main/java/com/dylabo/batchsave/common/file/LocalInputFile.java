package com.dylabo.batchsave.common.file;

import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.schema.MessageType;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

public class LocalInputFile implements InputFile {

    private final File file;

    public LocalInputFile(File file) {
        this.file = file;
    }

    @Override
    public long getLength() throws IOException {
        return file.length(); // 파일 크기 반환
    }

    @Override
    public SeekableInputStream newStream() throws IOException {
        return new SeekableFileInputStream(new FileInputStream(file)); // 스트림 생성
    }

    public static void main(String[] args) throws Exception {
        File parquetFile = new File("path/to/parquet-file.parquet");
        InputFile inputFile = new LocalInputFile(parquetFile);

        try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
            MessageType schema = reader.getFileMetaData().getSchema();
            System.out.println("Parquet Schema: " + schema);
        }
    }
}
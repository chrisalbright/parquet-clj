package com.cj.data.parquet;

import org.apache.parquet.hadoop.ParquetReader;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicReference;

public class ParquetIterable<T> implements Iterable<T> {

    private final ParquetReader<T> reader;
    private final AtomicReference<Iterator<T>> iterator = new AtomicReference<>();

    public ParquetIterable(ParquetReader<T> reader) {
        this.reader = reader;
    }

    public Iterator<T> iterator() {

        iterator.compareAndSet(null, new Iterator<T>() {

            private final int CHUNK_SIZE = 32;
            private int chunkIndex = 0;
            private int chunksLoaded = 0;
            private T[] chunks = (T[]) new Object[CHUNK_SIZE];

            {
                loadChunks();
            }

            private T read(ParquetReader<T> reader) {
                T r;
                try {
                    r = reader.read();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                return r;
            }

            private void loadChunks() {
                chunksLoaded = 0;
                chunkIndex = 0;
                for (int i = 0; i < CHUNK_SIZE; i++) {
                    final T r = read(reader);
                    if (r == null) break;
                    chunks[i] = r;
                    chunksLoaded++;
                }
            }

            public boolean hasNext() {
                if (chunksLoaded == CHUNK_SIZE && chunkIndex >= chunksLoaded) {
                    loadChunks();
                }
                return chunkIndex < chunksLoaded;
            }

            public T next() {
                return chunks[chunkIndex++];
            }

        });

        return iterator.get();
    }

}

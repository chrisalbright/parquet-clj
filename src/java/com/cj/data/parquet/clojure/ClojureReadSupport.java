package com.cj.data.parquet.clojure;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

import java.util.Map;

public class ClojureReadSupport extends ReadSupport {
    @Override
    public RecordMaterializer prepareForRead(Configuration configuration, Map keyValueMetaData, MessageType fileSchema, ReadContext readContext) {
        return new ClojureRecordMaterializer(fileSchema);
    }

    @Override
    public ReadContext init(InitContext context) {
        return new ReadContext(context.getFileSchema());
    }

}

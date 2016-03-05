package com.cj.data.parquet.clojure;

import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

public class ClojureRecordMaterializer extends RecordMaterializer {

    public final ClojureRecordConverter root;

    public ClojureRecordMaterializer(MessageType fileSchema) {
        this.root = new ClojureRecordConverter(fileSchema);
    }

    @Override
    public Object getCurrentRecord() {
        return root.getCurrentRecord();
    }

    @Override
    public GroupConverter getRootConverter() {
        return root;
    }
}

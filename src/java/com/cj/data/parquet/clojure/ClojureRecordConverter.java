package com.cj.data.parquet.clojure;

import clojure.lang.IPersistentMap;
import clojure.lang.Keyword;
import clojure.lang.PersistentArrayMap;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.Type;

public class ClojureRecordConverter extends GroupConverter {

    private final Converter converters[];
    private final ClojureRecordConverter parent;
    private final String name;
    protected IPersistentMap record;

    public ClojureRecordConverter(GroupType schema) {
        this(schema, null, null);
    }

    public ClojureRecordConverter(GroupType schema, String name, ClojureRecordConverter parent) {
        this.converters = new Converter[schema.getFieldCount()];
        this.parent = parent;
        this.name = name;

        int i = 0;
        for (Type field : schema.getFields()) {
            converters[i++] = createConverter(field);
        }
    }

    private Converter createConverter(Type field) {
        OriginalType otype = field.getOriginalType();
        if (field.isPrimitive()) {
            if (otype != null) {
                switch (otype) {
                    case UTF8:
                        return new StringConverter(field.getName());
                }
            }
            return new ClojurePrimitiveConverter(field.getName());
        }

        GroupType groupType = field.asGroupType();
        if (otype != null) {
            switch (otype) {
                case MAP:
                case LIST:
                    return new ClojureRecordConverter(groupType, field.getName(), this);
            }
        }
        return new ClojureRecordConverter(groupType, field.getName(), this);
    }

    private class ClojurePrimitiveConverter extends PrimitiveConverter {
        protected final Keyword name;

        public ClojurePrimitiveConverter(String name) {
            this.name = Keyword.intern(name);
        }

        @Override
        public void addBinary(Binary value) {
            record = record.assoc(name, value.getBytes());
        }

        @Override
        public void addBoolean(boolean value) {
            record = record.assoc(name, value);
        }

        @Override
        public void addDouble(double value) {
            record = record.assoc(name, value);
        }

        @Override
        public void addFloat(float value) {
            record = record.assoc(name, value);
        }

        @Override
        public void addInt(int value) {
            record = record.assoc(name, value);
        }

        @Override
        public void addLong(long value) {
            record = record.assoc(name, value);
        }
    }

    private class StringConverter extends ClojurePrimitiveConverter {
        public StringConverter(String name) {
            super(name);
        }

        @Override
        public void addBinary(Binary value) {
            record = record.assoc(name, value.toStringUsingUTF8());
        }
    }

    @Override
    public Converter getConverter(int fieldIndex) {
        return converters[fieldIndex];
    }

    @Override
    public void start() {
        record = PersistentArrayMap.EMPTY;
    }

    @Override
    public void end() {
        if (parent != null) {
            parent.record = parent.record.assoc(name, record);
        }
    }

    public IPersistentMap getCurrentRecord() {
        return record;
    }
}

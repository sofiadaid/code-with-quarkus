package org.acme.model;

public class Column {
    public String name;
    public DataType type;

    public Column() {} // obligatoire pour JSON

    public Column(String name, DataType type) {
        this.name = name;
        this.type = type;
    }
    public DataType getType() {
        return type;
    }
}

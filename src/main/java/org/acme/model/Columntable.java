package org.acme.model;

public class Columntable {
    public String name;
    public DataType type;

    public Columntable() {} // obligatoire pour JSON

    public Columntable(String name, DataType type) {
        this.name = name;
        this.type = type;
    }
}

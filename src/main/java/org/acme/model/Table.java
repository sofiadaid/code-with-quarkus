package org.acme.model;

import java.util.List;
public class Table{
    public String name;
    public List<Columntable> columns;

    public Table(){}

    public Table(String name,List<Columntable> columns){
        this.name=name;
        this.columns=columns;
    }
}

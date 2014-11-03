// =======================================================
// Copyright Mylife.com Inc., 2013. All rights reserved.
//
// =======================================================

package com.mylife.hbase.mapper.model;

public class LabeledPoint {

    private String label;
    private int x;
    private int y;

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((label == null) ? 0 : label.hashCode());
        result = prime * result + x;
        result = prime * result + y;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        LabeledPoint other = (LabeledPoint) obj;
        if (label == null) {
            if (other.label != null)
                return false;
        } else if (!label.equals(other.label))
            return false;
        if (x != other.x)
            return false;
        if (y != other.y)
            return false;
        return true;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public int getX() {
        return x;
    }

    public void setX(int x) {
        this.x = x;
    }

    public int getY() {
        return y;
    }

    public void setY(int y) {
        this.y = y;
    }

    public LabeledPoint(String label, int x, int y) {
        this.label = label;
        this.x = x;
        this.y = y;
    }

    public LabeledPoint() {
        this(null, 0, 0);
    }

    @Override
    public String toString() {
        return "LabeledPoint [label=" + label + ", x=" + x + ", y=" + y + "]";
    }

}
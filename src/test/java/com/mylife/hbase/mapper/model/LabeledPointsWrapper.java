// =======================================================
// Copyright Mylife.com Inc., 2013. All rights reserved.
//
// =======================================================

package com.mylife.hbase.mapper.model;

import java.util.List;

public class LabeledPointsWrapper {
    List<LabeledPoint> labeledPoints;

    public LabeledPointsWrapper(List<LabeledPoint> labeledPoints) {
        super();
        this.labeledPoints = labeledPoints;
    }

    public LabeledPointsWrapper() {
    }

    public List<LabeledPoint> getLabeledPoints() {
        return labeledPoints;
    }

    public void setLabeledPoints(List<LabeledPoint> labeledPoints) {
        this.labeledPoints = labeledPoints;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((labeledPoints == null) ? 0 : labeledPoints.hashCode());
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
        LabeledPointsWrapper other = (LabeledPointsWrapper) obj;
        if (labeledPoints == null) {
            if (other.labeledPoints != null)
                return false;
        } else if (!labeledPoints.equals(other.labeledPoints))
            return false;
        return true;
    }
}
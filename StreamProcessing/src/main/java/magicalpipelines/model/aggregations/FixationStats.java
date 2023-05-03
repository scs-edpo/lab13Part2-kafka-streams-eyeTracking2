package magicalpipelines.model.aggregations;

import com.google.gson.annotations.SerializedName;

public class FixationStats {

    @SerializedName("FixationCount")
    private long fixationCount;
    @SerializedName("TotalFixationDuration")
    private double totalFixationDuration;
    @SerializedName("AverageFixationDuration")
    private double averageFixationDuration;

    public FixationStats(long fixationCount, double totalFixationDuration, double averageFixationDuration) {
        this.fixationCount = fixationCount;
        this.totalFixationDuration = totalFixationDuration;
        this.averageFixationDuration = averageFixationDuration;
    }




    public long getFixationCount() {
        return fixationCount;
    }

    public void setFixationCount(long fixationCount) {
        this.fixationCount = fixationCount;
    }

    public double getTotalFixationDuration() {
        return totalFixationDuration;
    }

    public void setTotalFixationDuration(double totalFixationDuration) {
        this.totalFixationDuration = totalFixationDuration;
    }

    public double getAverageFixationDuration() {
        return averageFixationDuration;
    }

    public void setAverageFixationDuration(double averageFixationDuration) {
        this.averageFixationDuration = averageFixationDuration;
    }

    @Override
    public String toString() {
        return "FixationStats{" +
                "fixationCount=" + fixationCount +
                ", totalFixationDuration=" + totalFixationDuration +
                ", averageFixationDuration=" + averageFixationDuration +
                '}';
    }
}

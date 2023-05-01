package magicalpipelines.model.join;

import com.google.gson.annotations.SerializedName;
import magicalpipelines.model.aggregations.FixationStats;

public class FixationClick {

    @SerializedName("FixationStats")
    private FixationStats fixationStats;
    @SerializedName("ClickCount")
    private long clickCount;

    public FixationClick(FixationStats fixationStats, long clickCount) {
        this.fixationStats = fixationStats;
        this.clickCount = clickCount;
    }

    public FixationStats getFixationStats() {
        return fixationStats;
    }

    public void setFixationStats(FixationStats fixationStats) {
        this.fixationStats = fixationStats;
    }

    public long getClickCount() {
        return clickCount;
    }

    @Override
    public String toString() {
        return "FixationClick{" +
                "fixationStats=" + fixationStats +
                ", clickCount=" + clickCount +
                '}';
    }

    public void setClickCount(long clickCount) {
        this.clickCount = clickCount;
    }
}

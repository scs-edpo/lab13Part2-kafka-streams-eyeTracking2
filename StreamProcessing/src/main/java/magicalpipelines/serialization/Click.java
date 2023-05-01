package magicalpipelines.serialization;

import com.google.gson.annotations.SerializedName;

public class Click {

    @SerializedName("Timestamp")
    long timestamp;

    @SerializedName("X")
    double x;

    @SerializedName("Y")
    double y;

    @SerializedName("aoi")
    String aoi;

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public double getX() {
        return x;
    }

    public void setX(double x) {
        this.x = x;
    }

    public double getY() {
        return y;
    }

    public void setY(double y) {
        this.y = y;
    }

    public String getAoi() {
        return aoi;
    }

    public void setAoi(String aoi) {
        this.aoi = aoi;
    }
}

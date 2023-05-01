package magicalpipelines.serialization;

import com.google.gson.annotations.SerializedName;

public class Fixation {

  @SerializedName("Timestamp")
  long timestamp;
  @SerializedName("Xpos")
  int xpos;
  @SerializedName("Ypos")
  int ypos;
  @SerializedName("PupilSize")
  double pupilSize;
  @SerializedName("FixationDuration")
  double fixationDuration;
  @SerializedName("FixationDispersion")
  double fixationDispersion;
  @SerializedName("EventSource")
  int eventSource;
  @SerializedName("Participant")
  String participant;
  @SerializedName("Task")
  String task;


  public long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  public int getXpos() {
    return xpos;
  }

  public void setXpos(int xpos) {
    this.xpos = xpos;
  }

  public int getYpos() {
    return ypos;
  }

  public void setYpos(int ypos) {
    this.ypos = ypos;
  }

  public double getPupilSize() {
    return pupilSize;
  }

  public void setPupilSize(double pupilSize) {
    this.pupilSize = pupilSize;
  }

  public double getFixationDuration() {
    return fixationDuration;
  }

  public void setFixationDuration(double fixationDuration) {
    this.fixationDuration = fixationDuration;
  }

  public double getFixationDispersion() {
    return fixationDispersion;
  }

  public void setFixationDispersion(double fixationDispersion) {
    this.fixationDispersion = fixationDispersion;
  }

  public int getEventSource() {
    return eventSource;
  }

  public void setEventSource(int eventSource) {
    this.eventSource = eventSource;
  }

  public String getParticipant() {
    return participant;
  }

  public void setParticipant(String participant) {
    this.participant = participant;
  }

  public String getTask() {
    return task;
  }

  public void setTask(String task) {
    this.task = task;
  }
}


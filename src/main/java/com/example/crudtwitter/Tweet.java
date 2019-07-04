package com.example.crudtwitter;
import java.util.Date;

import twitter4j.*;

//public abstract class Tweet implements Status {
public class Tweet {

    // TODO: Converter Geolocation p/ String (lat and long) na hora de adicionar no Cassandra, fazer o mesmo para o User
    // TODO: Replace String type to real variables Type
    public Date CreatedDate;
    public long Id;
    public String Text;
    public String Source;
    public boolean IsTruncated;
    public GeoLocation Geolocation;
    public String Latitude;
    public String Longitude;
    public boolean IsFavorited;
    public String UserName;
    public long[] Contributors;
    public String Language;

    public Tweet() {
    }

    // Constructor // public Tweet(User User) { }
    // todo: User is an abstract class, so it can't be initialized, use Status instead
    // Constructor
    public Tweet(Status status)
    {
        this.CreatedDate = status.getCreatedAt();
        this.Id = status.getId();
        this.Text = status.getText();
        this.Source = status.getSource();
        this.IsTruncated = status.isTruncated();
        this.Geolocation = status.getGeoLocation();
        this.IsFavorited = status.isFavorited();
        this.UserName = status.getUser().getName();
        this.Contributors = status.getContributors();
        this.Language = status.getLang();
    }

    // Constructor
    public Tweet(
                   Date CreatedDate,
                   long Id,
                   String Text,
                   String Source,
                   boolean IsTruncated,
                   GeoLocation Geolocation,
                   boolean isFavorited,
                   String User,
                   long[] Contributors,
                   String Language
                )
    {
        this.CreatedDate = CreatedDate;
        this.Id = Id;
        this.Text = Text;
        this.Source = Source;
        this.IsTruncated = IsTruncated;
        this.Geolocation = Geolocation;
        this.IsFavorited = isFavorited;
        this.UserName = User;
        this.Contributors = Contributors;
        this.Language = Language;
    }

    // Constructor for test todo:delete
    public Tweet(
                    long Id,
                    String Text,
                    String Latitude,
                    String Longitude,
                    String User,
                    String Language
                )
    {
        this.Id = Id;
        this.Text = Text;
        this.Latitude = Latitude;
        this.Longitude = Longitude;
        this.UserName = User;
        this.Language = Language;
    }

    public Date getCreatedAt() { return this.CreatedDate; }
    public long getId() { return this.Id; }
    public String getText() { return this.Text; }
    public String getSource() { return this.Source; }
    public boolean getIsTruncated() { return this.IsTruncated; }

    // FIXME
    //    public String getLatitude() { return Double.toString(this.Geolocation.getLatitude()); }
    //    public String getLongitude() { return Double.toString(this.Geolocation.getLongitude()); }
        public String getLatitude() { return this.Latitude; }
        public String getLongitude() { return this.Longitude; }

    public boolean getIsFavorited() { return this.IsFavorited; }
    public String getUserName() { return this.UserName; }
    public long[] getContributors() { return this.Contributors; }
    public String getLanguage() { return this.Language; }

    //TODO: Finish SETs
    public void setCreatedAt(Date x) { this.CreatedDate = x; }
    public void setId(long x) { this.Id = x; }
}


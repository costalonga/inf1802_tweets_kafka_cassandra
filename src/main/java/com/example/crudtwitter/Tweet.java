package com.example.crudtwitter;
import java.util.Date;

import twitter4j.*;

//public abstract class Tweet implements Status {
public class Tweet {

    // TODO: Converter Geolocation p/ String (lat and long) na hora de adicionar no Cassandra, fazer o mesmo para o User
    // TODO: Replace String type to real variables Type
    public String CreatedDate;
    public long Id;
    public String Text;
    public String Source;
    public String IsTruncated;
    public String Latitude;
    public String Longitude;
    public String IsFavorited;
    public String UserName;
    public String Contributors;
    public String Language;

    public Tweet() {
    }

    // Constructor // public Tweet(User User) { }
    // todo: User is an abstract class, so it can't be initialized, use Status instead
    // Constructor
    public Tweet(Status status)
    {
        this.CreatedDate = status.getCreatedAt().toString();
        this.Id = status.getId();
        this.Text = status.getText();
        this.Source = status.getSource();
        this.IsTruncated = Boolean.toString(status.isTruncated());
//        this.Latitude = Double.toString(status.getGeoLocation().getLatitude());
//        this.Longitude = Double.toString(status.getGeoLocation().getLongitude());
        //FIXME
        this.Latitude = "defaultLatitude";
        this.Longitude = "defaultLongitude";

        this.IsFavorited = Boolean.toString(status.isFavorited());
        this.UserName = status.getUser().getName();
        this.Contributors = status.getContributors().toString();
        this.Language = status.getLang();
    }


    // Constructor
    public Tweet(
            String CreatedDate,
            long Id,
            String Text,
            String Source,
            String IsTruncated,
            String Latitude,
            String Longitude,
            String isFavorited,
            String User,
            String Language
    )
    {
        this.CreatedDate = CreatedDate;
        this.Id = Id;
        this.Text = Text;
        this.Source = Source;
        this.IsTruncated = IsTruncated;
        this.Latitude = Latitude;
        this.Longitude = Longitude;
        this.IsFavorited = isFavorited;
        this.UserName = User;
        this.Contributors = "DefaultContributors"; // todo check for problems
        this.Language = Language;
    }


//    // Constructor
//    public Tweet(
//            String CreatedDate,
//            long Id,
//            String Text,
//            String Source,
//            boolean IsTruncated,
//            String Latitude,
//            String Longitude,
//            boolean isFavorited,
//            String User,
//            String Language
//    )
//    {
//        this.CreatedDate = CreatedDate;
//        this.Id = Id;
//        this.Text = Text;
//        this.Source = Source;
//        this.IsTruncated = IsTruncated;
//        this.Latitude = Latitude;
//        this.Longitude = Longitude;
//        this.IsFavorited = isFavorited;
//        this.UserName = User;
//        this.Contributors = null; // todo check for problems
//        this.Language = Language;
//    }



    // FIXME
    //    public String getLatitude() { return Double.toString(this.Geolocation.getLatitude()); }
    //    public String getLongitude() { return Double.toString(this.Geolocation.getLongitude()); }

    public long getId() { return this.Id; }
    public String getCreatedDate() { return this.CreatedDate; }
    public String getText() { return this.Text; }
    public String getSource() { return this.Source; }
    public String getIsTruncated() { return this.IsTruncated; }
    public String getIsFavorited() { return this.IsFavorited; }
    public String getLatitude() { return this.Latitude; }
    public String getLongitude() { return this.Longitude; }
    public String getUserName() { return this.UserName; }
    public String getContributors() { return this.Contributors; }
    public String getLanguage() { return this.Language; }

    //TODO: Finish SETs
    public void setCreatedAt(String x) { this.CreatedDate = x; }
    public void setId(long x) { this.Id = x; }
    public void setLanguage(String language) { this.Language = language; }
}


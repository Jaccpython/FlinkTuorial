package com.atguigu.chaoter05Review;

import java.sql.Timestamp;

public class EventReview {
    public String user;
    public String url;
    public Long timestamp;

    public EventReview() {

    }

    public EventReview(String user, String url, Long timestamp) {
        this.user = user;
        this.url = url;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "EventReview{" +
                "user='" + user + '\'' +
                ", url='" + url + '\'' +
                ", timestamp=" + new Timestamp(timestamp) +
                '}';
    }
}

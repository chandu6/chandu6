

/**
 * Created by chandanambati on 9/29/16.
 */
package com.adbms.tweets;

        import com.mongodb.*;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

public class TwitterStreaming {

    public static void main(String[] args) {
        try
        {
            Mongo m = new Mongo("localhost", 27011);
            DB db = m.getDB("twitterdb");
//            int tweetCount = 100;

            final DBCollection coll = db.getCollection("tweet");

            ConfigurationBuilder cb = new ConfigurationBuilder();
            cb.setDebugEnabled(true)
                    .setOAuthConsumerKey("pPHEvdhqjxuKpxb1Xbt7FA9vP")
                    .setOAuthConsumerSecret("A2VGmYFSMIoztSiObOQGdxCNkIEY8J9PSG6YlIpBuWhkGieM7g")
                    .setOAuthAccessToken("4831348288-pFKUtkXsIsrzMXObvhFV5hEUX59oPSvlCxqYHe1")
                    .setOAuthAccessTokenSecret("GWVnojUCp2lugMfyVGFNgIyZiuzRKqY4sJyALYgDHVeNI");

            StatusListener listener = new StatusListener(){
                //                int count = 0;
                public void onStatus(Status status) {
//               		System.out.println(status.getId() +  " : " + status.getSource()+ " : " +status.getCreatedAt()+ " : " + status.getUser().getName() + " : " +status.getText());
                    //System.out.println(status.getUser().getName() + " : " + status.getText());

                    DBObject dbObj = new BasicDBObject();
                    dbObj.put("id_str", status.getId());
                    dbObj.put("name", status.getUser().getName());
                    dbObj.put("text", status.getText());
                    dbObj.put("source", status.getSource());
//                	if(status.getGeoLocation() != null) {
//	                	DBObject pos = new BasicDBObject();
//	                	pos.put("long", status.getGeoLocation().getLongitude());
//	                	pos.put("lat", status.getGeoLocation().getLatitude());
//	                	dbObj.put("pos", pos);
//                	} else if( status.getPlace() != null ) {
//                		dbObj.put("country", status.getPlace().getCountry());
//                	}
                    coll.insert(dbObj);
//                    System.out.println(++count);
                }
                public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {}
                public void onTrackLimitationNotice(int numberOfLimitedStatuses) {}
                public void onException(Exception ex) {
                    ex.printStackTrace();
                }
                @Override
                public void onScrubGeo(long arg0, long arg1) {
                    // TODO Auto-generated method stub

                }
                @Override
                public void onStallWarning(StallWarning arg0) {
                    // TODO Auto-generated method stub

                }
            };

            TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
            twitterStream.addListener(listener);
            // sample() method internally creates a thread which manipulates TwitterStream and calls these adequate listener methods continuously.
            twitterStream.sample();

        }catch(Exception e) {
            e.printStackTrace();
        }
    }

}

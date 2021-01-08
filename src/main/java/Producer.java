import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.json.JSONObject;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

public class Producer {

    private final static String QUEUE_NAME = "activities";

    public static void main(String[] args) throws IOException, TimeoutException {

        ConnectionFactory factory = new ConnectionFactory();

        factory.setHost("sparrow.rmq.cloudamqp.com");
        factory.setUsername("xvwhkdco");
        factory.setPassword("jLceeYgpHt2xuxqu5xGoFbIXU7TPVZsi");
        factory.setVirtualHost("xvwhkdco");

        String filename= "D:\\Proiect\\activity.txt";
        Stream<String> stream= null;

        try
        {
            stream= Files.lines(Paths.get(filename));
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }

        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            channel.queueDeclare(QUEUE_NAME, false, false, false, null);
            stream.forEach(line->{
                String[] l= line.split("		");
                String stringId="cb0c76d8-24a3-4913-a7b6-17116ce364ff";
                UUID id = UUID.fromString(stringId);
                String startDateTime = l[0];
                String endDateTime =l[1];

                JSONObject jsonObject = new JSONObject();
                String activity =l[2].replace("\t","");
                jsonObject.put("patient_id", id);
                jsonObject.put("activity_name", activity);
                jsonObject.put("start_date", startDateTime);
                jsonObject.put("end_date", endDateTime);
                try {
                    TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                try {
                    channel.basicPublish("", QUEUE_NAME, null, jsonObject.toString().getBytes(StandardCharsets.UTF_8));
                } catch (IOException e) {
                    e.printStackTrace();
                }
                System.out.println(" [x] Sent '" + jsonObject + "'");

            });

        }

    }
}

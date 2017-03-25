//import util.properties packages
import java.util.Properties;

//import simple producer packages
import org.apache.kafka.clients.producer.Producer;

//import KafkaProducer packages
import org.apache.kafka.clients.producer.KafkaProducer;

//import ProducerRecord packages
import org.apache.kafka.clients.producer.ProducerRecord;

//Create java class named SimpleProducer
import com.google.gson.*;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
public class Rutas {

   public static void main(String[] args) throws Exception{
      // Check arguments length value
      if(args.length == 0){
         System.out.println("Enter topic name");
         return;
      }

Random r = new Random();
        
      String[] estado={"disponible","no disponible"};

      Random randomGenerator = new Random();
      
       //Assign topicName to string variable
     String topicName = args[0].toString();
      // create instance for properties to access producer config
      Properties props = new Properties();
      //Assign localhost id
      props.put("bootstrap.servers", "localhost:9092");
      //Set acknowledgements for producer requests.
      props.put("acks", "all");

      //If the request fails, the producer can automatically retry,
      props.put("retries", 0);

      //Specify buffer size in config
      props.put("batch.size", 16384);
      //Reduce the no of requests less than 0
      props.put("linger.ms", 1);
      
      //The buffer.memory controls the total amount of memory available to the producer for buffering
      props.put("buffer.memory", 33554432);
      props.put("key.serializer",
         "org.apache.kafka.common.serialization.StringSerializer");
      props.put("value.serializer",
         "org.apache.kafka.common.serialization.StringSerializer");

      Producer<String, String> producer = new KafkaProducer<String, String>(props);
    Timer timer = new Timer();
    TimerTask task = new TimerTask() {
      double x1=4.689836;
      double x2=4.558327;
      double y1=-74.163206;
      double y2=-74.046148;

      double coordX=0.00;
      double  coordY=0.00;
     
        public void run()
        {
         for(int i=0;i<10;i++){
          coordX= x2 + (x1-x2) * r.nextDouble();
          coordY=-(Math.abs(y2) + (Math.abs(y1)-Math.abs(y2)) * r.nextDouble());
  
          String json= "'camion-"+i+"','"+coordX+"','"+coordY+"',"+estado[randomGenerator.nextInt(2)];
      System.out.println(json);  
           
          //      String json = gson.toJson(
        //      new Camion("camion#"+i,
        //                      zona[randomGenerator.nextInt(7)],
        //                      estado[randomGenerator.nextInt(4)]
        
        //  producer.send(new ProducerRecord<String, String>(topicName,Integer.toString(i), json));
            }
           }
      
     
             //  producer.close();
   };
timer.schedule( task, 0L ,60000L);
}
}
class Camion{

private String nombre;
private String zona;
private String estado;

public Camion(String nombre,String zona,String estado){
this.nombre=nombre;
this.zona=zona;
this.estado=estado;
}

public void setZona(String zona){
this.zona= zona;
}

public String getZona(){
return this.zona;
}

public void setEstado(String estado){
this.estado=estado;
}

public String getEstado(){
return this.estado;
}
public void setNombre(String nombre){
this.nombre=nombre;
}
public String getNombre(){
return this.nombre;
}
}








      
      
      
      

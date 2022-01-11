import java.util.Properties;

public class PropertiesTest {
    public static void main(String[] args){
        Properties props = new Properties();
        for(String key :props.stringPropertyNames()){
            System.out.println(props.getProperty(key));
        }
    }
}

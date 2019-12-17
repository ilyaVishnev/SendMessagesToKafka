package ru;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class ReceiverFromDataBase  extends Receiver{

    public static int min = 1024;
    public static int max = 1073741824;

    @Override
    public void onStop() {

    }

    public ReceiverFromDataBase(){
        super(StorageLevel.MEMORY_ONLY());
    }

    public static int getMin() {
        return min;
    }

    public static int getMax() {
        return max;
    }

    @Override
    public void onStart() {
        Properties properties = new Properties();
        properties.setProperty("user", "postgres");
        properties.setProperty("password", "pobeda");
        try {
            Connection connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/packetSize", properties);
            PreparedStatement prepMin = connection.prepareStatement("SELECT limit_value FROM limits_per_hour WHERE limits_per_hour.limit_name = \'min\' AND limits_per_hour.effective_date <= ? ORDER BY limits_per_hour.effective_date DESC LIMIT 1");
            PreparedStatement prepMax = connection.prepareStatement("SELECT limit_value FROM limits_per_hour WHERE limits_per_hour.limit_name = \'max\' AND limits_per_hour.effective_date <= ? ORDER BY limits_per_hour.effective_date DESC LIMIT 1");
            prepMin.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
            prepMax.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
            ResultSet resultSetMin = prepMin.executeQuery();
            ResultSet resultSetMax = prepMax.executeQuery();
            resultSetMin.next();
            min = resultSetMin.getInt(1);
            resultSetMax.next();
            max = resultSetMax.getInt(1);
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }
}

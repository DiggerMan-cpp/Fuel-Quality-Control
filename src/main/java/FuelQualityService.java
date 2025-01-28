import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.json.JSONArray;
import org.json.JSONObject;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class FuelQualityChecker {
    private static final String GOOD_FUEL_URL = "https://apidata.mos.ru/v1/datasets/753/rows?api_key= ";
    private static final String BAD_STATIONS_URL = "https://apidata.mos.ru/v1/datasets/754/rows?api_key= ";
    private static final String DB_URL = "jdbc:postgresql://localhost:5432/fuel_quality_db";
    private static final String DB_USER = "user";
    private static final String DB_PASSWORD = "password";

    public static void main(String[] args) {
        try {
            JSONArray goodFuelData = fetchDataFromPortal(GOOD_FUEL_URL);
            JSONArray badStationsData = fetchDataFromPortal(BAD_STATIONS_URL);
            Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
            for (int i = 0; i < goodFuelData.length(); i++) {
                JSONObject record = goodFuelData.getJSONObject(i);
                JSONObject cells = record.getJSONObject("Cells");

                String stationName = cells.getString("Address");
                String fuelType = cells.getString("FuelType");
                boolean isQualityOk = true;

                saveDataToDatabase(conn, stationName, fuelType, isQualityOk);
            }
            for (int i = 0; i < badStationsData.length(); i++) {
                JSONObject record = badStationsData.getJSONObject(i);
                JSONObject cells = record.getJSONObject("Cells");

                String stationName = cells.getString("Address");
                String fuelType = cells.has("FuelType") ? cells.getString("FuelType") : "Unknown";
                boolean isQualityOk = false; 

                saveDataToDatabase(conn, stationName, fuelType, isQualityOk);
            }

            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    private static JSONArray fetchDataFromPortal(String url) throws Exception {
        CloseableHttpClient httpClient = HttpClients.createDefault();
        HttpGet request = new HttpGet(url);
        HttpResponse response = httpClient.execute(request);
        String jsonResponse = EntityUtils.toString(response.getEntity());
        return new JSONArray(jsonResponse);
    }
    private static void saveDataToDatabase(Connection conn, String stationName, String fuelType, boolean isQualityOk) throws Exception {
        String query = "INSERT INTO fuel_quality (station_name, fuel_type, is_quality_ok) VALUES (?, ?, ?)";
        PreparedStatement pstmt = conn.prepareStatement(query);
        pstmt.setString(1, stationName);
        pstmt.setString(2, fuelType);
        pstmt.setBoolean(3, isQualityOk);
        pstmt.executeUpdate();
    }
}

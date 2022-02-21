package org.acme;

import io.agroal.api.AgroalDataSource;
import oracle.jdbc.OracleTypes;

import java.io.BufferedWriter;
import java.io.IOException;
import java.sql.*;
import java.util.*;

public class CSVExpoter {
    private int totalCount = -1;

    public CSVExpoter() {}
    public int getTotalCount() {
        return totalCount;
    }

    public List<Map<String, Object>> getData(AgroalDataSource dataSource, int rowStart, int rowEnd, String beginDate, String endDate) throws Exception {
        List<Map<String, Object>> listData = new ArrayList<>();
        Connection connection = null;
        CallableStatement callableStatement = null;

        try {
            connection = dataSource.getConnection();
            callableStatement = connection.prepareCall("{ call ?.?(?,?,?,?,?,?,?,?,?) } ");

            try {
                callableStatement.setString(1, "?");
                callableStatement.setString(2, "?");
                callableStatement.setString(3, "?");
                callableStatement.setString(4, "?");
                callableStatement.setString(5, "?");
                callableStatement.setString(6, "?");
                callableStatement.setString(7, "?");
                callableStatement.setString(8, "?");
                callableStatement.setString(9, "?");
                callableStatement.execute();

                ResultSet result = (ResultSet) callableStatement.getObject(8);
                if (totalCount == -1) {
                    totalCount = Integer.parseInt(callableStatement.getString(9));
                }
                ResultSetMetaData metaData = result.getMetaData();
                int numberOfColumns = metaData.getColumnCount();
                while (result.next()) {
                    Map<String, Object> r = new HashMap<>();
                    for (int i = 2; i <= numberOfColumns; i++) {
                        Object valueObject = result.getObject(i);
                        String columnName = metaData.getColumnName(i);
                        r.put(columnName, valueObject);
                    }
                    listData.add(r);
                }

                callableStatement.close();
            } catch (SQLException e) {
                e.printStackTrace();
                System.out.println("Connection error");
            } catch (Exception e) {
                e.printStackTrace();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (connection != null)
                    connection.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return listData;
    }

    public int writeCSV(List<Map<String, Object>> result, BufferedWriter fileWriter, int colcnt) throws Exception {
        int cnt = 0;
        for (Map<String, Object> r : result) {
            String line = "";
            int i = 2;
            for (Map.Entry<String, Object> entry : r.entrySet()) {
                String valueString = "";

                if(entry.getValue() != null) valueString = entry.getValue().toString();
                if(entry.getValue() instanceof String) {
                    valueString = "\"" + escapeDoubleQuotes(valueString) +"\"";
                }
                line = line.concat(Objects.toString(valueString, ""));
                if (i != colcnt) {
                    line = line.concat(",");
                }
            }
            fileWriter.newLine();
            fileWriter.write(line);
            cnt++;
        }
        return cnt;
    }

    public void writeHeaderLine(Map<String, Object> row, BufferedWriter fileWriter) throws SQLException, IOException {
        String headerLine = "";
        for (Map.Entry<String, Object> entry : row.entrySet()) {
            String columnName = entry.getKey();
            headerLine = headerLine.concat(columnName).concat(",");
        }
        fileWriter.write(headerLine.substring(0, headerLine.length() - 1));
    }

    private String escapeDoubleQuotes(String value) {
        return value.replaceAll("\"", "\"\"");
    }
}

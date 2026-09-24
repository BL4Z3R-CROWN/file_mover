package com.bluechip.move_files.model;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GenericDBModel {
    static Logger logger = LoggerFactory.getLogger(GenericDBModel.class.getName());
    private static String DBUri;
    private static String DBUser;
    private static String DBPassword;
    private Connection persistentConnection;
    private long lastUsedTime = 0;
    private static final long IDLE_TIMEOUT_MS = 5 * 60 * 1000; // 5 minutes

    public GenericDBModel(String dbUrl, String dbUserName, String dbPassword) {
        GenericDBModel.setDBUri(dbUrl);
        GenericDBModel.setDBUser(dbUserName);
        GenericDBModel.setDBPassword(dbPassword);
    }

    private Connection getConnection() throws SQLException {
        // Close stale connection if idle too long
        if (persistentConnection != null) {
            try {
                if (persistentConnection.isClosed()) {
                    persistentConnection = null;
                } else if (System.currentTimeMillis() - lastUsedTime > IDLE_TIMEOUT_MS) {
                    logger.info("Closing idle database connection (idle for > 5 minutes)");
                    persistentConnection.close();
                    persistentConnection = null;
                }
            } catch (SQLException e) {
                logger.warn("Error checking connection state, will reconnect: {}", e.getMessage());
                persistentConnection = null;
            }
        }

        // Create new connection if needed
        if (persistentConnection == null) {
            logger.info("Opening new database connection");
            persistentConnection = DriverManager.getConnection(DBUri, DBUser, DBPassword);
        }

        lastUsedTime = System.currentTimeMillis();
        return persistentConnection;
    }

    public AppSettingsModel GetApplicationSettings(int id) throws SQLException {
        AppSettingsModel settings = new AppSettingsModel();
        String query = "SELECT id, decompr_watched_path, profiling_directory, file_info_table_name, identity FROM app_settings WHERE id = ? LIMIT 1";
        try (Connection connection = DriverManager.getConnection(DBUri, DBUser, DBPassword);
             PreparedStatement stmt = connection.prepareStatement(query)) {
            stmt.setInt(1, id);
            System.out.println("Executing query: SELECT id, decompr_watched_path, profiling_directory, file_info_table_name, identity FROM app_settings WHERE id = " + id);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    settings.setId(rs.getInt("id"));
                    settings.setDecomprWatchedPath(rs.getString("decompr_watched_path"));
                    settings.setProfilingDirectory(rs.getString("profiling_directory"));
                    settings.setFileInfoTableName(rs.getString("file_info_table_name"));
                    settings.setIdentity(rs.getString("identity"));
                }
            }
        } catch (Exception e) {
            logger.error("Database error while fetching app settings: {}", e.getMessage(), e);
        }
        return settings;
    }

    public boolean fileNameExistsInFileInfoTable(String fileName, String fileInfoTableName) {
        String query = "SELECT 1 FROM " + fileInfoTableName + " WHERE file_name = ? LIMIT 1";
        try {
            Connection connection = getConnection();
            PreparedStatement stmt = connection.prepareStatement(query);
            stmt.setString(1, fileName);
            try (ResultSet rs = stmt.executeQuery()) {
                boolean exists = rs.next();
                stmt.close();
                return exists;
            }
        } catch (SQLException e) {
            logger.error("Database error while checking file existence: {}", e.getMessage(), e);
            // Properly close and reset connection on failure
            try {
                if (persistentConnection != null && !persistentConnection.isClosed()) {
                    persistentConnection.close();
                }
            } catch (SQLException ex) {
                // ignore close error
            }
            persistentConnection = null;
            return false;
        }
    }

    public static String getDBUri() {
        return DBUri;
    }

    public static void setDBUri(String dBUri) {
        DBUri = dBUri;
    }

    public static String getDBUser() {
        return DBUser;
    }

    public static void setDBUser(String dBUser) {
        DBUser = dBUser;
    }

    public static String getDBPassword() {
        return DBPassword;
    }

    public static void setDBPassword(String dBPassword) {
        DBPassword = dBPassword;
    }
}

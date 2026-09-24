package com.bluechip.move_files;

import com.bluechip.move_files.FileMover.FileMover;
import com.bluechip.move_files.Scheduler.MopUpScheduler;
import com.bluechip.move_files.model.AppSettingsModel;
import com.bluechip.move_files.model.GenericDBModel;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.Environment;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class MoveFilesApplication {
    public static void main(String[] args) {
        int streamId;
        ConfigurableApplicationContext context = SpringApplication.run(MoveFilesApplication.class, (String[])args);
        if (args.length == 0) {
            System.err.println("Error: stream id is required as the first CLI argument");
            System.exit(1);
        }
        try {
            streamId = Integer.parseInt(args[0]);
        }
        catch (NumberFormatException e) {
            System.err.println("Error: stream id must be an integer, got: " + args[0]);
            System.exit(1);
            return;
        }
        String cliIdentity = null;
        if (args.length > 1) {
            cliIdentity = args[1];
        }
        String cliSourceDirectory = null;
        if (args.length > 2) {
            cliSourceDirectory = args[2];
        }
        Environment env = context.getEnvironment();
        String dbUri = env.getProperty("spring.datasource.url");
        String dbUser = env.getProperty("spring.datasource.username");
        String dbPassword = env.getProperty("spring.datasource.password");
        if (dbUri == null || dbUser == null || dbPassword == null) {
            System.err.println("Error: database configuration (spring.datasource.url/username/password) not found in application.properties");
            System.exit(1);
        }
        GenericDBModel dbModel = new GenericDBModel(dbUri, dbUser, dbPassword);
        AppSettingsModel settings = null;
        try {
            settings = dbModel.GetApplicationSettings(streamId);
        }
        catch (Exception e) {
            System.err.println("Error fetching settings from database: " + e.getMessage());
            System.exit(1);
        }
        if (settings == null || settings.getId() == 0) {
            System.err.println("Error: no app_settings row found for stream id: " + streamId);
            System.exit(1);
        }
        String sourceDirectory = cliSourceDirectory != null ? cliSourceDirectory : settings.getDecomprWatchedPath();
        String destinationDirectory = settings.getProfilingDirectory();
        String identity = cliIdentity != null ? cliIdentity : settings.getIdentity();
        String fileInfoTableName = settings.getFileInfoTableName();
        if (sourceDirectory == null || sourceDirectory.isEmpty()) {
            System.err.println("Error: source directory is required (either via CLI arg or decompr_watched_path in app_settings)");
            System.exit(1);
        }
        if (destinationDirectory == null || destinationDirectory.isEmpty()) {
            System.err.println("Error: profiling_directory is required in app_settings");
            System.exit(1);
        }
        if (fileInfoTableName == null || fileInfoTableName.isEmpty()) {
            System.err.println("Error: file_info_table_name is required in app_settings");
            System.exit(1);
        }
        System.out.println("========== Resolved Configuration ==========");
        System.out.println("Stream ID: " + streamId);
        System.out.println("Source Directory: " + sourceDirectory + (cliSourceDirectory != null ? " (from CLI)" : " (from DB)"));
        System.out.println("Destination Directory: " + destinationDirectory + " (from DB)");
        System.out.println("Identity: " + (identity == null || identity.isEmpty() ? "(none)" : identity) + (cliIdentity != null ? " (from CLI)" : " (from DB)"));
        System.out.println("File Info Table: " + fileInfoTableName);
        System.out.println("==========================================");
        FileMover fileMover = new FileMover(dbModel, fileInfoTableName);
        fileMover.movefilesAtRest(sourceDirectory, destinationDirectory, identity);
        fileMover.movefilesOnCreation(sourceDirectory, destinationDirectory, identity);
        MopUpScheduler mopUpScheduler = (MopUpScheduler)context.getBean(MopUpScheduler.class);
        mopUpScheduler.setDirectories(sourceDirectory, destinationDirectory, identity);
        mopUpScheduler.setFileInfoTableName(fileInfoTableName);
        mopUpScheduler.setFileMover(fileMover);
        System.out.println("Mop-up scheduler initialized - will check every 15 minutes");
    }
}


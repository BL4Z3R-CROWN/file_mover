package com.bluechip.move_files.Scheduler;

import com.bluechip.move_files.FileMover.FileMover;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class MopUpScheduler {
    private FileMover fileMover = new FileMover();
    private String sourceDirectory = "";
    private String destinationDirectory = "";
    private String identity = "";
    private String fileInfoTableName = "";

    public void setDirectories(String source, String destination, String identity) {
        this.sourceDirectory = source;
        this.destinationDirectory = destination;
        this.identity = identity;
    }

    public void setFileInfoTableName(String fileInfoTableName) {
        this.fileInfoTableName = fileInfoTableName;
    }

    public void setFileMover(FileMover fileMover) {
        this.fileMover = fileMover;
    }

    @Scheduled(fixedRate=900000L)
    public void mopUp() {
        if (this.sourceDirectory != null && !this.sourceDirectory.isEmpty() && this.destinationDirectory != null && !this.destinationDirectory.isEmpty()) {
            this.fileMover.movefilesAtRest(this.sourceDirectory, this.destinationDirectory, this.identity);
        } else {
            System.out.println("[MOP-UP] Directories not configured, skipping mop-up");
        }
    }
}


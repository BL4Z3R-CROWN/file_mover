package com.bluechip.move_files.FileMover;

import com.bluechip.move_files.model.GenericDBModel;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.commons.io.monitor.FileAlterationListener;
import org.apache.commons.io.monitor.FileAlterationListenerAdaptor;
import org.apache.commons.io.monitor.FileAlterationMonitor;
import org.apache.commons.io.monitor.FileAlterationObserver;

public class FileMover {
    private GenericDBModel dbModel;
    private String fileInfoTableName;

    public FileMover() {
        this.dbModel = null;
        this.fileInfoTableName = null;
    }

    public FileMover(GenericDBModel dbModel, String fileInfoTableName) {
        this.dbModel = dbModel;
        this.fileInfoTableName = fileInfoTableName;
    }

    public void setDbModel(GenericDBModel dbModel) {
        this.dbModel = dbModel;
    }

    public void setFileInfoTableName(String fileInfoTableName) {
        this.fileInfoTableName = fileInfoTableName;
    }

    private String appendIdentityToFilename(String filename, String identity) {
        if (identity == null || identity.isEmpty()) {
            return filename;
        }
        int lastDotIndex = filename.lastIndexOf(46);
        if (lastDotIndex == -1) {
            return filename + "-" + identity;
        }
        return filename.substring(0, lastDotIndex) + "-" + identity + filename.substring(lastDotIndex);
    }

    private String resolveAvailableFilename(String proposedName, File destinationDirectory, String fileInfoTableName) {
        String extension;
        String baseName;
        if (destinationDirectory == null) {
            return proposedName;
        }

        int lastDotIndex = proposedName.lastIndexOf(46);
        if (lastDotIndex == -1) {
            baseName = proposedName;
            extension = "";
        } else {
            baseName = proposedName.substring(0, lastDotIndex);
            extension = proposedName.substring(lastDotIndex);
        }

        String candidateName = proposedName;
        int attempt = 1;
        while (new File(destinationDirectory, candidateName).exists()) {
            candidateName = baseName + "-" + String.format("%03d", attempt) + extension;
            attempt++;
            if (attempt > 9999) {
                System.out.println("Warning: Could not find an available filename after 9999 attempts for: " + proposedName);
                return proposedName;
            }
        }

        if (this.dbModel == null || fileInfoTableName == null || fileInfoTableName.isEmpty()) {
            return candidateName;
        }

        while (this.dbModel.fileNameExistsInFileInfoTable(candidateName, fileInfoTableName) || new File(destinationDirectory, candidateName).exists()) {
            candidateName = baseName + "-" + String.format("%03d", attempt) + extension;
            attempt++;
            if (attempt > 9999) {
                System.out.println("Warning: Could not find an available filename after 9999 attempts for: " + proposedName);
                return proposedName;
            }
        }
        return candidateName;
    }

    public void movefilesAtRest(String source, String destination, String identity) {
        File sourceDirectory = new File(source);
        File destinationDirectory = new File(destination);

        List<File> listOfFiles;
        try {
            listOfFiles = Files.walk(Paths.get(sourceDirectory.getPath()))
                .filter(Files::isRegularFile)
                .map(Path::toFile)
                .filter(f -> !f.getName().endsWith(".tmp"))
                .sorted(Comparator.comparingLong(File::lastModified))
                .collect(Collectors.toList());
        } catch (IOException e) {
            System.out.println("Failed to walk source directory: " + e.getMessage());
            return;
        }

        if (listOfFiles.isEmpty()) {
            System.out.println("Source directory is empty or not accessible: " + source);
            return;
        }

        for (File file : listOfFiles) {
            String newFilename = this.appendIdentityToFilename(file.getName(), identity);
            String resolvedFilename = this.resolveAvailableFilename(newFilename, destinationDirectory, this.fileInfoTableName);
            File destinationFile = new File(destinationDirectory, resolvedFilename);
            try {
                Files.move(file.toPath(), destinationFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException e) {
                System.out.println("Failed to move file: " + file.getName() + " -> " + e.getMessage());
            }
        }
    }

    public void movefilesOnCreation(String source, String destination, final String identity) {
        File sourceDirectory = new File(source);
        final File destinationDirectory = new File(destination);

        FileAlterationObserver directoryObserver = new FileAlterationObserver(
            sourceDirectory, TrueFileFilter.INSTANCE
        );
        directoryObserver.addListener((FileAlterationListener)new FileAlterationListenerAdaptor(){

            public void onFileCreate(File file) {
                if (file.isFile() && !file.getName().endsWith(".tmp")) {
                    String newFilename = FileMover.this.appendIdentityToFilename(file.getName(), identity);
                    String resolvedFilename = FileMover.this.resolveAvailableFilename(newFilename, destinationDirectory, FileMover.this.fileInfoTableName);
                    File destinationFile = new File(destinationDirectory, resolvedFilename);
                    try {
                        Files.move(file.toPath(), destinationFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
                    } catch (IOException e) {
                        System.out.println("Failed to move file: " + file.getName() + " -> " + e.getMessage());
                    }
                }
            }
        });
        FileAlterationMonitor monitor = new FileAlterationMonitor();
        monitor.addObserver(directoryObserver);
        try {
            monitor.start();
        }
        catch (Exception e) {
            System.out.println("Error starting file monitor: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
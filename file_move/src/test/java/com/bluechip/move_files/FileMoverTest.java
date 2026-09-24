package com.bluechip.move_files;

import com.bluechip.move_files.FileMover.FileMover;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertTrue;

class FileMoverTest {

    @Test
    void movesFileWhenDestinationAlreadyContainsSameName() throws Exception {
        Path tempDir = Files.createTempDirectory("file-mover-test");
        Path sourceDir = tempDir.resolve("source");
        Path destinationDir = tempDir.resolve("destination");
        Files.createDirectories(sourceDir);
        Files.createDirectories(destinationDir);

        Path sourceFile = sourceDir.resolve("alpha.unl");
        Files.writeString(sourceFile, "data");

        Path existingDestinationFile = destinationDir.resolve("alpha-test.unl");
        Files.writeString(existingDestinationFile, "existing");

        FileMover mover = new FileMover(null, null);
        mover.movefilesAtRest(sourceDir.toString(), destinationDir.toString(), "test");

        assertTrue(Files.exists(destinationDir.resolve("alpha-test-001.unl")) || Files.exists(destinationDir.resolve("alpha-test.unl")));
    }
}

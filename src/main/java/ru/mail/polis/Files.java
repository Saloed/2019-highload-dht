/*
 * Copyright 2019 (c) Odnoklassniki
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ru.mail.polis;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.concurrent.atomic.AtomicLong;

import org.jetbrains.annotations.NotNull;

/**
 * Utility methods for handling files.
 *
 * @author Vadim Tsesko
 */
public final class Files {
    private static final String TEMP_PREFIX = "highload-dht";

    private Files() {
        // Don't instantiate
    }

    /**
     * Provides temporary directory for testing purposes.
     */
    public static File createTempDirectory() throws IOException {
        final File data = java.nio.file.Files.createTempDirectory(TEMP_PREFIX).toFile();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                if (data.exists()) {
                    recursiveDelete(data);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }));
        return data;
    }

    /**
     * Remove entire directory with subdirectories.
     */
    public static void recursiveDelete(@NotNull final File path) throws IOException {
        java.nio.file.Files.walkFileTree(
                path.toPath(),
                new SimpleFileVisitor<>() {
                    @NotNull
                    @Override
                    public FileVisitResult visitFile(
                            @NotNull final Path file,
                            @NotNull final BasicFileAttributes attrs) throws IOException {
                        java.nio.file.Files.delete(file);
                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult postVisitDirectory(
                            final Path dir,
                            final IOException exc) throws IOException {
                        java.nio.file.Files.delete(dir);
                        return FileVisitResult.CONTINUE;
                    }
                });
    }

    /**
     * Calculates directory size in bytes.
     */
    public static long directorySize(@NotNull final File path) throws IOException {
        final AtomicLong result = new AtomicLong(0L);
        java.nio.file.Files.walkFileTree(
                path.toPath(),
                new SimpleFileVisitor<>() {
                    @NotNull
                    @Override
                    public FileVisitResult visitFile(
                            @NotNull final Path file,
                            @NotNull final BasicFileAttributes attrs) {
                        result.addAndGet(attrs.size());
                        return FileVisitResult.CONTINUE;
                    }
                });
        return result.get();
    }
}

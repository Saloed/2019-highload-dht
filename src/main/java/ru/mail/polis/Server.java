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
import java.util.Collections;

import ru.mail.polis.dao.DAO;
import ru.mail.polis.dao.DAOFactory;
import ru.mail.polis.service.Service;
import ru.mail.polis.service.ServiceFactory;

/**
 * Starts storage and waits for shutdown.
 *
 * @author Vadim Tsesko
 */
public final class Server {
    private static final int PORT = 8080;

    private Server() {
        // Not instantiable
    }

    public static void main(final String[] args) throws IOException {
        // Temporary storage in the file system
        final File data = Files.createTempDirectory();

        // Start the service
        final DAO dao = DAOFactory.create(data);
        final Service service =
                ServiceFactory.create(
                        PORT,
                        dao,
                        Collections.singleton("http://localhost:" + PORT));
        service.start();
        Runtime.getRuntime().addShutdownHook(
                new Thread(() -> {
                    service.stop();
                    try {
                        dao.close();
                    } catch (IOException e) {
                        throw new RuntimeException("Can't close dao", e);
                    }
                }));
    }
}
